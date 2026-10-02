//go:build e2e

// Package e2e exercises the running Compose stack (three application replicas
// behind Nginx, PostgreSQL, LocalStack and Keycloak) through its public HTTP API
// and the SQS command queue. Start the stack with
// `docker compose up -d --build --wait` and run `go test -tags=e2e -count=1 -v ./test/e2e/`.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsSQS "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const providerID = "provider-a"

var (
	baseURL     = env("E2E_BASE_URL", "http://localhost:8080")
	tokenURL    = env("E2E_TOKEN_URL", "http://localhost:8081/realms/backend/protocol/openid-connect/token")
	databaseURL = env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/betting?sslmode=disable")
	sqsEndpoint = env("AWS_ENDPOINT", "http://localhost:4566")
	queueName   = env("SQS_TRANSACTION_QUEUE", "wager-transactions.fifo")
	httpClient  = &http.Client{Timeout: 30 * time.Second}
)

type stack struct {
	t             *testing.T
	internalToken string
	providerToken string
	db            *pgxpool.Pool
	sqs           *awsSQS.Client
	queueURL      string
}

type wagerResponse struct {
	Status           string `json:"status"`
	TransactionID    string `json:"transactionId"`
	FailureCode      string `json:"failureCode"`
	IdempotentReplay bool   `json:"idempotentReplay"`
	Balance          struct {
		Amount string `json:"amount"`
	} `json:"balance"`
	httpStatus int
}

type wagerBody struct {
	ExternalTransactionID          string            `json:"externalTransactionId"`
	PlayerID                       string            `json:"playerId"`
	WalletID                       string            `json:"walletId"`
	RoundID                        string            `json:"roundId"`
	GameID                         string            `json:"gameId"`
	Kind                           string            `json:"kind"`
	Money                          map[string]string `json:"money"`
	ReferenceExternalTransactionID string            `json:"referenceExternalTransactionId,omitempty"`
}

func newStack(t *testing.T) *stack {
	t.Helper()
	if code, _ := get(t, baseURL+"/health/ready", ""); code != http.StatusOK {
		t.Skipf("the Compose stack is not ready at %s (status %d)", baseURL, code)
	}
	s := &stack{t: t}
	s.internalToken = token(t, url.Values{"grant_type": {"client_credentials"}, "client_id": {"backend-internal"}, "client_secret": {"backend-internal-secret"}})
	s.providerToken = token(t, url.Values{"grant_type": {"password"}, "client_id": {"backend-api"}, "username": {providerID}, "password": {providerID}})

	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s.db = pool

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithBaseEndpoint(sqsEndpoint),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	s.sqs = awsSQS.NewFromConfig(awsCfg)
	output, err := s.sqs.GetQueueUrl(context.Background(), &awsSQS.GetQueueUrlInput{QueueName: &queueName})
	if err != nil {
		t.Fatal(err)
	}
	s.queueURL = *output.QueueUrl
	return s
}

// 1. The same bet sent 50 times in parallel, spread by Nginx over the replicas,
// produces one debit; every other answer is a replay of the same result.
func TestE2EFiftyParallelIdenticalBets(t *testing.T) {
	s := newStack(t)
	playerID, walletID := s.openWallet("100.00")
	body := s.bet(playerID, walletID, "e2e-fifty-"+uuid.NewString(), "25.00")
	key := providerID + ":" + body.ExternalTransactionID

	responses := parallel(50, func(int) wagerResponse { return s.postWager(key, body) })
	fresh := 0
	for _, response := range responses {
		if response.httpStatus != http.StatusOK || response.Status != "PROCESSED" || response.TransactionID != responses[0].TransactionID || response.Balance.Amount != "75.00" {
			t.Fatalf("unexpected answer to a duplicate request: %+v", response)
		}
		if !response.IdempotentReplay {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("expected exactly one non-replay answer, got %d", fresh)
	}
	s.assertWallet(walletID, "75.00", 1)
}

// 2. Two different 80.00 bets race on a 100.00 wallet: one is processed, the
// other is rejected for insufficient funds, and resends do not change anything.
func TestE2EConcurrentOverdraftBets(t *testing.T) {
	s := newStack(t)
	playerID, walletID := s.openWallet("100.00")
	bodies := []wagerBody{
		s.bet(playerID, walletID, "e2e-overdraft-a-"+uuid.NewString(), "80.00"),
		s.bet(playerID, walletID, "e2e-overdraft-b-"+uuid.NewString(), "80.00"),
	}
	responses := parallel(2, func(i int) wagerResponse {
		return s.postWager(providerID+":"+bodies[i].ExternalTransactionID, bodies[i])
	})
	processed, rejected := 0, 0
	for _, response := range responses {
		switch {
		case response.httpStatus == http.StatusOK && response.Status == "PROCESSED":
			processed++
		case response.httpStatus == http.StatusUnprocessableEntity && response.FailureCode == "insufficient_funds":
			rejected++
		default:
			t.Fatalf("unexpected answer: %+v", response)
		}
	}
	if processed != 1 || rejected != 1 {
		t.Fatalf("expected one processed and one insufficient_funds, got %d/%d", processed, rejected)
	}
	for i, body := range bodies {
		replay := s.postWager(providerID+":"+body.ExternalTransactionID, body)
		if !replay.IdempotentReplay || replay.httpStatus != responses[i].httpStatus || replay.Status != responses[i].Status || replay.TransactionID != responses[i].TransactionID {
			t.Fatalf("resend changed the outcome: original %+v, resend %+v", responses[i], replay)
		}
	}
	s.assertWallet(walletID, "20.00", 1)
}

// 3. Independent wallets are processed concurrently and each ends correct.
func TestE2EIndependentWalletsInParallel(t *testing.T) {
	s := newStack(t)
	type walletRef struct{ player, wallet string }
	wallets := make([]walletRef, 20)
	for i := range wallets {
		wallets[i].player, wallets[i].wallet = s.openWallet("50.00")
	}
	responses := parallel(len(wallets), func(i int) wagerResponse {
		body := s.bet(wallets[i].player, wallets[i].wallet, "e2e-independent-"+uuid.NewString(), "10.00")
		return s.postWager(providerID+":"+body.ExternalTransactionID, body)
	})
	for i, response := range responses {
		if response.httpStatus != http.StatusOK || response.Status != "PROCESSED" || response.Balance.Amount != "40.00" {
			t.Fatalf("wallet %d: unexpected answer %+v", i, response)
		}
		s.assertWallet(wallets[i].wallet, "40.00", 1)
	}
}

// 4. The same operation arrives through HTTP and SQS at the same time, plus a
// later SQS copy with another messageId. Both channels compute the same payload
// hash, so none of them conflicts and the debit happens once.
func TestE2ESameOperationThroughHTTPAndSQS(t *testing.T) {
	s := newStack(t)
	playerID, walletID := s.openWallet("100.00")
	body := s.bet(playerID, walletID, "e2e-cross-"+uuid.NewString(), "30.00")
	key := providerID + ":" + body.ExternalTransactionID
	firstMessageID := "e2e-cross-" + uuid.NewString()

	var wg sync.WaitGroup
	var httpResponse wagerResponse
	wg.Add(2)
	go func() { defer wg.Done(); s.sendCommand(firstMessageID, key, body) }()
	go func() { defer wg.Done(); httpResponse = s.postWager(key, body) }()
	wg.Wait()
	if httpResponse.httpStatus != http.StatusOK || httpResponse.Status != "PROCESSED" || httpResponse.Balance.Amount != "70.00" {
		t.Fatalf("HTTP must process or replay the operation without conflict, got %+v", httpResponse)
	}
	s.waitForInbox(firstMessageID)

	secondMessageID := "e2e-cross-copy-" + uuid.NewString()
	s.sendCommand(secondMessageID, key, body)
	s.waitForInbox(secondMessageID)

	s.assertWallet(walletID, "70.00", 1)
	var transactions int
	if err := s.db.QueryRow(context.Background(), `SELECT COUNT(*) FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`, providerID, body.ExternalTransactionID).Scan(&transactions); err != nil {
		t.Fatal(err)
	}
	if transactions != 1 {
		t.Fatalf("expected one wager transaction for the operation, got %d", transactions)
	}
}

// 5. A producer keeps sending SQS commands while one replica is killed with
// SIGKILL in the middle of its work and another is stopped with SIGTERM.
// Every command must be applied exactly once and every wallet must reconcile.
// To make the interruption certain rather than lucky, the victim is frozen
// (docker pause) until its logs show a received message it has not deleted,
// and only then killed; SQS redelivers that message to a surviving replica.
func TestE2EInstanceFailuresDuringSQSLoad(t *testing.T) {
	s := newStack(t)
	containers := composeContainers(t)
	if len(containers) < 3 {
		t.Skipf("expected at least three application replicas, found %d", len(containers))
	}
	t.Cleanup(func() { restoreStack(t) })

	const walletsCount = 8
	runID := uuid.NewString()[:8]
	prefix := "e2e-crash-" + runID + "-"
	type walletRef struct{ player, wallet string }
	wallets := make([]walletRef, walletsCount)
	for i := range wallets {
		wallets[i].player, wallets[i].wallet = s.openWallet("100.00")
	}

	sent := make([]int, walletsCount)
	stopProducing := make(chan struct{})
	produced := make(chan struct{})
	go func() {
		defer close(produced)
		for n := 0; n < 900; n++ { // 0.10 each: at most 112 per wallet, never overdrawn
			select {
			case <-stopProducing:
				return
			default:
			}
			i := n % walletsCount
			id := fmt.Sprintf("%s%d-%d", prefix, i, sent[i])
			body := s.bet(wallets[i].player, wallets[i].wallet, id, "0.10")
			s.sendCommand(id, providerID+":"+body.ExternalTransactionID, body)
			sent[i]++
		}
	}()

	victim, stopped := containers[0], containers[1]
	s.waitForCompleted(prefix, 50)
	caught := 0
	for attempt := 0; attempt < 40 && caught == 0; attempt++ {
		docker(t, "pause", victim)
		if caught = receivedButNotDeleted(t, victim, prefix); caught == 0 {
			docker(t, "unpause", victim)
			time.Sleep(30 * time.Millisecond)
		}
	}
	if caught == 0 {
		t.Fatal("could not freeze the victim while it held an undeleted message")
	}
	docker(t, "kill", "--signal", "KILL", victim)
	interrupted := undeletedIDs(t, victim, prefix)

	s.waitForCompleted(prefix, 300)
	docker(t, "stop", "--time", "10", stopped)
	close(stopProducing)
	<-produced

	total := 0
	for _, n := range sent {
		total += n
	}
	s.waitForCompleted(prefix, total)
	for i, wallet := range wallets {
		cents := 10000 - sent[i]*10
		s.assertWallet(wallet.wallet, fmt.Sprintf("%d.%02d", cents/100, cents%100), sent[i])
	}

	if left := receivedButNotDeleted(t, stopped, prefix); left != 0 {
		t.Fatalf("the replica stopped with SIGTERM must finish its in-flight messages, left %d", left)
	}
	afterCommit := 0
	for _, id := range interrupted {
		if redeliveredAsDuplicate(t, containers[1:], id) {
			afterCommit++
		}
	}
	t.Logf("%d commands; SIGKILL interrupted %d received messages (%d after their commit, redelivered and stopped by the Inbox; %d before it, applied by a survivor); SIGTERM left 0",
		total, len(interrupted), afterCommit, len(interrupted)-afterCommit)
}

// waitForCompleted waits until at least n commands with the prefix committed.
func (s *stack) waitForCompleted(prefix string, n int) {
	s.t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	var completed int
	for time.Now().Before(deadline) {
		if err := s.db.QueryRow(context.Background(), `
			SELECT COUNT(*) FROM inbox_messages WHERE message_id LIKE $1 AND completed_at IS NOT NULL
		`, prefix+"%").Scan(&completed); err == nil && completed >= n {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.t.Fatalf("only %d of %d commands with prefix %s were processed", completed, n, prefix)
}

// receivedButNotDeleted counts messages a replica validated but never deleted,
// from its logs: work that was in flight when the replica went away.
func receivedButNotDeleted(t *testing.T, container, prefix string) int {
	return len(undeletedIDs(t, container, prefix))
}

// 6. After every replica restarts, replays still return the original results.
func TestE2ERestartPreservesIdempotency(t *testing.T) {
	s := newStack(t)
	if len(composeContainers(t)) == 0 {
		t.Skip("no application replicas found")
	}
	playerID, walletID := s.openWallet("100.00")
	body := s.bet(playerID, walletID, "e2e-restart-"+uuid.NewString(), "40.00")
	key := providerID + ":" + body.ExternalTransactionID
	original := s.postWager(key, body)
	if original.httpStatus != http.StatusOK || original.Status != "PROCESSED" {
		t.Fatalf("unexpected first answer: %+v", original)
	}
	pending := s.bet(playerID, walletID, "e2e-restart-refund-"+uuid.NewString(), "5.00")
	pending.Kind = "REFUND"
	pending.ReferenceExternalTransactionID = "e2e-never-sent-" + uuid.NewString()
	pendingAnswer := s.postWager(providerID+":"+pending.ExternalTransactionID, pending)
	if pendingAnswer.httpStatus != http.StatusAccepted || pendingAnswer.Status != "PENDING_REFERENCE" {
		t.Fatalf("expected a pending reference, got %+v", pendingAnswer)
	}

	compose(t, "restart", "application")
	waitReady(t)

	replay := s.postWager(key, body)
	if !replay.IdempotentReplay || replay.TransactionID != original.TransactionID || replay.Balance.Amount != "60.00" {
		t.Fatalf("replay after restart differs: original %+v, replay %+v", original, replay)
	}
	pendingReplay := s.postWager(providerID+":"+pending.ExternalTransactionID, pending)
	if !pendingReplay.IdempotentReplay || pendingReplay.Status != "PENDING_REFERENCE" || pendingReplay.TransactionID != pendingAnswer.TransactionID {
		t.Fatalf("pending reference was not preserved across the restart: %+v", pendingReplay)
	}
	s.assertWallet(walletID, "60.00", 1)
}

func (s *stack) openWallet(amount string) (string, string) {
	s.t.Helper()
	playerID := uuid.NewString()
	payload := fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":%q,"currency":"BRL"}}`, playerID, amount)
	code, body := post(s.t, baseURL+"/wallets", s.internalToken, "", payload)
	if code != http.StatusCreated && code != http.StatusOK {
		s.t.Fatalf("open wallet: %d %s", code, body)
	}
	var wallet struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &wallet); err != nil || wallet.ID == "" {
		s.t.Fatalf("decode wallet: %v %s", err, body)
	}
	return playerID, wallet.ID
}

func (s *stack) bet(playerID, walletID, externalID, amount string) wagerBody {
	return wagerBody{
		ExternalTransactionID: externalID,
		PlayerID:              playerID,
		WalletID:              walletID,
		RoundID:               "e2e-round",
		GameID:                "e2e-game",
		Kind:                  "BET",
		Money:                 map[string]string{"amount": amount, "currency": "BRL"},
	}
}

func (s *stack) postWager(idempotencyKey string, body wagerBody) wagerResponse {
	payload, err := json.Marshal(body)
	if err != nil {
		s.t.Error(err)
		return wagerResponse{}
	}
	code, raw := post(s.t, baseURL+"/wagering/transactions", s.providerToken, idempotencyKey, string(payload))
	var response wagerResponse
	_ = json.Unmarshal(raw, &response)
	response.httpStatus = code
	return response
}

func (s *stack) sendCommand(messageID, idempotencyKey string, body wagerBody) {
	data := map[string]any{
		"providerId":            providerID,
		"externalTransactionId": body.ExternalTransactionID,
		"idempotencyKey":        idempotencyKey,
		"playerId":              body.PlayerID,
		"walletId":              body.WalletID,
		"roundId":               body.RoundID,
		"gameId":                body.GameID,
		"kind":                  body.Kind,
		"money":                 body.Money,
	}
	if body.ReferenceExternalTransactionID != "" {
		data["referenceExternalTransactionId"] = body.ReferenceExternalTransactionID
	}
	payload, err := json.Marshal(map[string]any{
		"messageId":  messageID,
		"type":       "WagerTransactionRequested",
		"occurredAt": time.Now().UTC().Format(time.RFC3339Nano),
		"data":       data,
	})
	if err != nil {
		s.t.Error(err)
		return
	}
	message := string(payload)
	groupID := body.WalletID
	if _, err := s.sqs.SendMessage(context.Background(), &awsSQS.SendMessageInput{
		QueueUrl:               &s.queueURL,
		MessageBody:            &message,
		MessageGroupId:         &groupID,
		MessageDeduplicationId: &messageID,
	}); err != nil {
		s.t.Errorf("send command %s: %v", messageID, err)
	}
}

// waitForInbox waits until a consumer committed the command (Inbox completed).
func (s *stack) waitForInbox(messageID string) {
	s.t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		var completed bool
		err := s.db.QueryRow(context.Background(), `
			SELECT EXISTS (SELECT 1 FROM inbox_messages WHERE message_id = $1 AND completed_at IS NOT NULL)
		`, messageID).Scan(&completed)
		if err == nil && completed {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	s.t.Fatalf("command %s was not processed", messageID)
}

// assertWallet checks the balance through the API, the number of debits in the
// ledger, and the reconciliation endpoint (stored balance equals the ledger).
func (s *stack) assertWallet(walletID, balance string, debits int) {
	s.t.Helper()
	code, raw := get(s.t, baseURL+"/wallets/"+walletID, s.internalToken)
	var wallet struct {
		Balance struct {
			Amount string `json:"amount"`
		} `json:"balance"`
	}
	if err := json.Unmarshal(raw, &wallet); code != http.StatusOK || err != nil || wallet.Balance.Amount != balance {
		s.t.Fatalf("wallet %s: expected balance %s, got %d %s", walletID, balance, code, raw)
	}
	var count int
	if err := s.db.QueryRow(context.Background(), `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, walletID).Scan(&count); err != nil {
		s.t.Fatal(err)
	}
	if count != debits {
		s.t.Fatalf("wallet %s: expected %d debits, got %d", walletID, debits, count)
	}
	code, raw = post(s.t, baseURL+"/wallets/"+walletID+"/reconciliation", s.internalToken, "", "")
	var reconciliation struct {
		Consistent bool `json:"consistent"`
	}
	if err := json.Unmarshal(raw, &reconciliation); code != http.StatusOK || err != nil || !reconciliation.Consistent {
		s.t.Fatalf("wallet %s does not reconcile: %d %s", walletID, code, raw)
	}
}

func parallel(n int, fn func(int) wagerResponse) []wagerResponse {
	results := make([]wagerResponse, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = fn(i)
		}(i)
	}
	close(start)
	wg.Wait()
	return results
}

func token(t *testing.T, form url.Values) string {
	t.Helper()
	response, err := httpClient.PostForm(tokenURL, form)
	if err != nil {
		t.Skipf("Keycloak unavailable: %v", err)
	}
	defer response.Body.Close()
	var payload struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil || payload.AccessToken == "" {
		t.Fatalf("token request failed with status %d", response.StatusCode)
	}
	return payload.AccessToken
}

func post(t *testing.T, target, bearer, idempotencyKey, body string) (int, []byte) {
	request, err := http.NewRequest(http.MethodPost, target, strings.NewReader(body))
	if err != nil {
		t.Error(err)
		return 0, nil
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+bearer)
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	return do(t, request)
}

func get(t *testing.T, target, bearer string) (int, []byte) {
	request, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		t.Error(err)
		return 0, nil
	}
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	return do(t, request)
}

// do sends the request like a well-behaved client of this API: 502, 503 and 504
// are transient (a replica just died, or a dependency is briefly unavailable),
// so the same request, with the same Idempotency-Key, is sent again. Every POST
// of the API is safe to repeat.
func do(t *testing.T, request *http.Request) (int, []byte) {
	var lastErr error
	for attempt := 1; attempt <= 5; attempt++ {
		if attempt > 1 {
			time.Sleep(time.Duration(attempt) * 300 * time.Millisecond)
			if request.GetBody != nil {
				body, err := request.GetBody()
				if err != nil {
					t.Error(err)
					return 0, nil
				}
				request.Body = body
			}
		}
		response, err := httpClient.Do(request)
		if err != nil {
			lastErr = err
			continue
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		switch response.StatusCode {
		case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			lastErr = fmt.Errorf("status %d", response.StatusCode)
			if attempt < 5 {
				continue
			}
		}
		return response.StatusCode, bytes.TrimSpace(body)
	}
	t.Errorf("%s %s: %v", request.Method, request.URL, lastErr)
	return 0, nil
}

func projectDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..")
}

func compose(t *testing.T, args ...string) string {
	t.Helper()
	return run(t, "docker", append([]string{"compose"}, args...)...)
}

func docker(t *testing.T, args ...string) string {
	t.Helper()
	return run(t, "docker", args...)
}

func run(t *testing.T, name string, args ...string) string {
	t.Helper()
	command := exec.Command(name, args...)
	command.Dir = projectDir()
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, output)
	}
	return strings.TrimSpace(string(output))
}

func composeContainers(t *testing.T) []string {
	t.Helper()
	command := exec.Command("docker", "compose", "ps", "-q", "application")
	command.Dir = projectDir()
	output, err := command.Output()
	if err != nil {
		t.Skipf("docker compose is not available: %v", err)
	}
	ids := strings.Fields(string(output))
	rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	return ids
}

func restoreStack(t *testing.T) {
	compose(t, "up", "-d", "--wait")
}

func waitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if code, _ := get(t, baseURL+"/health/ready", ""); code == http.StatusOK {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatal("stack did not become ready")
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

// undeletedIDs lists the messages a replica validated but never deleted.
func undeletedIDs(t *testing.T, container, prefix string) []string {
	t.Helper()
	validated := map[string]bool{}
	for _, line := range strings.Split(docker(t, "logs", container), "\n") {
		id := messageIDIn(line, prefix)
		if id == "" {
			continue
		}
		if strings.Contains(line, `"message":"consumer_command_validated"`) {
			validated[id] = true
		} else if strings.Contains(line, `"message":"consumer_message_deleted"`) {
			delete(validated, id)
		}
	}
	ids := make([]string, 0, len(validated))
	for id := range validated {
		ids = append(ids, id)
	}
	return ids
}

// redeliveredAsDuplicate reports whether a surviving replica deleted the message
// without running the financial processing again, which is what the Inbox does
// when the original transaction had already committed.
func redeliveredAsDuplicate(t *testing.T, survivors []string, id string) bool {
	t.Helper()
	for _, container := range survivors {
		processed, deleted := false, false
		for _, line := range strings.Split(docker(t, "logs", container), "\n") {
			if messageIDIn(line, id) != id {
				continue
			}
			processed = processed || strings.Contains(line, `"message":"consumer_financial_processing_completed"`)
			deleted = deleted || strings.Contains(line, `"message":"consumer_message_deleted"`)
		}
		if deleted {
			return !processed
		}
	}
	return false
}

func messageIDIn(line, prefix string) string {
	start := strings.Index(line, `"messageId":"`+prefix)
	if start < 0 {
		return ""
	}
	id := line[start+len(`"messageId":"`):]
	return id[:strings.Index(id, `"`)]
}
