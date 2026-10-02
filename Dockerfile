FROM golang:1.27.1 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/backend-api ./cmd/api \
    && CGO_ENABLED=0 go build -o /out/migrate ./cmd/migrate

FROM gcr.io/distroless/static-debian12

WORKDIR /app
COPY --from=build /out/backend-api /app/backend-api
COPY --from=build /out/migrate /app/migrate
EXPOSE 8080
ENTRYPOINT ["/app/backend-api"]
