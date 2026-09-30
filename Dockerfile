FROM golang:1.27.1 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/backend-api ./cmd/api

FROM gcr.io/distroless/static-debian12

WORKDIR /app
COPY --from=build /out/backend-api /app/backend-api
EXPOSE 8080
ENTRYPOINT ["/app/backend-api"]
