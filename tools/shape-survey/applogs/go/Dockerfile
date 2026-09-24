FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod main.go /src/
# `go mod tidy` rather than a committed go.sum, which nobody here would review. Both modules are
# pinned in go.mod.
RUN go mod tidy && CGO_ENABLED=0 go build -o /out/app .

FROM alpine:3.21
COPY --from=build /out/app /app
