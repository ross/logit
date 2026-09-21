FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod main.go /src/
# `go mod tidy` rather than a committed go.sum: two modules, both pinned in go.mod, and a go.sum
# here would be a hash file nobody in this repo reviews. The resolved versions are recorded in the
# run's provenance.txt instead.
RUN go mod tidy && CGO_ENABLED=0 go build -o /out/app .

FROM alpine:3.21
COPY --from=build /out/app /app
