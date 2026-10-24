# syntax=docker/dockerfile:1
FROM node:22-alpine AS web
WORKDIR /src/web
COPY web/package.json web/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY web/ ./
RUN npm run build

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=web /src/web/dist ./web/dist
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/nebula ./cmd/nebula

FROM alpine:3.20
RUN apk add --no-cache ca-certificates docker-cli wget
COPY --from=build /out/nebula /usr/local/bin/nebula
EXPOSE 7000 8000
VOLUME /data
ENTRYPOINT ["nebula"]
CMD ["node", "--help"]
