FROM --platform=$BUILDPLATFORM golang:1.27.4-alpine3.23@sha256:146e3b9c7c1e2ec3d3f5348c8a686151a75e1976e50efac9fc022ebe446a9a82 AS builder

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG BUILD_DATE=unknown

ENV GOTOOLCHAIN=local

WORKDIR /workspace
COPY go.mod go.sum ./
COPY api/ ./api/
COPY pkg/ ./pkg/
COPY tools/observer/go.mod tools/observer/go.sum ./tools/observer/

WORKDIR /workspace/tools/observer
RUN go mod download

COPY tools/observer/ ./

RUN CGO_ENABLED=0 \
	GOOS=${TARGETOS:-linux} \
	GOARCH=${TARGETARCH} \
	go build \
	-ldflags "-s -w -buildid= \
	-X main.version=${VERSION} \
	-X main.gitCommit=${GIT_COMMIT} \
	-X main.buildDate=${BUILD_DATE}" \
	-trimpath -mod=readonly \
	-a -o multigres-observer \
	cmd/multigres-observer/main.go

FROM alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a

RUN apk add --no-cache curl

LABEL org.opencontainers.image.source="https://github.com/multigres/multigres-operator"
WORKDIR /
COPY --from=builder /workspace/tools/observer/multigres-observer .
USER 65532:65532

ENTRYPOINT ["/multigres-observer"]
