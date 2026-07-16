FROM --platform=$BUILDPLATFORM golang:1.26.5-alpine3.23 AS builder

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG BUILD_DATE=unknown

WORKDIR /workspace
COPY go.mod go.sum ./
COPY api/ ./api/
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

FROM alpine:3.23

RUN apk add --no-cache curl

LABEL org.opencontainers.image.source="https://github.com/multigres/multigres-operator"
WORKDIR /
COPY --from=builder /workspace/tools/observer/multigres-observer .
USER 65532:65532

ENTRYPOINT ["/multigres-observer"]
