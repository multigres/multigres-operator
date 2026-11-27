FROM --platform=$BUILDPLATFORM golang:1.27.4-alpine3.23@sha256:f70850ef6ce341e8b6427f677049145d63c0b7ce002d4d7e3d464828b1b96dcd AS builder

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

FROM alpine:3.24@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf

RUN apk add --no-cache curl

LABEL org.opencontainers.image.source="https://github.com/multigres/multigres-operator"
WORKDIR /
COPY --from=builder /workspace/tools/observer/multigres-observer .
USER 65532:65532

ENTRYPOINT ["/multigres-observer"]
