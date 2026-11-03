FROM --platform=$BUILDPLATFORM golang:1.27.3-alpine3.23@sha256:8a5046497cda39860b832ffdbe8dd66018a0f05bb01352f533a36e7e2ced66a0 AS builder

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

FROM alpine:3.23@sha256:ff7f0c727dd796aa36787bfcd9a64bf35a6ce72a73451b3359fde849b6703d63

RUN apk add --no-cache curl

LABEL org.opencontainers.image.source="https://github.com/multigres/multigres-operator"
WORKDIR /
COPY --from=builder /workspace/tools/observer/multigres-observer .
USER 65532:65532

ENTRYPOINT ["/multigres-observer"]
