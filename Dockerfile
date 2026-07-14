# Containerfile for multigres-operator

FROM --platform=$BUILDPLATFORM golang:1.26.0-alpine3.23 AS builder

ARG TARGETOS
ARG TARGETARCH

# Version metadata injected at build time via --build-arg.
# Defaults ensure a plain `docker build .` still works.
ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG BUILD_DATE=unknown

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the Go source (relies on .dockerignore to filter)
COPY . .

# Build using Go's native cross-compiler (CGO_ENABLED=0).
# --platform=$BUILDPLATFORM on the FROM makes this stage run on the host arch
# (amd64) even when targeting arm64, avoiding slow QEMU emulation.
#
# We produce two static binaries from the same source tree so the same image
# can be used as the operator's deployment AND as the CronJob that runs the
# multigres garbage collector (cmd/multigres-gc).
RUN CGO_ENABLED=0 \
	GOOS=${TARGETOS:-linux} \
	GOARCH=${TARGETARCH} \
	go build \
	-ldflags "-s -w -buildid= \
	-X main.version=${VERSION} \
	-X main.gitCommit=${GIT_COMMIT} \
	-X main.buildDate=${BUILD_DATE}" \
	-trimpath -mod=readonly \
	-a -o manager \
	cmd/multigres-operator/main.go

RUN CGO_ENABLED=0 \
	GOOS=${TARGETOS:-linux} \
	GOARCH=${TARGETARCH} \
	go build \
	-ldflags "-s -w -buildid= \
	-X main.version=${VERSION} \
	-X main.gitCommit=${GIT_COMMIT} \
	-X main.buildDate=${BUILD_DATE}" \
	-trimpath -mod=readonly \
	-a -o multigres-gc \
	cmd/multigres-gc/main.go

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/static:nonroot

LABEL org.opencontainers.image.source="https://github.com/multigres/multigres-operator"
WORKDIR /
COPY --from=builder /workspace/manager .
COPY --from=builder /workspace/multigres-gc .
USER 65532:65532

ENTRYPOINT ["/manager"]
