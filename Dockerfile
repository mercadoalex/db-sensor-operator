# Build the manager binary
# Override BASE_IMAGE to build from another registry, e.g. docker.io/library/golang:1.26
ARG BASE_IMAGE=golang:1.26
FROM ${BASE_IMAGE} AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the Go source (relies on .dockerignore to filter)
COPY . .

# Build
# the GOARCH has no default value to allow the binary to be built according to the host where the command
# was called. For example, if we call make docker-build in a local env which has the Apple Silicon M1 SO
# the docker BUILDPLATFORM arg will be linux/arm64 when for Apple x86 it will be linux/amd64. Therefore,
# by leaving it empty we can ensure that the container and binary shipped on it will have the same platform.
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -o manager cmd/main.go

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/manager .
USER 65532:65532

# Port documentation (EXPOSE is informational only — distroless has no runtime
# that acts on it, and the actual bind addresses are set via manager flags such
# as --operator-http-bind-address, --health-probe-bind-address and
# --metrics-bind-address). The manager runs as nonroot (USER 65532 above).
#   8080  operator HTTP server: /metrics (always on), /dashboard + /api/events
#         (gated by dashboard.enabled), /cdc, /healthz, /readyz
#   8081  manager health probes: /healthz, /readyz
#   8443  manager kube-RBAC-protected metrics
EXPOSE 8080
EXPOSE 8081
EXPOSE 8443

ENTRYPOINT ["/manager"]
