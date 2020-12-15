# Build stage
ARG goversion
FROM golang:${goversion}-alpine as builder
RUN apk add build-base git mercurial
ADD go.mod /spire/go.mod
RUN cd /spire && go mod download
ADD . /spire
WORKDIR /spire
RUN make bin/spire-server
RUN make bin/spire-agent
RUN make bin/k8s-workload-registrar
RUN make bin/oidc-discovery-provider
RUN make bin/wlapi-csi-driver

# Common base
FROM alpine AS spire-base
RUN apk --no-cache add dumb-init
RUN apk --no-cache add ca-certificates
RUN mkdir -p /opt/spire/bin

# SPIRE Server
FROM spire-base AS spire-server
COPY --from=builder /spire/bin/spire-server /opt/spire/bin/spire-server
WORKDIR /opt/spire
ENTRYPOINT ["/usr/bin/dumb-init", "/opt/spire/bin/spire-server", "run"]
CMD []

# SPIRE Agent
FROM spire-base AS spire-agent
RUN apk update && addgroup -S spire && adduser -S spire -G spire
USER spire
COPY --from=builder /spire/bin/spire-agent /opt/spire/bin/spire-agent
WORKDIR /opt/spire
ENTRYPOINT ["/usr/bin/dumb-init", "/opt/spire/bin/spire-agent", "run"]
CMD []

# K8S Workload Registrar
FROM spire-base AS k8s-workload-registrar
COPY --from=builder /spire/bin/k8s-workload-registrar /opt/spire/bin/k8s-workload-registrar
WORKDIR /opt/spire
ENTRYPOINT ["/usr/bin/dumb-init", "/opt/spire/bin/k8s-workload-registrar"]
CMD []

# OIDC Discovery Provider
FROM spire-base AS oidc-discovery-provider
COPY --from=builder /spire/bin/oidc-discovery-provider /opt/spire/bin/oidc-discovery-provider
WORKDIR /opt/spire
ENTRYPOINT ["/usr/bin/dumb-init", "/opt/spire/bin/oidc-discovery-provider"]
CMD []

# Workload API CSI Driver
FROM spire-base AS wlapi-csi-driver
COPY --from=builder /spire/bin/wlapi-csi-driver /opt/spire/bin/wlapi-csi-driver
WORKDIR /opt/spire
ENTRYPOINT ["/usr/bin/dumb-init", "/opt/spire/bin/wlapi-csi-driver"]
CMD []
