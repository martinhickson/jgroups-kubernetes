#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${ROOT_DIR}/.bin"
export PATH="${BIN_DIR}:${PATH}"

CLUSTER_NAME="${KUBERNETES_IT_CLUSTER:-kubernetes-it}"
NAMESPACE="${KUBERNETES_IT_NAMESPACE:-kubernetes-it}"
CONTEXT="kind-${CLUSTER_NAME}"
IMAGE="${KUBERNETES_IT_IMAGE:-kubernetes-it-jgroups-member:it}"

mkdir -p "${BIN_DIR}"

if ! command -v kind >/dev/null 2>&1; then
  curl -fsSL -o "${BIN_DIR}/kind" "https://kind.sigs.k8s.io/dl/v0.27.0/kind-linux-amd64"
  chmod +x "${BIN_DIR}/kind"
fi

if ! command -v kubectl >/dev/null 2>&1; then
  curl -fsSL -o "${BIN_DIR}/kubectl" "https://dl.k8s.io/release/$(curl -fsSL https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
  chmod +x "${BIN_DIR}/kubectl"
fi

if ! kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
  kind create cluster --name "${CLUSTER_NAME}" --wait 5m
fi

echo "Building member jar and image ${IMAGE}..."
mvn -q -f "${ROOT_DIR}/pom.xml" package -DskipTests
docker build -t "${IMAGE}" -f "${ROOT_DIR}/docker/Dockerfile" "${ROOT_DIR}"
kind load docker-image "${IMAGE}" --name "${CLUSTER_NAME}"

echo "Applying manifests to namespace ${NAMESPACE}..."
kubectl --context "${CONTEXT}" apply -f "${ROOT_DIR}/src/test/resources/k8s/"
kubectl --context "${CONTEXT}" get namespace "${NAMESPACE}"
kubectl --context "${CONTEXT}" -n "${NAMESPACE}" rollout status statefulset/kubernetes-it-member --timeout=300s

echo "Cluster '${CLUSTER_NAME}' ready. Inspect logs with:"
echo "  kubectl --context ${CONTEXT} -n ${NAMESPACE} logs -l kubernetes-it=jgroups --tail=100"
