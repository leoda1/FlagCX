#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <torch-api|perf>" >&2
  exit 2
fi

workload=$1
case "$workload" in
  torch-api|perf) ;;
  *)
    echo "Unknown PPU workload: $workload" >&2
    exit 2
    ;;
esac

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
project_root=$(cd "$script_dir/../../.." && pwd)
image=${FLAGCX_CI_IMAGE:-harbor.baai.ac.cn/flagos-dev/flagcx:manual-20260827-ppu-dev}
container_name=${FLAGCX_CI_CONTAINER_NAME:-flagcx-ppu-${workload//-/_}-$$}

docker_args=(
  --network=host
  --shm-size=100gb
  --ulimit=memlock=-1
  --ulimit=stack=67108864
  --device=/dev/alixpu
  --device=/dev/alixpu_ctl
  --device=/dev/alixpu_ppu0
  --device=/dev/alixpu_ppu1
  --device=/dev/alixpu_ppu2
  --device=/dev/alixpu_ppu3
  --device=/dev/alixpu_ppu4
  --device=/dev/alixpu_ppu5
  --device=/dev/alixpu_ppu6
  --device=/dev/alixpu_ppu7
  --device=/dev/alixpu_ppu8
  --device=/dev/alixpu_ppu9
  --device=/dev/alixpu_ppu10
  --device=/dev/alixpu_ppu11
  --device=/dev/alixpu_ppu12
  --device=/dev/alixpu_ppu13
  --device=/dev/alixpu_ppu14
  --device=/dev/alixpu_ppu15
  --device=/dev/infiniband/rdma_cm
  --device=/dev/infiniband/uverbs0
  --device=/dev/infiniband/uverbs1
  --device=/dev/infiniband/uverbs2
  --device=/dev/infiniband/uverbs3
  --device=/dev/infiniband/fic2_soe_ucmd0
  --device=/dev/infiniband/fic2_soe_ucmd1
  --device=/dev/infiniband/fic2_soe_ucmd2
  --device=/dev/infiniband/fic2_soe_ucmd3
)

for proxy_var in HTTP_PROXY HTTPS_PROXY NO_PROXY http_proxy https_proxy no_proxy; do
  if [[ -n "${!proxy_var:-}" ]]; then
    docker_args+=(--env "$proxy_var")
  fi
done

cleanup() {
  docker rm --force "$container_name" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cleanup
docker pull "$image"
docker run --rm \
  --name "$container_name" \
  "${docker_args[@]}" \
  --user=root \
  --volume "$project_root:/workspace-src:ro" \
  --workdir /workspace \
  --env HOME=/tmp/flagcx-ci-home \
  --env GITHUB_WORKSPACE=/workspace \
  --env GITHUB_ACTIONS=true \
  --env CI=true \
  --env FLAGCX_CI_WORKLOAD="$workload" \
  --entrypoint=/bin/bash \
  "$image" -lc '
    set -euo pipefail
    mkdir -p "$HOME" /workspace
    cp -a /workspace-src/. /workspace/
    git config --global --add safe.directory /workspace
    git config --global --add safe.directory /workspace/third-party/googletest
    git config --global --add safe.directory /workspace/third-party/json
    bash .github/scripts/ci/run_ppu_workload.sh "$FLAGCX_CI_WORKLOAD"
  '
