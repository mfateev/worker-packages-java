#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
sdk_dir=${TEMPORAL_SDK_DIR:-"$project_root/../sdk-java"}
# Use the developer's existing checkout as-is; clone main only when absent.
if [[ ! -d "$sdk_dir" ]]; then
  git clone --branch main --recurse-submodules https://github.com/temporalio/sdk-java.git "$sdk_dir"
fi
