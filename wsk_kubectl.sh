#!/usr/bin/env bash
set -euo pipefail

exec kubectl -n openwhisk exec openwhisk-wskadmin -- /usr/local/bin/wsk "$@"
