#!/usr/bin/env bash
set -eu
# Give inference and its child tests only the loopback network interface.
exec sudo ip netns exec qwen-live sudo -u "$(id -un)" env -i \
  PATH="$PATH" HOME="$RUNNER_TEMP/live-home" USER="$(id -un)" \
  RUNNER_TEMP="$RUNNER_TEMP" LANG=C.UTF-8 CI=true \
  OLLAMA_HOST=127.0.0.1:11434 OLLAMA_MODELS="$RUNNER_TEMP/models" \
  OLLAMA_NO_CLOUD=1 OLLAMA_NOPRUNE=1 OLLAMA_CONTEXT_LENGTH=16384 \
  OLLAMA_NUM_PARALLEL=1 OLLAMA_MAX_LOADED_MODELS=1 OLLAMA_KEEP_ALIVE=4h \
  "$@"
