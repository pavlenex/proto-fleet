#!/usr/bin/env bash

set -euo pipefail

echo "Starting Proto Fleet development environment..."
GIT_VERSION=$(git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "development")

# The Translator runs on the Compose network during development, but its SV1
# listeners are published on the host so physical LAN miners can use the same
# assignment flow as production. Advertise the host's default-route IPv4
# address unless the operator selected a specific interface/address.
detect_lan_ipv4() {
  case "$(uname -s)" in
    Darwin)
      local default_interface
      default_interface="$(route -n get default 2>/dev/null | awk '/interface:/{print $2; exit}')"
      if [[ -n "$default_interface" ]]; then
        ipconfig getifaddr "$default_interface" 2>/dev/null || true
      fi
      ;;
    Linux)
      ip -4 route get 1.1.1.1 2>/dev/null |
        awk '{for (i = 1; i <= NF; i++) if ($i == "src") {print $(i + 1); exit}}' ||
        true
      ;;
  esac
}

if [[ -z "${SV2_TRANSLATOR_ADVERTISE_HOST:-}" ]]; then
  if detected_lan_ipv4="$(detect_lan_ipv4)" && [[ -n "$detected_lan_ipv4" ]]; then
    export SV2_TRANSLATOR_ADVERTISE_HOST="$detected_lan_ipv4"
    echo "SV2 Translator will advertise ${SV2_TRANSLATOR_ADVERTISE_HOST} to miners"
  else
    echo "Warning: could not detect a LAN IPv4 address; SV2 Translator will remain Docker-only."
    echo "Set SV2_TRANSLATOR_ADVERTISE_HOST to test with physical miners."
  fi
fi

echo "Starting ProtoFleet client..."
# The Alerts settings nav is behind VITE_ALERTS_ENABLED; surface it
# only when the server is started with the alerts sidecar.
ALERTS_ENABLED=$([[ "${ENABLE_BETA_ALERTS:-}" = "true" ]] && echo "true" || echo "false")
(
  cd client
  VITE_VERSION="$GIT_VERSION" \
  VITE_BUILD_DATE="$BUILD_DATE" \
  VITE_COMMIT="$GIT_COMMIT" \
  VITE_ALERTS_ENABLED="$ALERTS_ENABLED" \
  npm run dev:protoFleet
) & CLIENT_PID=$!
echo "Client PID: $CLIENT_PID"

function start_server() {
  if [[ "${ENABLE_BETA_ALERTS:-}" = "true" ]]; then
    just dev-alerts
  else
    just dev
  fi
}

echo "Starting server..."
(cd server && start_server) & SERVER_PID=$!
echo "Server PID: $SERVER_PID"

echo "Both processes started. Press Ctrl+C to stop both processes"

cleanup() {
    echo "Stopping processes..."
    kill $CLIENT_PID $SERVER_PID 2>/dev/null || true
    wait
    echo "All processes stopped"
}

trap cleanup EXIT INT TERM

wait 
