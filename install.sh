#!/usr/bin/env bash
set -euo pipefail

REPO="ForgeAILab/forge"
PREFIX="${PREFIX:-/usr/local}"
BINARY_DIR="${BINARY_DIR:-${PREFIX}/bin}"
SHARE_DIR="${SHARE_DIR:-${PREFIX}/share/forge}"
TMP_DIR="$(mktemp -d)"
cleanup() { rm -rf "$TMP_DIR"; }
trap cleanup EXIT

can_write_target() {
    local dir="$1"
    while [ ! -e "$dir" ]; do
        dir="$(dirname "$dir")"
    done
    [ -w "$dir" ]
}

echo "==> Forge Installer"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$OS" in
    linux)  OS="linux" ;;
    darwin) OS="macos" ;;
    *)      echo "Error: unsupported OS '$OS'" >&2; exit 1 ;;
esac

case "$ARCH" in
    x86_64|amd64) ARCH="x86_64" ;;
    aarch64|arm64) ARCH="aarch64" ;;
    *)             echo "Error: unsupported architecture '$ARCH'" >&2; exit 1 ;;
esac

LIBC_SUFFIX=""
if [ "$OS" = "linux" ]; then
    LIBC="${FORGE_LIBC:-}"
    if [ -z "$LIBC" ] && command -v ldd >/dev/null 2>&1; then
        LDD_OUTPUT="$(ldd --version 2>&1 || true)"
        if printf '%s' "$LDD_OUTPUT" | grep -qi musl; then
            LIBC="musl"
        fi
    fi

    case "$LIBC" in
        musl) LIBC_SUFFIX="-musl" ;;
        gnu|"") ;;
        *) echo "Error: unsupported FORGE_LIBC '$LIBC' (expected 'gnu' or 'musl')" >&2; exit 1 ;;
    esac
fi

ARTIFACT="forge-${ARCH}-${OS}${LIBC_SUFFIX}"
URL="https://github.com/${REPO}/releases/latest/download/${ARTIFACT}.tar.gz"

echo "    OS:   ${OS}"
echo "    Arch: ${ARCH}"
if [ "$OS" = "linux" ]; then
    echo "    Libc: ${LIBC:-gnu}"
fi
echo "    Fetching: ${URL}"

if ! curl -fsSL "$URL" -o "${TMP_DIR}/${ARTIFACT}.tar.gz"; then
    echo "Error: failed to download ${URL}" >&2
    echo "There may not be a release for this platform yet." >&2
    exit 1
fi

tar -xzf "${TMP_DIR}/${ARTIFACT}.tar.gz" -C "$TMP_DIR"

echo "==> Installing forge, forge-ctl, and forge-solo to ${BINARY_DIR}"

install_mode=""
if ! can_write_target "$BINARY_DIR" || ! can_write_target "$SHARE_DIR"; then
    install_mode="sudo"
    echo "    (requires sudo for ${PREFIX})"
fi

$install_mode mkdir -p "$BINARY_DIR"
$install_mode install -m 755 "${TMP_DIR}/forge" "${BINARY_DIR}/forge"
$install_mode install -m 755 "${TMP_DIR}/forge-ctl" "${BINARY_DIR}/forge-ctl"
$install_mode install -m 755 "${TMP_DIR}/forge-solo" "${BINARY_DIR}/forge-solo"

if [ -d "${TMP_DIR}/web/dist" ]; then
    echo "==> Installing web UI assets to ${SHARE_DIR}/web/dist"
    $install_mode mkdir -p "${SHARE_DIR}/web"
    $install_mode rm -rf "${SHARE_DIR}/web/dist"
    $install_mode cp -R "${TMP_DIR}/web/dist" "${SHARE_DIR}/web/dist"
else
    echo "Warning: release archive did not include web/dist assets" >&2
fi

echo "==> Installed:"
echo "    forge     -> ${BINARY_DIR}/forge"
echo "    forge-ctl -> ${BINARY_DIR}/forge-ctl"
echo "    forge-solo -> ${BINARY_DIR}/forge-solo"
echo "    web UI    -> ${SHARE_DIR}/web/dist"
echo ""
echo "Run 'forge --help' for the server or 'forge-solo --help' for the interactive Git-repository TUI."
