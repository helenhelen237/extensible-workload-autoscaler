#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit -o nounset -o pipefail

 
PROTOC_VERSION="36.1" # https://github.com/protocolbuffers/protobuf/releases/tag/v36.1

linux_x86_64_EXPECTED_SHA="c4bc672d9d49214dc8cafdceadf4df92182d6ca8e3ec65a56b2d7de5602669b4"
linux_aarch_64_EXPECTED_SHA="237a68856edf1bd28b6204bddd0596c1cf46d298bc29c620012540b2e44c73e7"
osx_x86_64_EXPECTED_SHA="ee2c5496e4af0aa6a224894bc0f7025145260e004d890487d510725ce8b473eb"
osx_aarch_64_EXPECTED_SHA="de56d57afe30c5d191b11d24ff93dd4025728d7fb43b773886b2d3613e0bdbb2"


OS_NAME=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH_NAME=$(uname -m)

#  Detecting Host OS and CPU Architecture. Only osx and linux are supported
if [ "$OS_NAME" = "darwin" ]; then
  if [ "$ARCH_NAME" = "x86_64" ]; then
    PROTOC_PLATFORM="osx-x86_64"
  else
    PROTOC_PLATFORM="osx-aarch_64"
  fi
elif [ "$OS_NAME" = "linux" ]; then
  if [ "$ARCH_NAME" = "x86_64" ]; then
    PROTOC_PLATFORM="linux-x86_64"
  elif [ "$ARCH_NAME" = "aarch64" ] || [ "$ARCH_NAME" = "arm64" ]; then
    PROTOC_PLATFORM="linux-aarch_64"
  fi
else
  echo "Unsupported operating system: ${OS_NAME}" >&2
  exit 1
fi

# Target folder for local protoc binary
OUT_DIR="$(dirname "$0")/../bin/protoc-${PROTOC_VERSION}" # done to ensure that the pinned versioned is used
PROTOC_BIN="$OUT_DIR/bin/protoc"

if [ ! -f "$PROTOC_BIN" ]; then
  echo "Downloading protoc v${PROTOC_VERSION} for ${PROTOC_PLATFORM}..."
  mkdir -p "$OUT_DIR"
  URL="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-${PROTOC_PLATFORM}.zip"

  case "$PROTOC_PLATFORM" in
    ("linux-x86_64") EXPECTED_SHA="${linux_x86_64_EXPECTED_SHA}";;
    ("linux-aarch_64") EXPECTED_SHA="${linux_aarch_64_EXPECTED_SHA}";;
    ("osx-x86_64") EXPECTED_SHA="${osx_x86_64_EXPECTED_SHA}";;
    ("osx-aarch_64") EXPECTED_SHA="${osx_aarch_64_EXPECTED_SHA}";;
    (*) echo "Unknown platform $PROTOC_PLATFORM"; exit 1 ;;
  esac

# Download and verify
curl -sSL "$URL" -o "$OUT_DIR/protoc.zip"
if command -v sha256sum >/dev/null 2>&1; then
  ACTUAL_SHA=$(sha256sum "$OUT_DIR/protoc.zip" | awk '{print $1}')
else
  ACTUAL_SHA=$(shasum -a 256 "$OUT_DIR/protoc.zip" | awk '{print $1}')
fi

if [ "$ACTUAL_SHA" != "$EXPECTED_SHA" ]; then
  echo "Checksum verification failed for protoc download!"
  echo "Expected: $EXPECTED_SHA"
  echo "Got:      $ACTUAL_SHA"
  exit 1
fi

  unzip -q -o "$OUT_DIR/protoc.zip" -d "$OUT_DIR"
  rm -f "$OUT_DIR/protoc.zip"
  chmod +x "$PROTOC_BIN"
  echo "Local protoc installed at $PROTOC_BIN"
fi


exec "$PROTOC_BIN" "$@"