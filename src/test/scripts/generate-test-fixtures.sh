#!/usr/bin/env bash
# Creates the small MP3 fixtures needed by MetadataExtractorSpec and TranscoderSpec.
# Run once from the repo root (pipeline/), then commit the generated files.
#
# Prerequisites (same as running the pipeline locally):
#   macOS:  brew install ffmpeg
#   Ubuntu: apt-get install ffmpeg
set -euo pipefail

OUT=src/test/resources/fixtures
mkdir -p "$OUT"

# 1-second silent MP3 with known ID3 tags
ffmpeg -y \
  -f lavfi -i "sine=frequency=440:duration=1" \
  -ar 44100 -ac 1 -b:a 128k \
  -id3v2_version 3 \
  -metadata title="Test Title" \
  -metadata artist="Test Artist" \
  -metadata album="Test Album" \
  -metadata track="1" \
  -metadata date="2024" \
  -metadata genre="Test" \
  "$OUT/tagged.mp3"

# 1-second silent MP3 with all metadata stripped
ffmpeg -y \
  -f lavfi -i "sine=frequency=440:duration=1" \
  -ar 44100 -ac 1 -b:a 128k \
  -map_metadata -1 \
  "$OUT/untagged.mp3"

echo "Done — commit $OUT/tagged.mp3 and $OUT/untagged.mp3"
