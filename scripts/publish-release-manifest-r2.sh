#!/bin/sh
# Publish the generated release manifest to Cloudflare R2.
#
# Layout (bucket looper-releases, public host releases.looper.powerformer.com):
#   <tag>/manifest.json       immutable per-release copy
#   channels/<channel>.json   mutable latest pointer for that channel
#   manifest.json             alias of the latest stable pointer
#
# Required: wrangler (or npx wrangler) authenticated to the Powerformer
# Cloudflare account. CI supplies CLOUDFLARE_API_TOKEN + CLOUDFLARE_ACCOUNT_ID.

set -eu

BUCKET="${R2_RELEASES_BUCKET:-looper-releases}"
MANIFEST="${RELEASE_MANIFEST:-release-assets/manifest.json}"
CONTENT_TYPE="application/json; charset=utf-8"
VERSIONED_CACHE="public, max-age=31536000, immutable"
POINTER_CACHE="public, max-age=60"

if [ ! -f "$MANIFEST" ]; then
  echo "release manifest not found: $MANIFEST" >&2
  exit 1
fi

read_manifest_field() {
  python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$MANIFEST" "$1"
}

TAG="${RELEASE_TAG:-$(read_manifest_field tag)}"
CHANNEL="${RELEASE_CHANNEL:-$(read_manifest_field channel)}"

if [ -z "$TAG" ] || [ -z "$CHANNEL" ]; then
  echo "manifest is missing tag or channel" >&2
  exit 1
fi

wrangler_bin() {
  if command -v wrangler >/dev/null 2>&1; then
    wrangler "$@"
    return
  fi
  npx --yes wrangler@4 "$@"
}

put_object() {
  key="$1"
  cache="$2"
  echo "uploading $MANIFEST -> r2://${BUCKET}/${key}"
  wrangler_bin r2 object put "${BUCKET}/${key}" \
    --file "$MANIFEST" \
    --content-type "$CONTENT_TYPE" \
    --cache-control "$cache" \
    --remote
}

# Return 0 if the channel pointer should be replaced with TAG.
# Missing pointer or an older/equal existing tag → update.
# A strictly newer existing tag → skip, so a rerun of an old release
# cannot pin upgrades backwards. Reads R2 directly so CDN max-age=60
# cannot feed a stale pointer into the check.
should_update_pointer() {
  channel="$1"
  incoming_tag="$2"
  existing_file="$(mktemp)"
  get_err="$(mktemp)"
  key="channels/${channel}.json"
  if wrangler_bin r2 object get "${BUCKET}/${key}" --file "$existing_file" --remote 2>"$get_err"; then
    rm -f "$get_err"
  else
    if grep -Eqi 'not found|does not exist|404|NoSuchKey' "$get_err"; then
      rm -f "$existing_file" "$get_err"
      return 0
    fi
    cat "$get_err" >&2
    rm -f "$existing_file" "$get_err"
    echo "failed to read existing channel pointer r2://${BUCKET}/${key}" >&2
    exit 1
  fi

  python_status=0
  python3 - "$existing_file" "$incoming_tag" <<'PY' || python_status=$?
import json, re, sys

def version_key(tag):
    tag = str(tag or "").strip()
    if tag.startswith("v"):
        tag = tag[1:]
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?$", tag)
    if match is None:
        return None
    prerelease = match.group(4)
    if prerelease is None:
        pre_key = (1,)
    else:
        parts = []
        for part in prerelease.split("."):
            if part.isdigit():
                parts.append((0, int(part), ""))
            else:
                parts.append((1, 0, part))
        pre_key = (0,) + tuple(parts)
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)), pre_key)

existing = json.load(open(sys.argv[1], encoding="utf-8"))
existing_key = version_key(existing.get("tag") or existing.get("version") or "")
incoming_key = version_key(sys.argv[2])
if existing_key is None or incoming_key is None:
    sys.stderr.write("cannot compare channel pointer versions\n")
    raise SystemExit(2)
raise SystemExit(0 if incoming_key >= existing_key else 1)
PY
  rm -f "$existing_file"
  case "$python_status" in
    0)
      return 0
      ;;
    1)
      return 1
      ;;
    *)
      echo "failed to compare channel pointer versions" >&2
      exit 1
      ;;
  esac
}


put_object "${TAG}/manifest.json" "$VERSIONED_CACHE"

if should_update_pointer "$CHANNEL" "$TAG"; then
  put_object "channels/${CHANNEL}.json" "$POINTER_CACHE"
  if [ "$CHANNEL" = "stable" ]; then
    put_object "manifest.json" "$POINTER_CACHE"
  fi
else
  echo "skipping channel pointer update: existing ${CHANNEL} pointer is newer than ${TAG}"
fi
