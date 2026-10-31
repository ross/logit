#!/bin/bash
# Runs in the pinned `amazon/aws-cli` container: lists and fetches what the legs wrote to one
# store, so the verdict doesn't rest on `logit`'s own signer. Not run directly; see
# script/s3-interop.
#
# Reads AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, S3_INTEROP_ENDPOINT,
# S3_INTEROP_BUCKET, S3_INTEROP_LATE_BUCKET, S3_INTEROP_RUN_PREFIX, and S3_INTEROP_PATH_STYLE.
# Writes to /out:
#
#   objects/<key>        every object under the run prefix, in the bucket, fetched whole
#   late-objects/<key>   the same from the late bucket
#   listing.json         `list-objects-v2` over the run prefix
#   late-listing.json    `list-objects-v2` over the run prefix in the late bucket
#   head/<leg>.json      `head-object` of the leg's first object, or its error text
set -uo pipefail

# The checksum defaults of recent CLI versions add trailers that older S3-compatible stores
# reject; the legs under test send none.
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required
export AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
export AWS_EC2_METADATA_DISABLED=true
if [ "${S3_INTEROP_PATH_STYLE}" = true ]; then
    aws configure set default.s3.addressing_style path
fi
s3api() { aws --endpoint-url "${S3_INTEROP_ENDPOINT}" s3api "$@"; }

mkdir -p /out/objects /out/head
s3api list-objects-v2 --bucket "${S3_INTEROP_BUCKET}" --prefix "${S3_INTEROP_RUN_PREFIX}" \
    >/out/listing.json 2>/out/listing.err || true
s3api list-objects-v2 --bucket "${S3_INTEROP_LATE_BUCKET}" --prefix "${S3_INTEROP_RUN_PREFIX}" \
    >/out/late-listing.json 2>/out/late-listing.err || true

# A listing's keys, one per line. A key holds no whitespace.
list_keys() {
    python3 - "$1" <<'PY' 2>/dev/null || true
import json, sys
try:
    for item in json.load(open(sys.argv[1])).get("Contents", []):
        print(item["Key"])
except (OSError, ValueError):
    pass
PY
}
keys="$(list_keys /out/listing.json)"
declare -A first
for key in ${keys}; do
    mkdir -p "/out/objects/$(dirname "${key}")"
    s3api get-object --bucket "${S3_INTEROP_BUCKET}" --key "${key}" "/out/objects/${key}" \
        >/dev/null 2>>/out/get.err || echo "get ${key} failed" >>/out/get.err
    leg="${key#"${S3_INTEROP_RUN_PREFIX}"}"
    leg="${leg%%/*}"
    if [ -z "${first[${leg}]:-}" ]; then
        first[${leg}]=1
        s3api head-object --bucket "${S3_INTEROP_BUCKET}" --key "${key}" \
            >"/out/head/${leg}.json" 2>"/out/head/${leg}.err" || true
    fi
done

# The late bucket's objects, for the missing-bucket leg.
for key in $(list_keys /out/late-listing.json); do
    mkdir -p "/out/late-objects/$(dirname "${key}")"
    s3api get-object --bucket "${S3_INTEROP_LATE_BUCKET}" --key "${key}" "/out/late-objects/${key}" \
        >/dev/null 2>>/out/get.err || echo "get late ${key} failed" >>/out/get.err
done
chmod -R a+rwX /out
