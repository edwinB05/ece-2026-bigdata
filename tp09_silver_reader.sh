#!/usr/bin/env bash
# Lab 09 - exercise 2: catalog role "silver_reader" limited to the "silver" namespace.
# Creates principal silver_tester -> principal role silver_reader_role -> catalog role silver_reader.
# Uses the demo root secret of lab 1. The new credential is appended to ~/.polaris-credentials.env (never printed).
set -euo pipefail
POLARIS_URI="${POLARIS_URI:-http://polaris:8181}"
API="$POLARIS_URI/api/management/v1"
POLARIS_CATALOG="${POLARIS_CATALOG:-$KUBERNETES_NAMESPACE}"
CRED_FILE=/home/onyxia/.polaris-credentials.env
TOKEN=$(curl -s "$POLARIS_URI/api/catalog/v1/oauth/tokens" -d grant_type=client_credentials \
  -d client_id=root -d client_secret=s3cr3t -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
H=(-H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json")

RESP=$(curl -s -X POST "$API/principals" "${H[@]}" \
  -d '{"principal":{"name":"silver_tester","properties":{}},"credentialRotationRequired":false}')
echo "$RESP" | jq -e .credentials >/dev/null || RESP=$(curl -s -X POST "$API/principals/silver_tester/reset" "${H[@]}")
grep -v SILVER_TESTER "$CRED_FILE" > "$CRED_FILE.tmp" || true
mv "$CRED_FILE.tmp" "$CRED_FILE"
echo "$RESP" | jq -r '"POLARIS_SILVER_TESTER_CREDENTIAL=\"\(.credentials.clientId):\(.credentials.clientSecret)\""' >> "$CRED_FILE"

curl -s -X POST "$API/principal-roles" "${H[@]}" -d '{"principalRole":{"name":"silver_reader_role","properties":{}}}' >/dev/null
curl -s -X POST "$API/catalogs/$POLARIS_CATALOG/catalog-roles" "${H[@]}" -d '{"catalogRole":{"name":"silver_reader","properties":{}}}' >/dev/null
for P in NAMESPACE_LIST NAMESPACE_READ_PROPERTIES TABLE_LIST TABLE_READ_PROPERTIES TABLE_READ_DATA; do
  curl -s -X PUT "$API/catalogs/$POLARIS_CATALOG/catalog-roles/silver_reader/grants" "${H[@]}" \
    -d "{\"grant\":{\"type\":\"namespace\",\"namespace\":[\"silver\"],\"privilege\":\"$P\"}}" -o /dev/null -w "$P %{http_code}\n"
done
curl -s -X PUT "$API/principals/silver_tester/principal-roles" "${H[@]}" \
  -d '{"principalRole":{"name":"silver_reader_role"}}' -o /dev/null -w "assign principal role %{http_code}\n"
curl -s -X PUT "$API/principal-roles/silver_reader_role/catalog-roles/$POLARIS_CATALOG" "${H[@]}" \
  -d '{"catalogRole":{"name":"silver_reader"}}' -o /dev/null -w "assign catalog role %{http_code}\n"
curl -s "$API/catalogs/$POLARIS_CATALOG/catalog-roles/silver_reader/grants" "${H[@]}" | jq -c '.grants[]'
