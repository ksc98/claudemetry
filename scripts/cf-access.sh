#!/usr/bin/env bash
# Idempotently provisions all Cloudflare Access apps + policies for the
# burnage dashboard across every domain in $DOMAINS. Safe to run repeatedly:
# existing apps (matched by .domain) are reused rather than duplicated.
#
# End state (per domain D in $DOMAINS):
#   D           → Allow, "allow-any-google" (any authenticated identity)
#   D/v1/*      → Bypass, "bypass-everyone" (Claude Code API surface)
#   D/_cm/*     → Bypass, "bypass-everyone" (curl admin probes)
#
# Usage:
#   export CLOUDFLARE_API_TOKEN=<Access: Apps and Policies Edit scope>
#   export CLOUDFLARE_ACCOUNT_ID=<account id>
#   just cf-access     # or  ./scripts/cf-access.sh

set -euo pipefail

cd "$(dirname "$0")/.."
if [[ -f .env ]]; then
  set -o allexport
  # shellcheck disable=SC1091
  source .env
  set +o allexport
fi

: "${CLOUDFLARE_API_TOKEN:?set CLOUDFLARE_API_TOKEN (Access: Apps and Policies Edit scope)}"
: "${CLOUDFLARE_ACCOUNT_ID:?set CLOUDFLARE_ACCOUNT_ID}"
: "${DOMAINS:?set DOMAINS (space-separated list) in .env or the environment}"

API="https://api.cloudflare.com/client/v4/accounts/$CLOUDFLARE_ACCOUNT_ID/access"
AUTH="Authorization: Bearer $CLOUDFLARE_API_TOKEN"

api() {
  local method="$1" path="$2" body="${3-}"
  if [[ -n "$body" ]]; then
    curl -fsS -X "$method" "$API/$path" \
      -H "$AUTH" -H "Content-Type: application/json" --data "$body"
  else
    curl -fsS -X "$method" "$API/$path" -H "$AUTH"
  fi
}

find_app_by_domain() {
  local want="$1"
  api GET apps | jq -r --arg d "$want" '.result[] | select(.domain == $d) | .id' | head -1
}

ensure_app() {
  local name="$1" app_domain="$2" existing body
  existing=$(find_app_by_domain "$app_domain")
  if [[ -n "$existing" ]]; then
    echo "$existing"
    return
  fi
  body=$(jq -cn --arg name "$name" --arg domain "$app_domain" '{
    name: $name,
    domain: $domain,
    type: "self_hosted",
    session_duration: "24h",
    skip_interstitial: true,
    auto_redirect_to_identity: false
  }')
  api POST apps "$body" | jq -r .result.id
}

policy_exists() {
  local app_id="$1" policy_name="$2"
  api GET "apps/$app_id/policies" |
    jq -e --arg n "$policy_name" '.result[] | select(.name == $n)' > /dev/null
}

ensure_policy() {
  local app_id="$1" name="$2" decision="$3" body
  if policy_exists "$app_id" "$name"; then
    echo "   ✓ policy \"$name\" already present"
    return
  fi
  body=$(jq -cn --arg name "$name" --arg decision "$decision" '{
    name: $name,
    decision: $decision,
    include: [{ everyone: {} }]
  }')
  api POST "apps/$app_id/policies" "$body" > /dev/null
  echo "   + policy \"$name\" created"
}

for DOMAIN in $DOMAINS; do
  echo "→ $DOMAIN (catch-all, Google-gated)"
  app_root=$(ensure_app "$DOMAIN" "$DOMAIN")
  ensure_policy "$app_root" "allow-any-google" "allow"

  echo "→ $DOMAIN/v1/* (bypass, Anthropic API)"
  app_v1=$(ensure_app "bypass /v1/* ($DOMAIN)" "$DOMAIN/v1/*")
  ensure_policy "$app_v1" "bypass-everyone" "bypass"

  echo "→ $DOMAIN/_cm/* (bypass, admin probes)"
  app_cm=$(ensure_app "bypass /_cm/* ($DOMAIN)" "$DOMAIN/_cm/*")
  ensure_policy "$app_cm" "bypass-everyone" "bypass"
done

echo
echo "Done. Refresh the dashboard in a browser to test the Google login flow."
