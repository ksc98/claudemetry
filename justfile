set dotenv-load := true

default:
    @just --list

# -------- bootstrap --------

# Fresh-clone setup: install mise-pinned toolchains + cargo-only extras
# (wasm32 target, worker-build). Assumes `mise` is installed and activated.
setup-mise:
    mise trust
    mise install
    rustup target add wasm32-unknown-unknown
    cargo install worker-build --locked

# -------- burnage-api (Rust worker) --------

local:
    npx --yes wrangler@latest dev --local --port 8787

local-tee log="proxy.log":
    npx --yes wrangler@latest dev --local --port 8787 2>&1 | tee {{log}}

build:
    worker-build --release

login:
    npx --yes wrangler@latest login

# Expand __PROXY_ROUTES__ in wrangler.toml into one {/v1/*, /_cm/*} pair
# per domain in $DOMAINS (space-separated; first = primary). Deploys from
# the generated file, then drops it. Keeps hostnames out of source.
deploy-api:
    #!/usr/bin/env bash
    set -euo pipefail
    : "${DOMAINS:?set DOMAINS (space-separated list, first = primary) in .env}"
    routes=""
    for d in $DOMAINS; do
        routes+="  { pattern = \"${d}/v1/*\", zone_name = \"${d}\" },"$'\n'
        routes+="  { pattern = \"${d}/_cm/*\", zone_name = \"${d}\" },"$'\n'
    done
    routes="${routes%$'\n'}"
    awk -v r="$routes" '$0 == "__PROXY_ROUTES__" { print r; next } { print }' \
        wrangler.toml > wrangler.deploy.toml
    trap 'rm -f wrangler.deploy.toml' EXIT
    npx --yes wrangler@latest deploy -c wrangler.deploy.toml

tail:
    npx --yes wrangler@latest tail --format pretty

clean:
    rm -rf build target .wrangler

# -------- burnage-frontend (Astro dashboard) --------

dashboard-dev:
    cd dashboard && pnpm dev

# Astro's CF adapter produces dist/server/wrangler.json at build time
# with main/assets/bindings auto-filled. We rewrite its `.routes` array
# via jq from $DOMAINS (one custom_domain entry per domain) and deploy
# from it. Source wrangler.jsonc stays lean so the vite-plugin doesn't
# try to resolve a nonexistent `main` during the build phase. The
# primary domain (first in $DOMAINS) is passed as env.DOMAIN at runtime
# so the dashboard can show the correct proxy URL.
deploy-frontend:
    #!/usr/bin/env bash
    set -euo pipefail
    : "${DOMAINS:?set DOMAINS (space-separated list, first = primary) in .env}"
    primary="${DOMAINS%% *}"
    cd dashboard
    pnpm build
    routes_json=$(for d in $DOMAINS; do
        printf '{"pattern":"%s","custom_domain":true}\n' "$d"
    done | jq -s .)
    jq --argjson routes "$routes_json" '.routes = $routes' \
        dist/server/wrangler.json > dist/server/wrangler.deploy.json
    mv dist/server/wrangler.deploy.json dist/server/wrangler.json
    npx --yes wrangler@latest deploy -c dist/server/wrangler.json --var DOMAIN:"$primary"

dashboard-tail:
    cd dashboard && npx --yes wrangler@latest tail --format pretty

# Deploy both workers.
deploy-all: deploy-api deploy-frontend

# -------- Vectorize --------

# One-time provisioning for /_cm/search. Creates the `claudemetry-turns`
# index (768-dim, cosine — matches bge-base-en-v1.5). Per-user isolation is
# handled at query time via Vectorize namespaces (namespace=<user_hash>),
# not via a metadata index, so no create-metadata-index step is needed.
# Idempotent: re-runs treat "already exists" as success.
vectorize-create:
    #!/usr/bin/env bash
    set -uo pipefail
    out=$(npx --yes wrangler@latest vectorize create claudemetry-turns \
        --dimensions=768 --metric=cosine 2>&1)
    echo "$out"
    if echo "$out" | grep -qiE "successfully (created|enqueued)|already exists|duplicate_name|duplicate"; then
        echo "→ index ready"
    else
        echo "× index create failed" >&2; exit 1
    fi

vectorize-info:
    npx --yes wrangler@latest vectorize info claudemetry-turns

# -------- burnage (Rust CLI) --------

# Install burnage to ~/.cargo/bin. Bakes https://<primary-domain> in as
# the default proxy URL (primary = first entry in $DOMAINS) so the CLI
# works out of the box without --url.
burnage-install:
    #!/usr/bin/env bash
    set -euo pipefail
    : "${DOMAINS:?set DOMAINS (space-separated list, first = primary) in .env}"
    primary="${DOMAINS%% *}"
    BURNAGE_DEFAULT_URL="https://$primary" cargo install --path burnage --force

# -------- Cloudflare Access --------

# Idempotently provision / repair the Access apps and policies for every
# domain in $DOMAINS. Needs CLOUDFLARE_API_TOKEN (Access: Apps and Policies
# Edit) + CLOUDFLARE_ACCOUNT_ID.
cf-access:
    ./scripts/cf-access.sh
