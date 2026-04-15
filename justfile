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

# -------- cc-proxy (Rust worker) --------

local:
    npx --yes wrangler@latest dev --local --port 8787

local-tee log="proxy.log":
    npx --yes wrangler@latest dev --local --port 8787 2>&1 | tee {{log}}

build:
    worker-build --release

login:
    npx --yes wrangler@latest login

# Substitute __DOMAIN__ into wrangler.toml at deploy time, deploy from
# the generated file, then drop it. Keeps the hostname out of source.
deploy:
    #!/usr/bin/env bash
    set -euo pipefail
    sed "s/__DOMAIN__/$DOMAIN/g" wrangler.toml > wrangler.deploy.toml
    trap 'rm -f wrangler.deploy.toml' EXIT
    npx --yes wrangler@latest deploy -c wrangler.deploy.toml

tail:
    npx --yes wrangler@latest tail --format pretty

clean:
    rm -rf build target .wrangler

# -------- claudemetry (Astro dashboard) --------

dashboard-dev:
    cd dashboard && pnpm dev

# Astro's CF adapter produces dist/server/wrangler.json at build time
# with main/assets/bindings auto-filled. We sed __DOMAIN__ into that
# generated file and deploy from it. Source wrangler.jsonc stays lean
# so the vite-plugin doesn't try to resolve a nonexistent `main` during
# the build phase.
dashboard-deploy:
    #!/usr/bin/env bash
    set -euo pipefail
    cd dashboard
    pnpm build
    sed -i "s/__DOMAIN__/$DOMAIN/g" dist/server/wrangler.json
    npx --yes wrangler@latest deploy -c dist/server/wrangler.json --var DOMAIN:"$DOMAIN"

dashboard-tail:
    cd dashboard && npx --yes wrangler@latest tail --format pretty

# Deploy both workers.
deploy-all: deploy dashboard-deploy

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

# Install burnage to ~/.cargo/bin. Bakes https://$DOMAIN in as the default
# proxy URL so the CLI works out of the box without --url.
burnage-install:
    BURNAGE_DEFAULT_URL="https://$DOMAIN" cargo install --path burnage --force

# -------- Cloudflare Access --------

# Idempotently provision / repair the Access apps and policies for $DOMAIN.
# Needs CLOUDFLARE_API_TOKEN (Access: Apps and Policies Edit) + CLOUDFLARE_ACCOUNT_ID.
cf-access:
    ./scripts/cf-access.sh
