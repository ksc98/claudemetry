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
# per domain in $DOMAINS (space-separated; first = primary). The heavy
# lifting lives in scripts/deploy-api.sh so Cloudflare Workers Builds
# (CI) can reuse the exact same flow.
deploy-api:
    ./scripts/deploy-api.sh

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

# One-time provisioning for /_cm/search. Creates the `burnage`
# index (1024-dim, cosine — matches qwen3-embedding-0.6b). Per-user isolation is
# handled at query time via Vectorize namespaces (namespace=<user_hash>),
# not via a metadata index, so no create-metadata-index step is needed.
# Idempotent: re-runs treat "already exists" as success.
vectorize-create:
    #!/usr/bin/env bash
    set -uo pipefail
    out=$(npx --yes wrangler@latest vectorize create burnage \
        --dimensions=1024 --metric=cosine 2>&1)
    echo "$out"
    if echo "$out" | grep -qiE "successfully (created|enqueued)|already exists|duplicate_name|duplicate"; then
        echo "→ index ready"
    else
        echo "× index create failed" >&2; exit 1
    fi

vectorize-info:
    npx --yes wrangler@latest vectorize info burnage

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

# Build every release binary the host can produce. macOS arm64 is built
# natively via rustup when the host is Darwin, and skipped on Linux
# (cross-compile to macOS isn't worth the ceremony — just release from a
# Mac when one's available). Linux x86_64 always builds via Docker for
# glibc-portable binaries. Every arch gets --remap-path-prefix so the
# shipped binary contains no $HOME / $CARGO_HOME / build-dir paths.
#
# Homebrew builds DO NOT bake BURNAGE_DEFAULT_URL — the CLI falls back
# to $ANTHROPIC_BASE_URL at runtime, which brew users already set to
# route Claude Code through their proxy.
burnage-build-all:
    #!/usr/bin/env bash
    set -euo pipefail

    HOST="$(uname -s)"
    MAC_RUSTFLAGS="--remap-path-prefix=$HOME= --remap-path-prefix=$PWD=."
    DOCKER_RUSTFLAGS="--remap-path-prefix=/usr/local/cargo= --remap-path-prefix=/src=."

    if [[ "$HOST" == "Darwin" ]]; then
      echo "==> building macOS arm64 (native rustup)"
      rustup target add aarch64-apple-darwin >/dev/null
      RUSTFLAGS="$MAC_RUSTFLAGS" cargo build --release --package burnage --target aarch64-apple-darwin
    else
      echo "==> skipping macOS arm64 (not on Darwin; run from your Mac to include mac binary)"
    fi

    echo "==> building Linux x86_64 (docker)"
    docker run --rm --platform linux/amd64 \
      -v "$PWD":/src -w /src \
      -e CARGO_TARGET_DIR=/src/target/linux-x86_64 \
      -e RUSTFLAGS="$DOCKER_RUSTFLAGS" \
      rust:bookworm cargo build --release --locked --package burnage

    echo "==> packaging"
    mkdir -p dist
    rm -f dist/burnage-*.tar.gz
    if [[ "$HOST" == "Darwin" ]]; then
      tar -C target/aarch64-apple-darwin/release -czf dist/burnage-macos-arm64.tar.gz burnage
    fi
    tar -C target/linux-x86_64/release -czf dist/burnage-linux-x86_64.tar.gz burnage

    echo "==> sha256 digests"
    cd dist && (shasum -a 256 burnage-*.tar.gz 2>/dev/null || sha256sum burnage-*.tar.gz)

# Cut a new CLI release. Bumps burnage/Cargo.toml (NOT the workspace-root
# Cargo.toml, which belongs to burnage-api), tags burnage-vX.Y.Z, builds
# whatever binaries the host can natively produce (macOS arm64 via
# rustup on Darwin; Linux x86_64 via Docker always), uploads them to the
# ksc98/homebrew-tap release, and rewrites the brew formula on Darwin.
# On Linux the brew formula is left alone (mac brew users keep the last
# Darwin-host release).
#
# Re-entrant: if an earlier run bumped/tagged but failed later, re-running
# the same version skips the already-done steps and resumes from where
# it stopped.
#
# Usage: just burnage-release 0.2.0
burnage-release VERSION:
    #!/usr/bin/env bash
    set -euo pipefail

    if ! git diff-index --quiet HEAD --; then
      echo "working tree dirty — commit or stash first" >&2
      exit 1
    fi
    if [[ "$(git rev-parse --abbrev-ref HEAD)" != "main" ]]; then
      echo "must be on main branch" >&2
      exit 1
    fi

    HOST="$(uname -s)"
    TAP="$HOME/dev/homebrew-tap"

    if [[ "$HOST" == "Darwin" ]]; then
      # Only Darwin releases rewrite the brew formula, so only Darwin needs
      # the tap clone. Auto-clone if missing.
      if [[ ! -d "$TAP" ]]; then
        echo "==> cloning homebrew-tap to $TAP"
        git clone https://github.com/ksc98/homebrew-tap.git "$TAP"
      fi
    fi

    if ! docker info >/dev/null 2>&1; then
      echo "Docker is not running — start Docker and retry (needed for Linux builds)" >&2
      exit 1
    fi

    echo "==> bumping burnage/Cargo.toml to {{VERSION}}"
    sed 's/^version = ".*"$/version = "{{VERSION}}"/' burnage/Cargo.toml > burnage/Cargo.toml.tmp
    mv burnage/Cargo.toml.tmp burnage/Cargo.toml
    cargo check --release --quiet --package burnage

    if ! git diff --quiet -- burnage/Cargo.toml Cargo.lock; then
      echo "==> commit + push"
      git add burnage/Cargo.toml Cargo.lock
      git commit -m "Release burnage v{{VERSION}}"
      git push
    else
      echo "==> burnage/Cargo.toml already at {{VERSION}}, skipping commit"
    fi

    if git rev-parse burnage-v{{VERSION}} >/dev/null 2>&1; then
      echo "==> tag burnage-v{{VERSION}} already exists, skipping"
    else
      echo "==> tag burnage-v{{VERSION}}"
      git tag burnage-v{{VERSION}}
      git push origin burnage-v{{VERSION}}
    fi

    just burnage-build-all

    # Which arches did we actually build?
    archs=(linux-x86_64)
    if [[ "$HOST" == "Darwin" ]]; then
      archs=(macos-arm64 "${archs[@]}")
    fi

    declare -A SHA
    for arch in "${archs[@]}"; do
      SHA[$arch]=$(sha256sum "dist/burnage-${arch}.tar.gz" 2>/dev/null || shasum -a 256 "dist/burnage-${arch}.tar.gz")
      SHA[$arch]=$(awk '{print $1}' <<<"${SHA[$arch]}")
      echo "==> $arch: ${SHA[$arch]}"
    done

    echo "==> upload binaries to tap release burnage-v{{VERSION}}"
    # Binaries live on the tap (public) so the source repo can stay public
    # without churn per release; same pattern as nba-tv.
    gh release create burnage-v{{VERSION}} \
      --repo ksc98/homebrew-tap \
      --title "burnage v{{VERSION}}" \
      --notes "Release burnage CLI v{{VERSION}}." 2>/dev/null || true
    for arch in "${archs[@]}"; do
      gh release upload burnage-v{{VERSION}} \
        --repo ksc98/homebrew-tap \
        "dist/burnage-${arch}.tar.gz" --clobber
    done

    if [[ "$HOST" == "Darwin" ]]; then
      just _burnage-write-formula {{VERSION}} "${SHA[macos-arm64]}" "${SHA[linux-x86_64]}"
    else
      echo "==> skipping brew formula rewrite (no macOS binary on this host)"
      echo "==> mac brew users will stay on the previous formula version"
      echo "==> cut the next release from a Mac to refresh mac binaries"
    fi

    echo ""
    echo "✓ Released burnage v{{VERSION}}"
    echo "  brew update && brew upgrade ksc98/tap/burnage"

# Internal helper — rewrites Formula/burnage.rb in the tap clone, commits,
# and pushes. Only called by `burnage-release` on Darwin hosts; broken
# out so the heredoc isn't nested inside a conditional (bash won't match
# an indented EOF terminator).
_burnage-write-formula VERSION MAC_SHA LINUX_SHA:
    #!/usr/bin/env bash
    set -euo pipefail

    TAP="$HOME/dev/homebrew-tap"
    cd "$TAP"
    git pull --rebase --quiet

    cat > Formula/burnage.rb <<EOF
    class Burnage < Formula
      desc "CLI for your burnage Claude Code usage proxy"
      homepage "https://github.com/ksc98/burnage"
      version "{{VERSION}}"
      license "MIT"

      on_macos do
        on_arm do
          url "https://github.com/ksc98/homebrew-tap/releases/download/burnage-v{{VERSION}}/burnage-macos-arm64.tar.gz"
          sha256 "{{MAC_SHA}}"
        end
      end

      on_linux do
        on_intel do
          url "https://github.com/ksc98/homebrew-tap/releases/download/burnage-v{{VERSION}}/burnage-linux-x86_64.tar.gz"
          sha256 "{{LINUX_SHA}}"
        end
      end

      def install
        bin.install "burnage"
      end

      test do
        assert_match "burnage", shell_output("#{bin}/burnage --help")
      end
    end
    EOF

    git add Formula/burnage.rb
    if git diff --cached --quiet; then
      echo "==> formula already at v{{VERSION}}, skipping commit"
    else
      git commit -m "Bump burnage to v{{VERSION}}"
      git push
    fi

# -------- Cloudflare Access --------

# Idempotently provision / repair the Access apps and policies for every
# domain in $DOMAINS. Needs CLOUDFLARE_API_TOKEN (Access: Apps and Policies
# Edit) + CLOUDFLARE_ACCOUNT_ID.
cf-access:
    ./scripts/cf-access.sh
