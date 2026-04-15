default:
    @just --list

local:
    npx --yes wrangler@latest dev --local --port 8787

local-tee log="proxy.log":
    npx --yes wrangler@latest dev --local --port 8787 2>&1 | tee {{log}}

build:
    worker-build --release

login:
    npx --yes wrangler@latest login

deploy:
    npx --yes wrangler@latest deploy

tail:
    npx --yes wrangler@latest tail --format pretty

clean:
    rm -rf build target .wrangler
