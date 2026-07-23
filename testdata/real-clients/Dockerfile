FROM node:22.22.0-bookworm-slim@sha256:dd9d21971ec4395903fa6143c2b9267d048ae01ca6d3ea96f16cb30df6187d94

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/human-real-clients
COPY package.json package-lock.json ./
RUN set -eux; \
    npm_arch="$(node -p 'process.arch')"; \
    case "${npm_arch}" in x64|arm64) ;; *) echo "unsupported architecture: ${npm_arch}" >&2; exit 1;; esac; \
    npm config set fetch-retries 5; \
    npm config set fetch-retry-mintimeout 2000; \
    npm config set fetch-retry-maxtimeout 30000; \
    npm ci --omit=dev --omit=optional --ignore-scripts; \
    install_attempt=1; \
    until npm install --no-save --omit=optional --ignore-scripts \
      "@anthropic-ai/claude-code-linux-${npm_arch}@2.1.217" \
      "opencode-linux-${npm_arch}@1.17.18" \
      "@openai/codex-linux-${npm_arch}@npm:@openai/codex@0.145.0-linux-${npm_arch}"; do \
        if [ "${install_attempt}" -ge 3 ]; then exit 1; fi; \
        install_attempt="$((install_attempt + 1))"; \
        sleep 2; \
    done; \
    node node_modules/@anthropic-ai/claude-code/install.cjs; \
    node node_modules/opencode-ai/postinstall.mjs; \
    ./node_modules/.bin/claude --version; \
    ./node_modules/.bin/opencode --version; \
    ./node_modules/.bin/codex --version

ENV PATH="/opt/human-real-clients/node_modules/.bin:${PATH}" \
    DISABLE_AUTOUPDATER=1 \
    CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1 \
    CI=1

CMD ["sleep", "infinity"]
