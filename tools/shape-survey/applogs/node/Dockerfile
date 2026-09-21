FROM node:22-slim

# Pinned, and installed without a package.json on purpose: two direct dependencies, both exact.
RUN npm install --no-save --no-fund --no-audit --prefix /app pino@10.3.1 pino-http@11.0.0

WORKDIR /app
COPY app.js /app/
