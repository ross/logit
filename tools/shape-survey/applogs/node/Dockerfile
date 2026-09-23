FROM node:22-slim

# Two exact-pinned direct dependencies, so no package.json.
RUN npm install --no-save --no-fund --no-audit --prefix /app pino@10.3.1 pino-http@11.0.0

WORKDIR /app
COPY app.js /app/
