FROM node:20-alpine AS builder

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev

COPY . .

FROM node:20-alpine

RUN apk add --no-cache tini

WORKDIR /app
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package*.json ./
COPY --from=builder /app/bot.js ./
COPY --from=builder /app/processor.js ./
COPY --from=builder /app/stations.json ./
COPY --from=builder /app/communities.geojson ./
COPY --from=builder /app/provinces.geojson ./

RUN addgroup -g 1001 nodejs && \
    adduser -S botuser -u 1001 -G nodejs

RUN mkdir -p /app/pdfs && \
    mkdir -p /app/output && \
    mkdir -p /app/temp && \
    mkdir -p /app/wfs
RUN chown -R botuser:nodejs /app/pdfs && \
    chown -R botuser:nodejs /app/output && \
    chown -R botuser:nodejs /app/temp && \
    chown -R botuser:nodejs /app/wfs
VOLUME ["/app/pdfs", "/app/output", "/app/temp", "/app/wfs"]

USER botuser

ENTRYPOINT ["/sbin/tini", "--"]

ENV NODE_ENV=production

CMD ["node", "bot.js"]