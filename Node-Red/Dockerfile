FROM nodered/node-red:4.1.15-22

WORKDIR /data
COPY --chown=node-red:node-red package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund
COPY --chown=node-red:node-red flows.json ./

COPY --chown=node-red:node-red public/ /opt/satellite-radar/public/

WORKDIR /usr/src/node-red
