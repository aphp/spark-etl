FROM node:12

WORKDIR /app
COPY . ./

RUN npm ci --only=production && npm run build
EXPOSE 8080

CMD ["node", "server.js", "/mnt/tables.csv", "/mnt/attributes.csv"]
