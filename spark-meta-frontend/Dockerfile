FROM debian:buster-slim

RUN apt update && apt -y install postgresql nodejs npm && npm i npm@latest -g

# COPY make-schema.sh /docker-entrypoint-initdb.d
# COPY schema.sql /mnt
COPY . /app/
WORKDIR /app
RUN npm ci --only=production && npm run build
RUN chmod u+x /app/scripts/*.sh
EXPOSE 8080

CMD ["/app/scripts/run.sh"]
