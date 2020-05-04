FROM debian:buster-slim

RUN apt update && apt -y install postgresql nodejs npm graphviz && npm i npm@latest -g

COPY . /app/
WORKDIR /app
# We copy images for the svg rendering
RUN npm ci --only=production && npm run build && cp -a public/images images
RUN chmod u+x /app/scripts/*.sh
EXPOSE 8080

CMD ["/app/scripts/run.sh"]
