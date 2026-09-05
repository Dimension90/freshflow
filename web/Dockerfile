FROM nginx:1.27.2-alpine

COPY web/nginx.conf /etc/nginx/conf.d/default.conf
COPY web/index.html web/app.js web/api.js web/format.js web/styles.css /usr/share/nginx/html/
COPY web/assets /usr/share/nginx/html/assets

EXPOSE 8080
