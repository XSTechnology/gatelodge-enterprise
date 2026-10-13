# datagatelodge.com — static site, Coolify/Docker deployable
#
# Build context = this directory. Coolify: deploy as a Dockerfile application,
# expose port 80, set the FQDN (datagatelodge.com / www) on the app.
FROM nginx:alpine

COPY . /usr/share/nginx/html

EXPOSE 80
