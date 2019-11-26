#### Stage 1: Build the Angular application
FROM node:alpine as build

WORKDIR /app

COPY . .

RUN npm install && npm run build

#### Stage 2: Serve the Angular application from Nginx 
FROM nginx:alpine
# Copy our custom nginx config
COPY nginx.conf /etc/nginx/
# Copy the react build from Stage 1
COPY --from=build /app/dist/client /usr/share/nginx/html
# Expose port 80 to the Docker host, so we can access it 
# from the outside.
EXPOSE 80

ENTRYPOINT ["nginx","-g","daemon off;"]