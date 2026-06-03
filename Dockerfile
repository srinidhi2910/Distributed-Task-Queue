# Use Node 20 on Alpine Linux
FROM node:20-alpine

WORKDIR /app

COPY package*.json ./

RUN npm install --omit=dev

COPY src/ ./src/

RUN mkdir -p logs

EXPOSE 3000

CMD ["node", "src/index.js"]