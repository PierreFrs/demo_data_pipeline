FROM node:20-alpine
WORKDIR /app

# Installe les deps
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev || npm install --omit=dev

# Copie le code et les assets
COPY server.js ./server.js
COPY public ./public

EXPOSE 3000
CMD ["node","server.js"]
