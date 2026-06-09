# syntax=docker/dockerfile:1

# ---- Build stage: install all deps and compile TypeScript to dist/ ----
FROM node:20-alpine AS build
WORKDIR /app
COPY package.json ./
RUN npm install
COPY tsconfig.json ./
COPY src ./src
RUN npm run build

# ---- Runtime stage: production deps only + compiled output + Lua asset ----
FROM node:20-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production
COPY package.json ./
RUN npm install --omit=dev
COPY --from=build /app/dist ./dist
# The Lua script is a runtime asset loaded by the rate limiter.
COPY src/lua ./dist/lua
CMD ["node", "dist/main.js"]
