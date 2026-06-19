FROM node:20-alpine

WORKDIR /excalidraw-room

COPY package.json yarn.lock ./
RUN yarn --frozen-lockfile

COPY tsconfig.json ./
COPY src ./src
RUN yarn build
USER node

EXPOSE 80
CMD ["yarn", "start"]
