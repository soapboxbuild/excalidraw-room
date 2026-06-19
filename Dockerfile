FROM node:20-alpine

WORKDIR /excalidraw-room

COPY package.json yarn.lock ./
RUN yarn --frozen-lockfile --production

COPY dist ./dist
USER node

EXPOSE 80
CMD ["yarn", "start"]
