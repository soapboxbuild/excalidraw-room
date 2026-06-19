"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const debug_1 = __importDefault(require("debug"));
const express_1 = __importDefault(require("express"));
const http_1 = __importDefault(require("http"));
const socket_io_1 = require("socket.io");
const redis_adapter_1 = require("@socket.io/redis-adapter");
const redis_1 = require("redis");
const serverDebug = (0, debug_1.default)("server");
const ioDebug = (0, debug_1.default)("io");
const socketDebug = (0, debug_1.default)("socket");
const app = (0, express_1.default)();
const port = process.env.PORT || (process.env.NODE_ENV !== "development" ? 80 : 3002);
app.use(express_1.default.static("public"));
app.get("/", (req, res) => {
    res.send("Excalidraw collaboration server is up :)");
});
const server = http_1.default.createServer(app);
server.listen(port, () => {
    serverDebug(`listening on port: ${port}`);
});
try {
    const io = new socket_io_1.Server(server, {
        transports: ["websocket", "polling"],
        cors: {
            allowedHeaders: ["Content-Type", "Authorization"],
            origin: process.env.CORS_ORIGIN || "*",
            credentials: true,
        },
        allowEIO3: true,
    });
    const redisUrl = process.env.REDIS_URL;
    if (redisUrl) {
        const pubClient = (0, redis_1.createClient)({ url: redisUrl });
        const subClient = pubClient.duplicate();
        Promise.all([pubClient.connect(), subClient.connect()])
            .then(() => {
            io.adapter((0, redis_adapter_1.createAdapter)(pubClient, subClient));
            const safeUrl = redisUrl.replace(/:\/\/[^@]+@/, '://<redacted>@');
            console.log("[excalidraw-room] Redis adapter active on %s", safeUrl);
        })
            .catch((err) => {
            console.error("[excalidraw-room] Redis adapter connection failed, running without pub/sub:", err);
        });
    }
    io.on("connection", (socket) => {
        ioDebug("connection established!");
        io.to(`${socket.id}`).emit("init-room");
        socket.on("join-room", async (roomID) => {
            socketDebug(`${socket.id} has joined ${roomID}`);
            await socket.join(roomID);
            const sockets = await io.in(roomID).fetchSockets();
            if (sockets.length <= 1) {
                io.to(`${socket.id}`).emit("first-in-room");
            }
            else {
                socketDebug(`${socket.id} new-user emitted to room ${roomID}`);
                socket.broadcast.to(roomID).emit("new-user", socket.id);
            }
            io.in(roomID).emit("room-user-change", sockets.map((socket) => socket.id));
        });
        socket.on("server-broadcast", (roomID, encryptedData, iv) => {
            socketDebug(`${socket.id} sends update to ${roomID}`);
            socket.broadcast.to(roomID).emit("client-broadcast", encryptedData, iv);
        });
        socket.on("server-volatile-broadcast", (roomID, encryptedData, iv) => {
            socketDebug(`${socket.id} sends volatile update to ${roomID}`);
            socket.volatile.broadcast
                .to(roomID)
                .emit("client-broadcast", encryptedData, iv);
        });
        socket.on("user-follow", async (payload) => {
            const roomID = `follow@${payload.userToFollow.socketId}`;
            switch (payload.action) {
                case "FOLLOW": {
                    await socket.join(roomID);
                    const sockets = await io.in(roomID).fetchSockets();
                    const followedBy = sockets.map((socket) => socket.id);
                    io.to(payload.userToFollow.socketId).emit("user-follow-room-change", followedBy);
                    break;
                }
                case "UNFOLLOW": {
                    await socket.leave(roomID);
                    const sockets = await io.in(roomID).fetchSockets();
                    const followedBy = sockets.map((socket) => socket.id);
                    io.to(payload.userToFollow.socketId).emit("user-follow-room-change", followedBy);
                    break;
                }
            }
        });
        socket.on("disconnecting", async () => {
            socketDebug(`${socket.id} has disconnected`);
            for (const roomID of Array.from(socket.rooms)) {
                const otherClients = (await io.in(roomID).fetchSockets()).filter((_socket) => _socket.id !== socket.id);
                const isFollowRoom = roomID.startsWith("follow@");
                if (!isFollowRoom && otherClients.length > 0) {
                    socket.broadcast.to(roomID).emit("room-user-change", otherClients.map((socket) => socket.id));
                }
                if (isFollowRoom && otherClients.length === 0) {
                    const socketId = roomID.replace("follow@", "");
                    io.to(socketId).emit("broadcast-unfollow");
                }
            }
        });
        socket.on("disconnect", () => {
            socket.removeAllListeners();
            socket.disconnect();
        });
    });
}
catch (error) {
    console.error(error);
}
