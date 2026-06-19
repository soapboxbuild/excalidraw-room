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
app.use(express_1.default.json({ limit: "10mb" }));
app.get("/", (req, res) => {
    res.send("Excalidraw collaboration server is up :)");
});
const server = http_1.default.createServer(app);
server.listen(port, () => {
    serverDebug(`listening on port: ${port}`);
});
// Separate persist client for scene storage (not pub/sub)
let persistClient = null;
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
        // Pub/sub adapter for horizontal scaling
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
        // Separate client for scene persistence
        persistClient = (0, redis_1.createClient)({ url: redisUrl });
        persistClient.connect()
            .then(() => console.log("[excalidraw-room] Persist client connected"))
            .catch((err) => {
            console.error("[excalidraw-room] Persist client failed:", err);
            persistClient = null;
        });
    }
    // Seed endpoint — accepts pre-encrypted scene data for a room
    app.post("/api/seed/:roomID", async (req, res) => {
        const secret = process.env.SEED_SECRET;
        if (secret && req.headers["x-seed-secret"] !== secret) {
            return res.status(401).json({ error: "unauthorized" });
        }
        if (!persistClient) {
            return res.status(503).json({ error: "persistence not configured" });
        }
        const { encryptedData, iv } = req.body;
        if (!encryptedData || !iv) {
            return res.status(400).json({ error: "encryptedData and iv required" });
        }
        const key = `excalidraw:scene:${req.params.roomID}`;
        await persistClient.set(key, JSON.stringify({ encryptedData, iv }), { EX: 30 * 24 * 60 * 60 });
        console.log(`[excalidraw-room] Seeded room: ${req.params.roomID}`);
        res.json({ ok: true });
    });
    io.on("connection", (socket) => {
        ioDebug("connection established!");
        io.to(`${socket.id}`).emit("init-room");
        socket.on("join-room", async (roomID) => {
            socketDebug(`${socket.id} has joined ${roomID}`);
            await socket.join(roomID);
            const sockets = await io.in(roomID).fetchSockets();
            if (sockets.length <= 1) {
                // First in room — try to load persisted scene
                if (persistClient) {
                    try {
                        const saved = await persistClient.get(`excalidraw:scene:${roomID}`);
                        if (saved) {
                            const { encryptedData, iv } = JSON.parse(saved);
                            socket.emit("client-broadcast",
                                Buffer.from(encryptedData, "base64"),
                                Buffer.from(iv, "base64")
                            );
                            console.log(`[excalidraw-room] Restored scene for room: ${roomID}`);
                            io.in(roomID).emit("room-user-change", sockets.map((s) => s.id));
                            return;
                        }
                    }
                    catch (err) {
                        console.error("[excalidraw-room] Failed to load persisted scene:", err);
                    }
                }
                io.to(`${socket.id}`).emit("first-in-room");
            }
            else {
                socketDebug(`${socket.id} new-user emitted to room ${roomID}`);
                socket.broadcast.to(roomID).emit("new-user", socket.id);
            }
            io.in(roomID).emit("room-user-change", sockets.map((socket) => socket.id));
        });
        socket.on("server-broadcast", async (roomID, encryptedData, iv) => {
            socketDebug(`${socket.id} sends update to ${roomID}`);
            socket.broadcast.to(roomID).emit("client-broadcast", encryptedData, iv);
            // Persist scene to Redis
            if (persistClient) {
                try {
                    await persistClient.set(
                        `excalidraw:scene:${roomID}`,
                        JSON.stringify({
                            encryptedData: Buffer.from(encryptedData).toString("base64"),
                            iv: Buffer.from(iv).toString("base64"),
                        }),
                        { EX: 30 * 24 * 60 * 60 }
                    );
                }
                catch (err) {
                    console.error("[excalidraw-room] Failed to persist scene:", err);
                }
            }
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
