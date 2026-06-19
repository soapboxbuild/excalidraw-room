import debug from "debug";
import express from "express";
import http from "http";
import { Server as SocketIO } from "socket.io";
import { createAdapter } from "@socket.io/redis-adapter";
import { createClient } from "redis";

type UserToFollow = {
  socketId: string;
  username: string;
};
type OnUserFollowedPayload = {
  userToFollow: UserToFollow;
  action: "FOLLOW" | "UNFOLLOW";
};

const serverDebug = debug("server");
const ioDebug = debug("io");
const socketDebug = debug("socket");


const app = express();
const port =
  process.env.PORT || (process.env.NODE_ENV !== "development" ? 80 : 3002);

app.use(express.static("public"));

app.get("/", (req, res) => {
  res.send("Excalidraw collaboration server is up :)");
});

// Seed endpoint for pre-loading a room's scene from external sources
app.post("/api/seed/:roomID", express.json({ limit: "10mb" }), async (req, res) => {
  const secret = process.env.SEED_SECRET;
  if (secret && req.headers["x-seed-secret"] !== secret) {
    return res.status(401).json({ error: "unauthorized" });
  }
  const { encryptedData, iv } = req.body; // both base64 strings
  if (!encryptedData || !iv) {
    return res.status(400).json({ error: "encryptedData and iv are required" });
  }
  if (persistClient) {
    await persistClient.set(
      `excalidraw:scene:${req.params.roomID}`,
      JSON.stringify({ encryptedData, iv }),
      { EX: 30 * 24 * 60 * 60 },
    );
    res.json({ ok: true });
  } else {
    res.status(503).json({ error: "persistence not available (no REDIS_URL)" });
  }
});

const server = http.createServer(app);

server.listen(port, () => {
  serverDebug(`listening on port: ${port}`);
});

// Separate Redis client for scene persistence (not pub/sub)
let persistClient: ReturnType<typeof createClient> | null = null;

try {
  const io = new SocketIO(server, {
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
    const pubClient = createClient({ url: redisUrl });
    const subClient = pubClient.duplicate();
    Promise.all([pubClient.connect(), subClient.connect()])
      .then(() => {
        io.adapter(createAdapter(pubClient, subClient));
        const safeUrl = redisUrl.replace(/:\/\/[^@]+@/, "://<redacted>@");
        console.log("[excalidraw-room] Redis adapter active on %s", safeUrl);
      })
      .catch((err: Error) => {
        console.error("[excalidraw-room] Redis adapter connection failed, running without pub/sub:", err);
      });

    // Persistence client (separate connection)
    persistClient = createClient({ url: redisUrl });
    persistClient.connect()
      .then(() => {
        console.log("[excalidraw-room] Redis persist client connected");
      })
      .catch((err: Error) => {
        console.error("[excalidraw-room] Redis persist client connection failed:", err);
        persistClient = null;
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
        // First user in room — try to load persisted scene
        let loaded = false;
        if (persistClient) {
          try {
            const stored = await persistClient.get(`excalidraw:scene:${roomID}`);
            if (stored) {
              const { encryptedData, iv } = JSON.parse(stored);
              socketDebug(`${socket.id} loading persisted scene for ${roomID}`);
              io.to(`${socket.id}`).emit(
                "client-broadcast",
                Buffer.from(encryptedData, "base64"),
                Buffer.from(iv, "base64"),
              );
              loaded = true;
            }
          } catch (err) {
            console.error("[excalidraw-room] Failed to load persisted scene:", err);
          }
        }
        if (!loaded) {
          io.to(`${socket.id}`).emit("first-in-room");
        }
      } else {
        socketDebug(`${socket.id} new-user emitted to room ${roomID}`);
        socket.broadcast.to(roomID).emit("new-user", socket.id);
      }

      io.in(roomID).emit(
        "room-user-change",
        sockets.map((socket) => socket.id),
      );
    });

    socket.on(
      "server-broadcast",
      (roomID: string, encryptedData: ArrayBuffer, iv: Uint8Array) => {
        socketDebug(`${socket.id} sends update to ${roomID}`);
        socket.broadcast.to(roomID).emit("client-broadcast", encryptedData, iv);

        // Persist the latest scene state
        if (persistClient) {
          persistClient
            .set(
              `excalidraw:scene:${roomID}`,
              JSON.stringify({
                encryptedData: Buffer.from(encryptedData).toString("base64"),
                iv: Buffer.from(iv).toString("base64"),
              }),
              { EX: 30 * 24 * 60 * 60 },
            )
            .catch((err: Error) => {
              console.error("[excalidraw-room] Failed to persist scene:", err);
            });
        }
      },
    );

    socket.on(
      "server-volatile-broadcast",
      (roomID: string, encryptedData: ArrayBuffer, iv: Uint8Array) => {
        socketDebug(`${socket.id} sends volatile update to ${roomID}`);
        socket.volatile.broadcast
          .to(roomID)
          .emit("client-broadcast", encryptedData, iv);
      },
    );

    socket.on("user-follow", async (payload: OnUserFollowedPayload) => {
      const roomID = `follow@${payload.userToFollow.socketId}`;

      switch (payload.action) {
        case "FOLLOW": {
          await socket.join(roomID);

          const sockets = await io.in(roomID).fetchSockets();
          const followedBy = sockets.map((socket) => socket.id);

          io.to(payload.userToFollow.socketId).emit(
            "user-follow-room-change",
            followedBy,
          );

          break;
        }
        case "UNFOLLOW": {
          await socket.leave(roomID);

          const sockets = await io.in(roomID).fetchSockets();
          const followedBy = sockets.map((socket) => socket.id);

          io.to(payload.userToFollow.socketId).emit(
            "user-follow-room-change",
            followedBy,
          );

          break;
        }
      }
    });

    socket.on("disconnecting", async () => {
      socketDebug(`${socket.id} has disconnected`);
      for (const roomID of Array.from(socket.rooms)) {
        const otherClients = (await io.in(roomID).fetchSockets()).filter(
          (_socket) => _socket.id !== socket.id,
        );

        const isFollowRoom = roomID.startsWith("follow@");

        if (!isFollowRoom && otherClients.length > 0) {
          socket.broadcast.to(roomID).emit(
            "room-user-change",
            otherClients.map((socket) => socket.id),
          );
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
} catch (error) {
  console.error(error);
}
