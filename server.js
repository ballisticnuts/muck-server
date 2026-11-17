// Tiny matchmaking server for browser multiplayer. It only passes connection messages between players;
// the game itself runs directly between the browsers.
const http = require("http");
const { WebSocketServer } = require("ws");

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("ok");
});
const wss = new WebSocketServer({ server });

const lobbies = new Map(); // code -> { host: ws, players: Map(id -> ws), next: number }
const CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

function makeCode() {
  let code;
  do {
    code = Array.from({ length: 5 }, () => CHARS[Math.floor(Math.random() * CHARS.length)]).join("");
  } while (lobbies.has(code));
  return code;
}
const send = (ws, msg) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg)); };

wss.on("connection", (ws) => {
  ws.alive = true;
  ws.on("pong", () => { ws.alive = true; });

  ws.on("message", (data) => {
    let m;
    try { m = JSON.parse(data.toString()); } catch { return; }

    if (m.type === "host") {
      const code = makeCode();
      lobbies.set(code, { players: new Map([[1, ws]]), next: 2 });
      ws.lobby = code; ws.pid = 1;
      send(ws, { type: "hosted", code, id: 1 });
      return;
    }
    if (m.type === "join") {
      const code = String(m.code || "").toUpperCase().trim();
      const lobby = lobbies.get(code);
      if (!lobby) return send(ws, { type: "error", message: "No game with that code. Check the code and try again" });
      if (lobby.players.size >= 8) return send(ws, { type: "error", message: "That game is full" });
      const id = lobby.next++;
      const peers = [...lobby.players.keys()];
      lobby.players.set(id, ws);
      ws.lobby = code; ws.pid = id;
      send(ws, { type: "joined", id, peers });
      for (const [pid, p] of lobby.players) if (pid !== id) send(p, { type: "peer_joined", id });
      return;
    }
    if (["offer", "answer", "candidate"].includes(m.type)) {
      const lobby = lobbies.get(ws.lobby);
      if (!lobby) return;
      const target = lobby.players.get(Number(m.to));
      m.from = ws.pid;
      send(target, m);
    }
  });

  ws.on("close", () => {
    const lobby = lobbies.get(ws.lobby);
    if (!lobby) return;
    lobby.players.delete(ws.pid);
    if (ws.pid === 1) {
      for (const p of lobby.players.values()) send(p, { type: "lobby_closed" });
      lobbies.delete(ws.lobby);
    } else {
      for (const p of lobby.players.values()) send(p, { type: "peer_left", id: ws.pid });
    }
  });
});

// drop dead connections
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false; ws.ping();
  }
}, 30000);

server.listen(process.env.PORT || 8080, () => console.log("signaling server up"));
