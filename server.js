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
// Relay (TURN) servers from Metered. Set METERED_APP (the part before .metered.live) and METERED_KEY (API key) on Render.
let iceCache = null, iceTime = 0;
async function getIce() {
  // Simple way: fixed relay login. TURN_USER + TURN_PASS (+ optional TURN_HOST, default global.relay.metered.ca)
  if (process.env.TURN_USER && process.env.TURN_PASS) {
    const h = process.env.TURN_HOST || "global.relay.metered.ca";
    const u = process.env.TURN_USER, c = process.env.TURN_PASS;
    return [
      { urls: [`turn:${h}:80`, `turn:${h}:80?transport=tcp`, `turn:${h}:443`, `turns:${h}:443?transport=tcp`], username: u, credential: c },
    ];
  }
  const app = process.env.METERED_APP, key = process.env.METERED_KEY;
  if (!app || !key) return [];
  if (iceCache && Date.now() - iceTime < 30 * 60 * 1000) return iceCache;
  try {
    const r = await fetch(`https://${app}.metered.live/api/v1/turn/credentials?apiKey=${key}`);
    const list = await r.json();
    if (Array.isArray(list)) { iceCache = list; iceTime = Date.now(); return list; }
  } catch (e) { console.log("could not get relay servers", e.message); }
  return iceCache || [];
}
const send = (ws, msg) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg)); };

wss.on("connection", (ws) => {
  ws.alive = true;
  ws.on("pong", () => { ws.alive = true; });

  ws.on("message", async (data) => {
    let m;
    try { m = JSON.parse(data.toString()); } catch { return; }

    if (m.type === "host") {
      const code = makeCode();
      lobbies.set(code, { players: new Map([[1, ws]]), next: 2 });
      ws.lobby = code; ws.pid = 1;
      send(ws, { type: "hosted", code, id: 1, ice: await getIce() });
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
      send(ws, { type: "joined", id, peers, ice: await getIce() });
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
