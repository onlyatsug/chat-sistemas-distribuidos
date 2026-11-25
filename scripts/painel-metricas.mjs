// Pessoa 04: Servidor do Painel de Métricas Ao Vivo + Orquestrador Interativo de Carga
// Node.js 22+, zero dependências externas (usa http, fs, WebSocket e fetch nativos).

import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PANEL_PORT || 4040);
const BIND = process.env.PANEL_BIND || "0.0.0.0";
const TARGET_URL = (process.env.TARGET_URL || "http://localhost:4000").replace(/\/+$/, "");
const HTML_PATH = path.join(__dirname, "painel-metricas.html");

const wsEndpoint = new URL("/socket/websocket?vsn=2.0.0", TARGET_URL);
wsEndpoint.protocol = wsEndpoint.protocol === "https:" ? "wss:" : "ws:";

const HISTORY_LIMIT = 120; // 60s com intervalo de 500ms
const DEDUP_WINDOW = 1500;
const LATENCY_WINDOW = 4000;
const RESERVOIR_LIMIT = 20000;

const sseClients = new Set();
const history = [];
let prevNodeStats = new Map();
let lastBeamMetrics = null;
let lastLbMetrics = null;
let lastPollError = null;

// Estado do motor de carga interativo
const loadState = {
  active: false,
  starting: false,
  preset: "inativo",
  config: { clients: 0, rooms: 1, intervalMs: 500, durationSec: 60 },
  startedAt: null,
  endsAt: null,
  rooms: [],
  bots: [],
  stopTimer: null,
  heartbeatTimer: null,
  runId: 0,
  counters: freshCounters(),
  windowSent: 0,
  windowAcked: 0,
  windowDelivered: 0,
  recentLatencies: [],
  recentAckLatencies: [],
  reservoirLatencies: [],
  reservoirSeen: 0,
  peakMsgRate: 0,
  peakDeliveryRate: 0
};

function freshCounters() {
  return {
    enviadas: 0,
    confirmadas: 0,
    entregas: 0,
    duplicadas: 0,
    errosEnvio: 0,
    reconexoes: 0
  };
}

function recordLatency(ms) {
  if (!Number.isFinite(ms) || ms < 0) return;
  loadState.recentLatencies.push(ms);
  if (loadState.recentLatencies.length > LATENCY_WINDOW) {
    loadState.recentLatencies.shift();
  }
  loadState.reservoirSeen += 1;
  if (loadState.reservoirLatencies.length < RESERVOIR_LIMIT) {
    loadState.reservoirLatencies.push(ms);
  } else {
    const idx = Math.floor(Math.random() * loadState.reservoirSeen);
    if (idx < RESERVOIR_LIMIT) loadState.reservoirLatencies[idx] = ms;
  }
}

function recordAckLatency(ms) {
  if (!Number.isFinite(ms) || ms < 0) return;
  loadState.recentAckLatencies.push(ms);
  if (loadState.recentAckLatencies.length > 2000) {
    loadState.recentAckLatencies.shift();
  }
}

function computePercentiles(samples) {
  if (!samples.length) {
    return { media: 0, p50: 0, p95: 0, p99: 0, max: 0, amostras: 0 };
  }
  const sorted = [...samples].sort((a, b) => a - b);
  const n = sorted.length;
  const sum = sorted.reduce((acc, v) => acc + v, 0);
  const pick = (p) => sorted[Math.min(n - 1, Math.max(0, Math.ceil(p * n) - 1))];
  return {
    media: Number((sum / n).toFixed(2)),
    p50: Number(pick(0.5).toFixed(2)),
    p95: Number(pick(0.95).toFixed(2)),
    p99: Number(pick(0.99).toFixed(2)),
    max: Number(sorted[n - 1].toFixed(2)),
    amostras: n
  };
}

let globalRef = 1;

class LoadBot {
  constructor(index, room, intervalMs, runId) {
    this.index = index;
    this.room = room;
    this.intervalMs = intervalMs;
    this.runId = runId;
    this.ws = null;
    this.joined = false;
    this.node = null;
    this.joinRef = null;
    this.seq = 0;
    this.closed = false;
    this.wasConnectedOnce = false;
    this.pending = new Map();
    this.seenQueue = [];
    this.seenSet = new Set();
    this.sendTimer = null;
    this.reconnectTimer = null;
    this.open();
  }

  open() {
    if (this.closed || loadState.runId !== this.runId) return;
    let ws;
    try {
      ws = new WebSocket(wsEndpoint);
    } catch {
      this.scheduleReconnect();
      return;
    }
    this.ws = ws;

    ws.addEventListener("open", () => {
      if (this.closed || ws !== this.ws) return;
      this.joinRef = String(globalRef++);
      this.push("phx_join", { usuario: `p4-bot-${this.index}`, chave: null }, `room:${this.room.id}`)
        .then((reply) => {
          if (this.closed || ws !== this.ws) return;
          if (this.wasConnectedOnce) {
            loadState.counters.reconexoes += 1;
          }
          this.wasConnectedOnce = true;
          this.joined = true;
          this.node = reply.no || "desconhecido";
          this.startSending();
        })
        .catch(() => {
          try { ws.close(); } catch {}
        });
    });

    ws.addEventListener("message", ({ data }) => {
      if (this.closed) return;
      let parsed;
      try {
        parsed = JSON.parse(data);
      } catch {
        return;
      }
      const [, ref, , event, payload] = parsed;
      if (event === "phx_reply") {
        const item = this.pending.get(ref);
        if (!item) return;
        this.pending.delete(ref);
        clearTimeout(item.timer);
        if (payload.status === "ok") item.resolve(payload.response);
        else item.reject(new Error(payload.response?.reason || "rejeitado"));
      } else if (event === "new_msg" && payload && payload.id) {
        this.handleBroadcast(payload);
      }
    });

    ws.addEventListener("close", () => this.handleDisconnect(ws));
    ws.addEventListener("error", () => this.handleDisconnect(ws));
  }

  handleBroadcast(payload) {
    if (this.seenSet.has(payload.id)) {
      loadState.counters.duplicadas += 1;
      return;
    }
    this.seenSet.add(payload.id);
    this.seenQueue.push(payload.id);
    if (this.seenQueue.length > DEDUP_WINDOW) {
      const oldest = this.seenQueue.shift();
      this.seenSet.delete(oldest);
    }

    if (typeof payload.texto === "string" && payload.texto.startsWith(`[P4:${this.runId}:`)) {
      loadState.counters.entregas += 1;
      loadState.windowDelivered += 1;
      const closeBracket = payload.texto.indexOf("]");
      if (closeBracket > 4) {
        const parts = payload.texto.slice(1, closeBracket).split(":");
        const sentPerf = Number(parts[2]);
        if (Number.isFinite(sentPerf)) {
          recordLatency(performance.now() - sentPerf);
        }
      }
    }
  }

  startSending() {
    clearTimeout(this.sendTimer);
    clearInterval(this.sendInterval);
    const jitter = Math.floor(Math.random() * this.intervalMs);
    this.sendTimer = setTimeout(() => {
      if (this.closed) return;
      this.tick();
      this.sendInterval = setInterval(() => this.tick(), this.intervalMs);
    }, jitter);
  }

  tick() {
    if (this.closed || !this.joined || loadState.runId !== this.runId) return;
    const t0 = performance.now();
    const texto = `[P4:${this.runId}:${t0.toFixed(2)}] Carga #${++this.seq} · bot-${this.index} (${this.room.nome})`;
    loadState.counters.enviadas += 1;
    loadState.windowSent += 1;
    this.push("new_msg", { texto }, `room:${this.room.id}`)
      .then(() => {
        loadState.counters.confirmadas += 1;
        loadState.windowAcked += 1;
        recordAckLatency(performance.now() - t0);
      })
      .catch(() => {
        loadState.counters.errosEnvio += 1;
      });
  }

  heartbeat() {
    if (this.closed || !this.joined || !this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    this.push("heartbeat", {}, "phoenix").catch(() => {});
  }

  push(event, payload, topic) {
    const ref = String(globalRef++);
    return new Promise((resolve, reject) => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
        reject(new Error("socket fechado"));
        return;
      }
      const timer = setTimeout(() => {
        this.pending.delete(ref);
        reject(new Error("timeout"));
      }, 8000);
      this.pending.set(ref, { resolve, reject, timer });
      try {
        const jRef = topic === "phoenix" ? null : this.joinRef;
        this.ws.send(JSON.stringify([jRef, ref, topic, event, payload]));
      } catch (err) {
        clearTimeout(timer);
        this.pending.delete(ref);
        reject(err);
      }
    });
  }

  handleDisconnect(ws) {
    if (ws !== this.ws) return;
    this.ws = null;
    this.joined = false;
    clearTimeout(this.sendTimer);
    clearInterval(this.sendInterval);
    for (const { reject, timer } of this.pending.values()) {
      clearTimeout(timer);
      reject(new Error("desconectado"));
    }
    this.pending.clear();
    if (!this.closed && loadState.active && loadState.runId === this.runId) {
      this.scheduleReconnect();
    }
  }

  scheduleReconnect() {
    clearTimeout(this.reconnectTimer);
    const delay = 250 + Math.floor(Math.random() * 650);
    this.reconnectTimer = setTimeout(() => this.open(), delay);
  }

  destroy() {
    this.closed = true;
    this.joined = false;
    clearTimeout(this.sendTimer);
    clearInterval(this.sendInterval);
    clearTimeout(this.reconnectTimer);
    for (const { timer } of this.pending.values()) clearTimeout(timer);
    this.pending.clear();
    if (this.ws) {
      try { this.ws.close(); } catch {}
      this.ws = null;
    }
  }
}

async function ensureBenchmarkRooms(count) {
  if (count <= 1) {
    return [{ id: "geral", nome: "Geral" }];
  }
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(wsEndpoint);
    const pending = new Map();
    const joinRef = String(globalRef++);
    const timeout = setTimeout(() => {
      try { ws.close(); } catch {}
      reject(new Error("Tempo esgotado preparando salas de benchmark."));
    }, 10000);

    function call(event, payload) {
      const ref = String(globalRef++);
      return new Promise((res, rej) => {
        pending.set(ref, { res, rej });
        ws.send(JSON.stringify([joinRef, ref, "catalog:rooms", event, payload]));
      });
    }

    ws.addEventListener("error", () => {
      clearTimeout(timeout);
      reject(new Error("Falha ao conectar no catálogo para preparar salas."));
    });

    ws.addEventListener("message", ({ data }) => {
      const [, ref, , event, payload] = JSON.parse(data);
      if (event === "phx_reply" && pending.has(ref)) {
        const { res, rej } = pending.get(ref);
        pending.delete(ref);
        if (payload.status === "ok") res(payload.response);
        else rej(new Error(payload.response?.reason || "Erro no catálogo"));
      }
    });

    ws.addEventListener("open", async () => {
      try {
        const joinReply = await call("phx_join", { usuario: "coord-p4" });
        const existing = new Map((joinReply.salas || []).map((s) => [s.nome, s]));
        const selected = [{ id: "geral", nome: "Geral" }];

        for (let i = 2; i <= count; i++) {
          const roomName = `Bench Sala ${i}`;
          if (existing.has(roomName)) {
            const r = existing.get(roomName);
            selected.push({ id: r.id, nome: r.nome });
          } else {
            const created = await call("create_room", { nome: roomName, tipo: "publica" });
            selected.push({ id: created.sala.id, nome: created.sala.nome });
          }
        }
        clearTimeout(timeout);
        try { ws.close(); } catch {}
        resolve(selected);
      } catch (err) {
        clearTimeout(timeout);
        try { ws.close(); } catch {}
        reject(err);
      }
    });
  });
}

function stopLoad() {
  clearTimeout(loadState.stopTimer);
  clearInterval(loadState.heartbeatTimer);
  loadState.stopTimer = null;
  loadState.heartbeatTimer = null;
  loadState.active = false;
  loadState.starting = false;
  for (const bot of loadState.bots) bot.destroy();
  loadState.bots = [];
  if (lastBeamMetrics?.resumo_cluster) {
    prevClusterMsgs = lastBeamMetrics.resumo_cluster.mensagens_cluster ?? prevClusterMsgs;
    prevClusterDeliveries = lastBeamMetrics.resumo_cluster.entregas_cluster ?? prevClusterDeliveries;
  }
}

async function startLoad(params) {
  stopLoad();

  const clients = clampInt(params.clients ?? 30, 1, 500);
  const roomsCount = clampInt(params.rooms ?? 1, 1, 15);
  const intervalMs = clampInt(params.intervalMs ?? 400, 50, 5000);
  const durationSec = clampInt(params.durationSec ?? 60, 0, 600);
  const preset = String(params.preset || "custom");

  const runId = ++loadState.runId;
  loadState.starting = true;
  loadState.active = true;
  loadState.preset = preset;
  loadState.config = { clients, rooms: roomsCount, intervalMs, durationSec };
  loadState.counters = freshCounters();
  loadState.recentLatencies = [];
  loadState.recentAckLatencies = [];
  loadState.reservoirLatencies = [];
  loadState.reservoirSeen = 0;
  loadState.peakMsgRate = 0;
  loadState.peakDeliveryRate = 0;
  loadState.startedAt = Date.now();
  loadState.endsAt = durationSec > 0 ? Date.now() + durationSec * 1000 : null;

  const targetRooms = await ensureBenchmarkRooms(roomsCount);
  if (loadState.runId !== runId || !loadState.active) return;
  loadState.rooms = targetRooms;

  // Rampa suave em lotes para não saturar o handshake simultâneo
  const batchSize = 25;
  for (let i = 0; i < clients; i++) {
    if (loadState.runId !== runId || !loadState.active) return;
    const room = targetRooms[i % targetRooms.length];
    loadState.bots.push(new LoadBot(i + 1, room, intervalMs, runId));
    if ((i + 1) % batchSize === 0 && i + 1 < clients) {
      await new Promise((r) => setTimeout(r, 60));
    }
  }

  loadState.starting = false;
  loadState.heartbeatTimer = setInterval(() => {
    for (const bot of loadState.bots) bot.heartbeat();
  }, 20000);

  if (durationSec > 0) {
    loadState.stopTimer = setTimeout(() => {
      if (loadState.runId === runId) stopLoad();
    }, durationSec * 1000);
  }
}

function clampInt(val, min, max) {
  const n = Math.round(Number(val));
  if (!Number.isFinite(n)) return min;
  return Math.min(max, Math.max(min, n));
}

function parseNginxStubStatus(text) {
  // Active connections: 42 \n server accepts handled requests \n 100 100 250 \n Reading: 0 Writing: 1 Waiting: 41
  const activeMatch = text.match(/Active connections:\s*(\d+)/i);
  const countsMatch = text.match(/\n\s*(\d+)\s+(\d+)\s+(\d+)\s*\n/);
  const rwwMatch = text.match(/Reading:\s*(\d+)\s+Writing:\s*(\d+)\s+Waiting:\s*(\d+)/i);
  if (!activeMatch) return null;
  return {
    conexoes_ativas: Number(activeMatch[1]),
    aceitas: countsMatch ? Number(countsMatch[1]) : 0,
    tratadas: countsMatch ? Number(countsMatch[2]) : 0,
    requisicoes_http: countsMatch ? Number(countsMatch[3]) : 0,
    reading: rwwMatch ? Number(rwwMatch[1]) : 0,
    writing: rwwMatch ? Number(rwwMatch[2]) : 0,
    waiting: rwwMatch ? Number(rwwMatch[3]) : 0
  };
}

async function pollCluster() {
  const now = Date.now();

  try {
    const res = await fetch(`${TARGET_URL}/metrics`, { signal: AbortSignal.timeout(1500) });
    if (!res.ok) throw new Error(`HTTP ${res.status} em /metrics`);
    const data = await res.json();

    // Enriquece cada nó com taxas por segundo (reduções/s e I/O KB/s) e detecção de mensagem recente
    const nextStats = new Map();
    if (Array.isArray(data.nos)) {
      for (const node of data.nos) {
        const prev = prevNodeStats.get(node.no);
        const red = node.atividade_vm?.reducoes ?? 0;
        const ioIn = node.atividade_vm?.io_entrada_kb ?? 0;
        const ioOut = node.atividade_vm?.io_saida_kb ?? 0;
        const msgs = node.mensagens_no?.publicadas ?? 0;
        let redPerSec = 0;
        let ioKbPerSec = 0;
        if (prev && now > prev.ts) {
          const dt = (now - prev.ts) / 1000;
          redPerSec = Math.max(0, Math.round((red - prev.red) / dt));
          ioKbPerSec = Number(Math.max(0, (ioIn - prev.ioIn + (ioOut - prev.ioOut)) / dt).toFixed(1));
          if (msgs > prev.msgs) {
            lastActiveNode = node.no;
            lastClusterMsgAt = now;
          }
        }
        const prevActiveAt = prev?.activeAt ?? 0;
        const activeAt = prev && msgs > prev.msgs ? now : prevActiveAt;
        nextStats.set(node.no, { ts: now, red, ioIn, ioOut, msgs, activeAt });
        node.taxas = { reducoes_seg: redPerSec, io_kb_seg: ioKbPerSec };
        node.ativo_recente = now - activeAt < 3000;
      }
    }
    prevNodeStats = nextStats;
    lastBeamMetrics = data;
    lastPollError = null;
  } catch (err) {
    lastPollError = err.message;
  }

  try {
    const lbRes = await fetch(`${TARGET_URL}/lb-status`, { signal: AbortSignal.timeout(1000) });
    if (lbRes.ok) {
      const txt = await lbRes.text();
      lastLbMetrics = parseNginxStubStatus(txt);
    } else {
      lastLbMetrics = null;
    }
  } catch {
    lastLbMetrics = null;
  }
}

let lastTickPerf = performance.now();
let prevClusterMsgs = null;
let prevClusterDeliveries = null;
let lastActiveNode = null;
let lastClusterMsgAt = 0;
const humanWindow = []; // janela deslizante de 4 ticks (~2s) para mensagens manuais

async function tickTelemetry() {
  await pollCluster();

  const nowPerf = performance.now();
  const dtSec = Math.max(0.1, (nowPerf - lastTickPerf) / 1000);
  lastTickPerf = nowPerf;

  const botSentRate = Math.round(loadState.windowSent / dtSec);
  const botAckRate = Math.round(loadState.windowAcked / dtSec);
  const botDeliveryRate = Math.round(loadState.windowDelivered / dtSec);
  loadState.windowSent = 0;
  loadState.windowAcked = 0;
  loadState.windowDelivered = 0;

  // Telemetria real do cluster (captura mensagens digitadas por humanos no chat em qualquer sala!)
  const resumo = lastBeamMetrics?.resumo_cluster;
  let humanMsgRate = 0;
  let humanDelivRate = 0;
  let serverLatencyMs = 0;

  if (resumo && typeof resumo.mensagens_cluster === "number") {
    const currMsgs = resumo.mensagens_cluster;
    const currDeliv = resumo.entregas_cluster ?? currMsgs;
    serverLatencyMs = Number(resumo.latencia_servidor_ms || 0);

    if (prevClusterMsgs === null) {
      prevClusterMsgs = currMsgs;
      prevClusterDeliveries = currDeliv;
      if (!loadState.active && loadState.counters.confirmadas === 0 && currMsgs > 0) {
        loadState.counters.enviadas = currMsgs;
        loadState.counters.confirmadas = currMsgs;
        loadState.counters.entregas = currDeliv;
      }
    } else {
      const deltaMsgs = Math.max(0, currMsgs - prevClusterMsgs);
      const deltaDeliv = Math.max(0, currDeliv - prevClusterDeliveries);
      prevClusterMsgs = currMsgs;
      prevClusterDeliveries = currDeliv;

      if (!loadState.active) {
        if (deltaMsgs > 0) {
          loadState.counters.enviadas += deltaMsgs;
          loadState.counters.confirmadas += deltaMsgs;
          loadState.counters.entregas += deltaDeliv;
          const latSample = Math.max(0.1, serverLatencyMs);
          recordLatency(latSample);
          recordAckLatency(latSample);
        }
        humanWindow.push({ msgs: deltaMsgs, deliv: deltaDeliv, dt: dtSec });
        if (humanWindow.length > 4) humanWindow.shift();
        const sumMsgs = humanWindow.reduce((a, x) => a + x.msgs, 0);
        const sumDeliv = humanWindow.reduce((a, x) => a + x.deliv, 0);
        const sumDt = Math.max(0.5, humanWindow.reduce((a, x) => a + x.dt, 0));
        humanMsgRate = sumMsgs > 0 ? Math.max(1, Math.round(sumMsgs / sumDt)) : 0;
        humanDelivRate = sumDeliv > 0 ? Math.max(1, Math.round(sumDeliv / sumDt)) : 0;
      } else {
        humanWindow.length = 0;
      }
    }
  }

  const sentRate = loadState.active ? botSentRate : humanMsgRate;
  const ackRate = loadState.active ? botAckRate : humanMsgRate;
  const deliveryRate = loadState.active ? botDeliveryRate : humanDelivRate;

  if (sentRate > loadState.peakMsgRate) loadState.peakMsgRate = sentRate;
  if (deliveryRate > loadState.peakDeliveryRate) loadState.peakDeliveryRate = deliveryRate;

  const botsOnline = loadState.bots.filter((b) => b.joined).length;
  const botsByNode = {};
  for (const bot of loadState.bots) {
    if (bot.joined && bot.node) {
      botsByNode[bot.node] = (botsByNode[bot.node] || 0) + 1;
    }
  }

  const liveLatency = computePercentiles(loadState.recentLatencies);
  const overallLatency = computePercentiles(loadState.reservoirLatencies);
  const ackLatency = computePercentiles(loadState.recentAckLatencies);

  // Quando não há carga ativa nem mensagem nos últimos 2s, a curva ao vivo de latência volta à base 0
  const chartLat = (loadState.active || humanMsgRate > 0) ? liveLatency : { p50: 0, p95: 0, p99: 0 };

  const elapsedSec = loadState.startedAt ? Math.round((Date.now() - loadState.startedAt) / 1000) : 0;
  const remainingSec =
    loadState.active && loadState.endsAt ? Math.max(0, Math.ceil((loadState.endsAt - Date.now()) / 1000)) : null;

  const snapshot = {
    ts: Date.now(),
    target_url: TARGET_URL,
    erro_coleta: lastPollError,
    cluster: lastBeamMetrics,
    balanceador: lastLbMetrics,
    carga: {
      ativa: loadState.active,
      iniciando: loadState.starting,
      preset: loadState.preset,
      config: loadState.config,
      salas_alvo: loadState.rooms,
      bots_configurados: loadState.bots.length,
      bots_online: botsOnline,
      bots_por_no: botsByNode,
      ultimo_no_ativo: Date.now() - lastClusterMsgAt < 4000 ? lastActiveNode : null,
      tempo_decorrido_seg: elapsedSec,
      tempo_restante_seg: remainingSec,
      taxas: {
        msgs_enviadas_seg: sentRate,
        confirmacoes_seg: ackRate,
        entregas_recebidas_seg: deliveryRate,
        pico_msgs_seg: loadState.peakMsgRate,
        pico_entregas_seg: loadState.peakDeliveryRate
      },
      latencia_janela_ms: liveLatency,
      latencia_acumulada_ms: overallLatency,
      latencia_ack_ms: ackLatency,
      contadores: { ...loadState.counters }
    }
  };

  const chartPoint = {
    ts: snapshot.ts,
    msgs_seg: sentRate,
    entregas_seg: deliveryRate,
    p50: chartLat.p50,
    p95: chartLat.p95,
    p99: chartLat.p99,
    sessoes_crdt: lastBeamMetrics?.resumo_cluster?.sessoes_ativas ?? botsOnline,
    processos_beam: lastBeamMetrics?.resumo_cluster?.processos_cluster ?? 0,
    memoria_mb: lastBeamMetrics?.resumo_cluster?.memoria_cluster_mb ?? 0
  };

  history.push(chartPoint);
  if (history.length > HISTORY_LIMIT) history.shift();

  const payload = `event: tick\ndata: ${JSON.stringify({ snapshot, point: chartPoint })}\n\n`;
  for (const res of sseClients) {
    try {
      res.write(payload);
    } catch {
      sseClients.delete(res);
    }
  }
}

setInterval(() => {
  tickTelemetry().catch(() => {});
}, 500);

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
      if (body.length > 32_768) req.destroy();
    });
    req.on("end", () => {
      if (!body) return resolve({});
      try {
        resolve(JSON.parse(body));
      } catch (err) {
        reject(err);
      }
    });
    req.on("error", reject);
  });
}

function sendJson(res, status, obj) {
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store"
  });
  res.end(JSON.stringify(obj, null, 2));
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
  const pathname = url.pathname.replace(/^\/painel(?=\/|$)/, "") || "/";

  if (req.method === "GET" && url.pathname === "/painel") {
    res.writeHead(302, { Location: "/painel/" });
    res.end();
    return;
  }

  if (req.method === "GET" && pathname === "/") {
    try {
      const html = fs.readFileSync(HTML_PATH, "utf8");
      res.writeHead(200, {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store"
      });
      res.end(html);
    } catch (err) {
      sendJson(res, 500, { error: `Erro ao ler interface: ${err.message}` });
    }
    return;
  }

  if (req.method === "GET" && pathname === "/api/stream") {
    res.writeHead(200, {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive"
    });
    res.write(`event: init\ndata: ${JSON.stringify({ history, target_url: TARGET_URL, public_chat_url: process.env.PUBLIC_CHAT_URL || null })}\n\n`);
    sseClients.add(res);
    req.on("close", () => sseClients.delete(res));
    return;
  }

  if (req.method === "POST" && pathname === "/api/load/start") {
    try {
      const body = await readJsonBody(req);
      startLoad(body).catch((err) => {
        console.error("[Painel P4] Erro ao iniciar carga:", err.message);
        stopLoad();
      });
      sendJson(res, 200, { ok: true, config: loadState.config, preset: loadState.preset });
    } catch (err) {
      sendJson(res, 400, { ok: false, error: err.message });
    }
    return;
  }

  if (req.method === "POST" && pathname === "/api/load/stop") {
    stopLoad();
    sendJson(res, 200, { ok: true });
    return;
  }

  if (req.method === "POST" && pathname === "/api/load/reset") {
    stopLoad();
    loadState.preset = "inativo";
    loadState.counters = freshCounters();
    loadState.recentLatencies = [];
    loadState.recentAckLatencies = [];
    loadState.reservoirLatencies = [];
    loadState.reservoirSeen = 0;
    loadState.peakMsgRate = 0;
    loadState.peakDeliveryRate = 0;
    loadState.startedAt = null;
    loadState.endsAt = null;
    lastActiveNode = null;
    lastClusterMsgAt = 0;
    humanWindow.length = 0;
    if (lastBeamMetrics?.resumo_cluster) {
      prevClusterMsgs = lastBeamMetrics.resumo_cluster.mensagens_cluster ?? 0;
      prevClusterDeliveries = lastBeamMetrics.resumo_cluster.entregas_cluster ?? 0;
    }
    history.length = 0;
    sendJson(res, 200, { ok: true });
    return;
  }

  if (req.method === "GET" && pathname === "/api/export") {
    const lat = computePercentiles(loadState.reservoirLatencies);
    const report = {
      exportado_em: new Date().toISOString(),
      alvo: TARGET_URL,
      cluster: lastBeamMetrics,
      balanceador: lastLbMetrics,
      ensaio_carga: {
        preset: loadState.preset,
        configuracao: loadState.config,
        salas: loadState.rooms,
        pico_msgs_por_segundo: loadState.peakMsgRate,
        pico_entregas_por_segundo: loadState.peakDeliveryRate,
        latencia_broadcast_ms: lat,
        contadores: loadState.counters
      }
    };
    res.writeHead(200, {
      "Content-Type": "application/json; charset=utf-8",
      "Content-Disposition": `attachment; filename="relatorio-metricas-p4-${Date.now()}.json"`
    });
    res.end(JSON.stringify(report, null, 2));
    return;
  }

  if (req.method === "GET" && pathname === "/health") {
    sendJson(res, 200, { status: "ok", servico: "painel-metricas-p4", target: TARGET_URL });
    return;
  }

  sendJson(res, 404, { error: "Rota não encontrada." });
});

server.listen(PORT, BIND, () => {
  console.log(`[Pessoa 04] Painel de Métricas Ao Vivo ouvindo em http://localhost:${PORT} -> alvo: ${TARGET_URL}`);
});
