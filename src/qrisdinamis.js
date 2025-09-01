// src/store-bot.js
require("dotenv").config();
const {
  default: makeWASocket,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  Browsers,
  jidNormalizedUser
} = require("@whiskeysockets/baileys");
const qrcodeTerminal = require("qrcode-terminal");
const express = require("express");
const pino = require("pino");
const fs = require("fs");
const path = require("path");
const { Low } = require("lowdb");
const { JSONFile } = require("lowdb/node");
const { customAlphabet } = require("nanoid");
const QRCode = require("qrcode");

// ===== ENV =====
const OWNER = (process.env.OWNER || "").replace(/[^0-9]/g, "");
const USE_PAIRING = process.env.PAIRING === "1";
const STORE_NAME = process.env.STORE_NAME || "Vienze Store";
const QRIS_PAYLOAD_BASE = process.env.QRIS_PAYLOAD || "";
const QRIS_DYNAMIC = process.env.QRIS_DYNAMIC === "1";
const EXPIRE_HOURS = Math.max(0, Number(process.env.EXPIRE_HOURS || "2"));
const RATE_LIMIT_MS = Math.max(0, Number(process.env.RATE_LIMIT_MS || "1200"));
const MAX_MSG_CHARS = Math.max(50, Number(process.env.MAX_MSG_CHARS || "500"));
const AUDIT_LOG = process.env.AUDIT_LOG === "1";
const WEBHOOK_PORT = Number(process.env.WEBHOOK_PORT || "3001");
const CS_CONTACT = process.env.CS_CONTACT || "Hubungi admin: wa.me/62XXXXXXXXXXX";

// ===== PATHS =====
const root = process.cwd();
const dataDir = path.join(root, "data");
const logsDir = path.join(root, "logs");
const backupsDir = path.join(root, "backups");
for (const d of [dataDir, logsDir, backupsDir]) if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });

// ===== DB =====
const db = new Low(new JSONFile(path.join(dataDir, "db.json")), { users: {}, orders: [] });
const productsPath = path.join(root, "products.json");
function loadProducts() { if (!fs.existsSync(productsPath)) fs.writeFileSync(productsPath, "[]"); return JSON.parse(fs.readFileSync(productsPath, "utf8")); }
let PRODUCTS = loadProducts();
function saveProducts() { fs.writeFileSync(productsPath, JSON.stringify(PRODUCTS, null, 2)); backupSnapshot(); }

// ===== UTIL =====
const nanoID = customAlphabet("ABCDEFGHJKLMNPQRSTUVWXYZ23456789", 6);
const formatPrice = n => "Rp " + Number(n || 0).toLocaleString("id-ID");
const pct = x => Math.max(0, Math.min(100, Number(x || 0)));
const finalPrice = (p, d) => Math.round(Number(p || 0) * (100 - pct(d)) / 100);
const cleanNumber = n => String(n || "").replace(/[^0-9]/g, "");
const isAdminJid = jid => OWNER && cleanNumber(jid) === OWNER;
const capitalize = s => (s||"").charAt(0).toUpperCase() + (s||"").slice(1);
const now = () => Date.now();
const todayStr = () => new Date().toISOString().slice(0,10);
const clampText = (s, max=4000) => { s = String(s||""); return s.length>max ? (s.slice(0,max)+"…") : s; };
const audit = line => { if (!AUDIT_LOG) return; const p = path.join(logsDir, `audit-${todayStr()}.log`); fs.appendFileSync(p, `[${new Date().toISOString()}] ${line}\n`); };
function backupSnapshot(){ const d = todayStr(); const dir = path.join(backupsDir, d); if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true }); try { fs.copyFileSync(productsPath, path.join(dir, "products.json")); const dbPath = path.join(dataDir, "db.json"); if (fs.existsSync(dbPath)) fs.copyFileSync(dbPath, path.join(dir, "db.json")); } catch {} }
function getUser(jid){ const id = jidNormalizedUser(jid); db.data.users[id] = db.data.users[id] || { name:"", banned:false, lastTs:0 }; return db.data.users[id]; }
const productById = id => PRODUCTS.find(p => p.id.toLowerCase() === String(id).toLowerCase());
const makeOrderId = () => "VS-" + nanoID();

// Timestamp WIB
function formatTs(ts) {
  if (!ts) return "-";
  const dt = new Date(ts);
  const pad = n => String(n).padStart(2, "0");
  const y = dt.toLocaleString("id-ID", { timeZone: "Asia/Jakarta", year: "numeric" });
  const M = pad(parseInt(dt.toLocaleString("id-ID", { timeZone: "Asia/Jakarta", month: "2-digit" }), 10));
  const d = pad(parseInt(dt.toLocaleString("id-ID", { timeZone: "Asia/Jakarta", day: "2-digit" }), 10));
  const h = pad(parseInt(dt.toLocaleString("id-ID", { timeZone: "Asia/Jakarta", hour: "2-digit", hour12: false }), 10));
  const m = pad(parseInt(dt.toLocaleString("id-ID", { timeZone: "Asia/Jakarta", minute: "2-digit" }), 10));
  const s = pad(parseInt(dt.toLocaleString("id-ID", { timeZone: "Asia/Jakarta", second: "2-digit" }), 10));
  return `${y}-${M}-${d} ${h}:${m}:${s} WIB`;
}

// Divider
const DIV = "——————————";

// ===== EMV/QRIS DYNAMIC BUILD =====
// Helper: remove Tag 63 CRC from payload, if exists
function stripCRC(p) {
  // Tag 63 has format 63LLCCCC (LL=04), usually at the end
  return p.replace(/63(04)[0-9A-F]{4}$/i, "");
}

// Helper: find TLV tag by 2-char tag (e.g., "54", "62"). Returns {start,end,len,val} or null
function findTag(p, tag) {
  const r = new RegExp(`(${tag})([0-9]{2})`, "g");
  let m;
  while ((m = r.exec(p)) !== null) {
    const idx = m.index;
    const len = parseInt(p.substr(idx+2, 2), 10);
    const valStart = idx + 4;
    const valEnd = valStart + len;
    return { start: idx, end: valEnd, len, val: p.substring(valStart, valEnd) };
  }
  return null;
}

// Helper: set or replace TLV tag
function setTag(p, tag, valueRaw) {
  const val = String(valueRaw);
  const len = String(val.length).padStart(2, "0");
  const chunk = `${tag}${len}${val}`;
  const hit = findTag(p, tag);
  if (hit) {
    return p.substring(0, hit.start) + chunk + p.substring(hit.end);
  }
  // insert before CRC if exists, else append
  const without = stripCRC(p);
  return without + chunk;
}

// Helper: set Tag 62 (Additional Data) with subtag 01 (Bill Number)
function setTag62BillNumber(p, billNo) {
  const subVal = `01${String(billNo.length).padStart(2,"0")}${billNo}`;
  // We can replace entire Tag62 with only subtag 01
  return setTag(p, "62", subVal);
}

// CRC16-CCITT (0x1021)
function crc16ccitt(hexStr) {
  // Compute over bytes; input is ASCII string of digits (not hex bytes), so we use its char codes
  let crc = 0xFFFF;
  for (let i = 0; i < hexStr.length; i++) {
    crc ^= (hexStr.charCodeAt(i) & 0xFF) << 8;
    for (let j = 0; j < 8; j++) {
      if ((crc & 0x8000) !== 0) crc = (crc << 1) ^ 0x1021;
      else crc <<= 1;
      crc &= 0xFFFF;
    }
  }
  return crc.toString(16).toUpperCase().padStart(4, "0");
}

// Build dynamic QRIS payload: add Tag54 (amount) & Tag62.01 (bill number), recalc Tag63
function buildDynamicQRIS(basePayload, amountIDR, orderId) {
  if (!basePayload) throw new Error("QRIS base payload kosong");
  // format amount: 2 decimals with dot (IDR)
  const amt = (Number(amountIDR) || 0).toFixed(2);
  let p = stripCRC(basePayload);
  // Set Tag 54 (amount)
  p = setTag(p, "54", amt);
  // Set Tag 62 with subtag 01 = bill number (order id)
  p = setTag62BillNumber(p, orderId);
  // Append Tag63 placeholder for CRC calculation
  const preCRC = p + "6304";
  const crc = crc16ccitt(preCRC);
  return preCRC + crc;
}

// ===== QR code buffer =====
async function qrisPngBufferFromPayload(payload) {
  return await QRCode.toBuffer(payload, { type:"png", errorCorrectionLevel:"M", margin:2, width:512 });
}

// ===== APP LOGIC UTILS =====
function prettyMenu(){
  const header = [
    `*🛍️ List Produk — ${STORE_NAME}*`,
    "",
    "💡 Cara order:",
    "1) Cek *kode produk* di bawah",
    "2) Ketik: *beli <kode> <jumlah>*",
    "   Contoh: *beli net1u 1*",
    "3) Bot kasih *ID Order* + instruksi bayar (QRIS)",
    "4) Setelah transfer, ketik: *bayar <ID Order>* (opsional)",
    "5) Admin konfirmasi → produk dikirim",
    ""
  ].join("\n");

  const blocks = PRODUCTS.map((p, idx) => {
    const price = finalPrice(p.price,p.discount);
    const desc = (p.descLines||[]).map(d => "  - " + d).join("\n") || "  -";
    const lines = [
      `~ ${p.title}`,
      `• 💸 Harga: ${formatPrice(price)}${p.discount ? ` (diskon ${p.discount}%)` : ""}`,
      "• 📝 Deskripsi:",
      desc,
      `• 🏷️ Kode: ${p.id}`,
      `• 📦 Stok: ${p.stock}`,
      `• 🔥 Terjual: ${p.sold || 0}`
    ].join("\n");
    return idx === PRODUCTS.length - 1 ? lines : `${lines}\n${DIV}`;
  });

  return `${header}\n${blocks.join("\n")}\n\n_✍️ Ketik: beli <kode> <jumlah>_`;
}

function userHelp() {
  return [
    "❓ *Perintah tidak dikenali.*",
    "",
    "*📚 Panduan Singkat*",
    "• 🧾 *menu* — lihat daftar produk",
    "• 🛒 *beli <kode> <jumlah>* — contoh: *beli net1u 1*",
    "• 💳 *bayar <idorder>* — _opsional_ (manual)",
    "• 🔎 *cekorder <id>* — detail 1 order",
    "• 📂 *orders* — daftar semua order kamu",
    "• 👨‍💻 *cs* — kontak customer support",
    "",
    "*Contoh lengkap:*",
    "1) Ketik *menu* dan lihat *kode produk*",
    "2) Ketik *beli net1u 1*",
    "3) Scan QR & bayar *nominal persis* (sudah termasuk *kode unik 3 digit*)",
    "4) *Verifikasi otomatis* ✅ (tidak perlu ketik *bayar*)"
  ].join("\n");
}

function adminHelp() {
  return [
    "*🛠️ Admin Help*",
    "",
    "• ✅ konfirmasi <id> <nomor> — tandai order PAID (manual)",
    "• 📦 send <id> <nomor> email:...|password:...|other:...|note:...|rules:...",
    "• ⛔ batal <id> <nomor> [alasan] — batalkan order",
    "• 💵 refund <id> <nomor> [alasan] — tandai REFUNDED",
    "• 🧾 /orders — list 10 order terakhir",
    "",
    "📦 CRUD Produk:",
    "• addprod id|title|category|price|discount|stock|sold|desc1;desc2",
    "• prod <id> — detail produk",
    "• prods [page] — list produk",
    "• search <kata> — cari produk",
    "• setharga <id>|<price>|<discount>",
    "• setstok <id>|<qty>",
    "• settitle <id>|<title>",
    "• setcat <id>|<category>",
    "• setdesc <id>|desc1;desc2;desc3",
    "• delprod <id> — hapus (konfirmasi: ya <id>)",
    "",
    "📣 Broadcast:",
    "• bc all Pesan kamu...",
    "",
    "🔐 Secret:",
    "• adminhelp@ — tampilkan help admin",
    ""
  ].join("\n");
}

// Kode unik 3 digit
function pickUniqueAdd(baseTotal, existingOrders) {
  let add = (Math.floor(Math.random() * 300) + 1);
  const taken = new Set(
    existingOrders
      .filter(o => (o.status === "NEW" || o.status === "AWAIT_CONFIRM") && o.expectedAmount)
      .map(o => o.expectedAmount)
  );
  let tries = 0;
  while (taken.has(baseTotal + add) && tries < 350) {
    add = (add % 300) + 1;
    tries++;
  }
  return add;
}

function summarizeOrderItems(items){
  return items.map(i => {
    const p = productById(i.id);
    const sub = finalPrice(p.price, p.discount) * i.qty;
    return `• ${p.title} x${i.qty} = ${formatPrice(sub)}`;
  }).join("\n");
}

function buildPaymentCaption(order, itemsText){
  const base = formatPrice(order.total);
  const expect = formatPrice(order.expectedAmount);
  const deadline = EXPIRE_HOURS>0 ? `⏱️ Batas waktu: ${EXPIRE_HOURS} jam (otomatis batal jika lewat)` : null;

  return [
    "*💳 Pembayaran via QRIS*",
    "",
    `ID Order: *${order.id}*`,
    `🕒 Dibuat: ${formatTs(order.createdAt)}`,
    "",
    "🛍️ *Item:*",
    itemsText,
    "—",
    `💰 Total (sebelum kode unik): *${base}*`,
    `🔢 Total yang harus dibayar: *${expect}*`,
    "_(sudah termasuk kode unik 3 digit untuk verifikasi otomatis)_",
    "",
    "📌 *Cara bayar:*",
    "1) Scan QR di atas (QRIS)",
    `2) *Tidak perlu isi nominal* (QR sudah berisi jumlah yang benar)`,
    "3) Selesaikan pembayaran — verifikasi otomatis ✅",
    deadline ? ("\n" + deadline) : ""
  ].join("\n").trim();
}

function allowProcess(u){ const t=Date.now(); if (RATE_LIMIT_MS<=0) {u.lastTs=t;return true;} if (t-(u.lastTs||0)<RATE_LIMIT_MS) return false; u.lastTs=t; return true; }

async function checkExpiry(sendFunc){
  if (EXPIRE_HOURS <= 0) return;
  const limit = EXPIRE_HOURS*3600*1000, nowTs=Date.now();
  let changed=false;
  for (const o of db.data.orders){
    if (o.status==="NEW" && (nowTs-o.createdAt)>limit){
      o.status="EXPIRED"; changed=true;
      try{ await sendFunc(o.userJid, `⏰ Order *${o.id}* sudah *expired*. Silakan buat order baru.\n🕒 Waktu: ${formatTs(nowTs)}`);}catch{}
      audit(`EXPIRE ${o.id}`);
    }
  }
  if (changed) await db.write();
}

// Rupiah parser (notifikasi)
function parseRupiahFromText(txt){
  if (!txt) return null;
  const m = txt.replace(/[\u00A0]/g, " ").match(/Rp\s*([\d\.]+)/i);
  if (!m) return null;
  const num = Number(m[1].replace(/\./g, ""));
  return Number.isFinite(num) ? num : null;
}
function findOrderByPaidAmount(amount){
  const nowTs = Date.now();
  const ttl = 24 * 3600 * 1000;
  return db.data.orders.find(o =>
    (o.status === "NEW" || o.status === "AWAIT_CONFIRM") &&
    o.expectedAmount === amount &&
    (nowTs - (o.createdAt || 0) < ttl)
  );
}

// ====== SOCKET REFERENCE (untuk webhook tahan reconnect) ======
let currentSock = null;
function setCurrentSock(sock){ currentSock = sock; }
function getCurrentSock(){ return currentSock; }
async function safeSendText(jid, text){
  const sock = getCurrentSock();
  if (!sock) throw new Error("Socket not ready");
  return sock.sendMessage(jid, { text: clampText(text, 4000) });
}
async function safeDelete(jid, key){
  const sock = getCurrentSock();
  if (!sock) throw new Error("Socket not ready");
  return sock.sendMessage(jid, { delete: key });
}
async function safeSendImage(jid, buffer, caption){
  const sock = getCurrentSock();
  if (!sock) throw new Error("Socket not ready");
  return sock.sendMessage(jid, { image: buffer, caption: clampText(caption, 4000) });
}

// ===== WEBHOOK (start sekali, socket dinamis) =====
let webhookStarted = false;
function startPaymentWebhook(){
  if (webhookStarted) return;
  webhookStarted = true;

  const app = express();
  app.use(express.urlencoded({ extended: false }));
  app.use(express.json());

  app.post("/api/transactions/pending", async (req, res) => {
    try {
      const body = req.body || {};
      const text = body.text || body.bigtext || body.title || "";
      const amount = parseRupiahFromText(text);
      if (!amount) return res.status(200).json({ ok: true, info: "no-amount" });

      const order = findOrderByPaidAmount(amount);
      if (!order) return res.status(200).json({ ok: true, info: "no-order" });

      if (order.status !== "PAID") {
        order.status = "PAID";
        order.confirmedAt = now();
        order.paidAt = order.paidAt || now();
        await db.write();
        audit(`AUTO-PAID ${order.id} amount=${amount}`);

        try { if (order.paymentMsgKey) await safeDelete(order.userJid, order.paymentMsgKey); } catch(e){}

        await safeSendText(order.userJid,
          [
            "✅ *Pembayaran kamu sudah diterima!*",
            `ID Order: *${order.id}*`,
            `Nominal: *${formatPrice(order.expectedAmount)}*`,
            `🕒 Waktu: ${formatTs(order.paidAt)}`,
            "",
            "Terima kasih ya 🙏 Admin akan segera memproses dan mengirimkan pesanan kamu.",
            "Jika butuh bantuan, balas chat ini."
          ].join("\n")
        );

        if (OWNER) {
          const itemsText = summarizeOrderItems(order.items);
          await safeSendText(
            OWNER + "@s.whatsapp.net",
            [
              "💸 *Auto-verify pembayaran diterima*",
              `ID: *${order.id}*`,
              `User: ${cleanNumber(order.userJid)}`,
              "",
              "🛍️ Order:",
              itemsText,
              "—",
              `Nominal: *${formatPrice(order.expectedAmount)}*`,
              `🕒 Waktu: ${formatTs(order.paidAt)}`,
              "",
              "Silakan proses pengiriman:",
              `*send ${order.id} ${cleanNumber(order.userJid)} email:...|password:...|note:...|rules:...*`
            ].join("\n")
          );
        }
      }

      res.json({ ok: true, matched: order.id, amount });
    } catch (e) {
      console.error("webhook error:", e);
      res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  });

  try {
    const server = app.listen(WEBHOOK_PORT, () => {
      console.log(`Server running di http://localhost:${WEBHOOK_PORT}`);
      console.log(`Endpoint siap: POST http://localhost:${WEBHOOK_PORT}/api/transactions/pending`);
    });
    server.on("error", (err) => {
      if (err && err.code === "EADDRINUSE") {
        console.log(`⚠️ Port ${WEBHOOK_PORT} sudah dipakai. Webhook tidak akan diduplikasi.`);
      } else {
        console.error("Webhook server error:", err);
      }
    });
  } catch (err) {
    if (err && err.code === "EADDRINUSE") {
      console.log(`⚠️ Port ${WEBHOOK_PORT} sudah dipakai. Webhook tidak akan diduplikasi.`);
    } else {
      console.error("Webhook start error:", err);
    }
  }
}

// =========================
// BOOTSTRAP + RECONNECTOR
// =========================
async function startSock() {
  await db.read();
  const { state, saveCreds } = await useMultiFileAuthState("./auth");
  const { version } = await fetchLatestBaileysVersion();

  let pairingRequested = false;
  let reconnectTimer = null;

  const sock = makeWASocket({
    version,
    logger: pino({ level:"silent" }),
    printQRInTerminal: false,
    browser: Browsers.ubuntu("Chrome"),
    auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, pino({ level:"silent" })) },
    markOnlineOnConnect: false
  });

  // expose socket terbaru untuk webhook
  setCurrentSock(sock);

  async function safeReconnect(delayMs = 1500) {
    if (reconnectTimer) return;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      console.log("🔁 Reconnecting…");
      startSock().catch(err => console.error("reconnect start error:", err));
    }, delayMs);
  }

  async function tryPairingOnce() {
    if (pairingRequested || !USE_PAIRING || sock.authState.creds.registered) return;
    pairingRequested = true;

    if (!OWNER) { console.log("Set OWNER=62xxxxxxxxxx untuk pairing code."); return; }
    for (let attempt = 1; attempt <= 5; attempt++) {
      try {
        const code = await sock.requestPairingCode(OWNER);
        console.log("\n👉 Pairing Code:", code, "\nBuka WhatsApp > Perangkat Tertaut > Tautkan dengan nomor telepon\n");
        break;
      } catch (e) {
        const msg = e?.output?.payload?.message || e.message || String(e);
        console.log(`Gagal pairing (attempt ${attempt}): ${msg}`);
        if (attempt === 5) {
          console.log("❌ Gagal meminta pairing code berkali-kali. Coba ulangi: rm -rf auth && npm start");
        } else {
          await new Promise(r => setTimeout(r, 1000 * attempt));
        }
      }
    }
  }

  sock.ev.on("creds.update", saveCreds);
  sock.ev.on("connection.update", u => {
    const { connection, lastDisconnect, qr } = u;

    if (qr && !USE_PAIRING) {
      console.log("\nScan QR ini dari WhatsApp:");
      try { qrcodeTerminal.generate(qr, { small: true }); } catch {}
    }

    if (connection === "connecting") setTimeout(() => tryPairingOnce().catch(()=>{}), 300);
    if (connection === "open") {
      console.log("✅ Connected.");
      setCurrentSock(sock); // update ref untuk webhook
      pairingRequested = true;
    }
    if (connection === "close") {
      const reason = lastDisconnect?.error?.output?.payload?.message || lastDisconnect?.error?.message || "Connection was lost";
      console.log("❌ Disconnected:", reason);
      const status = lastDisconnect?.error?.output?.statusCode;
      const isLoggedOut = status === 401 || /logged out/i.test(reason || "");
      if (isLoggedOut) console.log("🚪 Session logged out. Hapus folder auth/ lalu jalankan ulang untuk login lagi.");
      else safeReconnect(1500);
    }
  });

  // Start webhook sekali (server process-level)
  startPaymentWebhook();

  // Wrappers (pakai socket terkini)
  const send = (jid, text) => safeSendText(jid, text);
  const sendImage = (jid, buf, cap) => safeSendImage(jid, buf, cap);

  setInterval(()=>checkExpiry(send), 5*60*1000);

  async function handleMessage(mUp){
    try{
      const m = mUp.messages?.[0];
      if (!m || !m.message || m.key.fromMe) return;
      const jid = m.key.remoteJid;
      if (jid.endsWith("@g.us")) return;

      const sender = jidNormalizedUser(m.key.participant || jid);
      const isAdmin = isAdminJid(sender);
      const user = getUser(sender); user.name ||= (m.pushName || "Customer");

      const body = m.message.conversation || m.message.extendedTextMessage?.text || m.message.imageMessage?.caption || m.message.videoMessage?.caption || "";
      let text = (body||"").trim(); if (!text) return;
      if (text.length > MAX_MSG_CHARS) text = text.slice(0, MAX_MSG_CHARS);
      if (user.banned) { await send(jid, "🚫 Akses dibatasi. Hubungi admin."); return; }
      if (!allowProcess(user)) return;

      const lower = text.toLowerCase();

      // secret admin help
      if (text.trim().toLowerCase() === "adminhelp@") {
        if (!isAdmin) { await send(jid, "Perintah khusus admin."); return; }
        audit(`ADMINHELP by ${sender}`);
        await send(jid, adminHelp());
        return;
      }

      // Kontak CS
      if (["cs","kontak","contact","support"].includes(lower)) {
        await send(jid, `👨‍💻 *Customer Support*\n${CS_CONTACT}\n🕒 Waktu: ${formatTs(now())}`);
        return;
      }

      // MENU
      if (["menu","/menu","help","/help","/start","start"].includes(lower)) {
        await send(jid, prettyMenu()); return;
      }

      // CEK ORDER
      if (lower.startsWith("cekorder ")) {
        const id = text.split(/\s+/)[1];
        const o = db.data.orders.find(x => x.id === id && x.userJid === sender);
        if (!o) { await send(jid, "⚠️ Order tidak ditemukan."); return; }
        const itemsText = summarizeOrderItems(o.items);
        const lines = [
          `🧾 *Detail Order*`,
          `ID: *${o.id}*`,
          `Status: *${o.status}*`,
          "",
          "🛍️ *Item:*",
          itemsText,
          "—",
          `Total: *${formatPrice(o.total)}*`,
          o.expectedAmount ? `Tagihan (kode unik): *${formatPrice(o.expectedAmount)}*` : null,
          "",
          "*🕒 Waktu:*",
          `• Dibuat: ${formatTs(o.createdAt)}`,
          o.paidAt        ? `• Dibayar: ${formatTs(o.paidAt)}` : null,
          o.confirmedAt   ? `• Dikonfirmasi: ${formatTs(o.confirmedAt)}` : null,
          o.sentAt        ? `• Dikirim: ${formatTs(o.sentAt)}` : null,
          o.canceledAt    ? `• Dibatalkan: ${formatTs(o.canceledAt)}` : null,
          o.refundedAt    ? `• Refund: ${formatTs(o.refundedAt)}` : null
        ].filter(Boolean);
        await send(jid, lines.join("\n"));
        return;
      }

      // ORDERS
      if (lower === "orders") {
        const list = [...db.data.orders].filter(o => o.userJid === sender).reverse().slice(0, 10);
        if (!list.length) { await send(jid, "Belum ada order."); return; }
        const out = list.map(o => {
          const id = `*${o.id}*`;
          const st = `*${o.status}*`;
          const amt = `*${formatPrice(o.total)}*`;
          const tagih = o.expectedAmount ? ` (tagihan: *${formatPrice(o.expectedAmount)}*)` : "";
          const ts = formatTs(o.createdAt);
          return `• ${id} — ${st} — ${amt}${tagih} — ${ts}`;
        }).join("\n");
        await send(jid, `🧾 *Order Kamu*\n${out}`);
        return;
      }

      // BELI
      if (lower.startsWith("beli ")) {
        const parts = text.split(/\s+/);
        const code = parts[1];
        const qty = Math.max(1, Number(parts[2] || "1") || 1);
        const p = productById(code);
        if (!p) { await send(jid, "⚠️ Kode produk tidak ditemukan. Ketik *menu* untuk melihat daftar."); return; }
        if (p.stock < qty) { await send(jid, `⚠️ Stok tidak cukup. Sisa: ${p.stock}.`); return; }

        const orderId = makeOrderId();
        const items = [{ id: p.id, qty }];
        const total = items.reduce((acc,it)=>{ const _p=productById(it.id); return acc + finalPrice(_p.price,_p.discount)*it.qty; }, 0);

        const uniqueAdd = pickUniqueAdd(total, db.data.orders);
        const expectedAmount = total + uniqueAdd;

        const order = {
          id: orderId,
          userJid: sender,
          name: user.name,
          items,
          total,
          uniqueAdd,
          expectedAmount,
          paymentMsgKey: null,
          qrisPayload: null,
          status: "NEW",
          createdAt: now(),
          paidAt: null, confirmedAt: null, sentAt: null, canceledAt: null, refundedAt: null
        };
        db.data.orders.push(order);
        await db.write();
        audit(`NEW ${order.id} by ${sender} total=${total} expected=${expectedAmount}`);

        const itemsText = summarizeOrderItems(items);
        const caption = buildPaymentCaption(order, itemsText);

        try {
          if (!QRIS_PAYLOAD_BASE) {
            const sent = await send(jid, "⚠️ QRIS belum disetel. Hubungi admin.");
            order.paymentMsgKey = sent?.key || null;
          } else {
            // ==== QRIS DINAMIS (Tag 54 + Tag 62.01 + CRC) ====
            let payloadToUse = QRIS_PAYLOAD_BASE;
            if (QRIS_DYNAMIC) {
              try {
                payloadToUse = buildDynamicQRIS(QRIS_PAYLOAD_BASE, expectedAmount, order.id);
                order.qrisPayload = payloadToUse;
              } catch (e) {
                console.error("buildDynamicQRIS error:", e);
                payloadToUse = QRIS_PAYLOAD_BASE; // fallback
              }
            }
            const buf = await qrisPngBufferFromPayload(payloadToUse);
            const sent = await safeSendImage(jid, buf, caption);
            order.paymentMsgKey = sent?.key || null;
          }
          await db.write();
        } catch (e) {
          console.error("QRIS send error:", e);
          const sent = await send(jid, caption + "\n\n⚠️ Gagal memuat gambar QR. Minta admin kirim ulang QR.");
          order.paymentMsgKey = sent?.key || null;
          await db.write();
        }

        if (OWNER) {
          await send(
            OWNER + "@s.whatsapp.net",
            [
              "🛒 *Order baru masuk*",
              `ID: *${order.id}*`,
              `User: ${cleanNumber(sender)} (${user.name})`,
              "",
              "🛍️ Rincian:",
              itemsText,
              "—",
              `Total: ${formatPrice(total)}`,
              `*Tagihan (kode unik): ${formatPrice(expectedAmount)}*`,
              `🕒 ${formatTs(order.createdAt)}`,
              "",
              `⏳ Menunggu transfer user (auto-verifikasi).`
            ].join("\n")
          );
        }
        return;
      }

      // BAYAR (opsional)
      if (lower.startsWith("bayar ")) {
        const id = text.split(/\s+/)[1];
        const order = db.data.orders.find(o=>o.id===id && o.userJid===sender);
        if (!order) { await send(jid, "⚠️ ID order tidak ditemukan."); return; }
        if (!["NEW","AWAIT_CONFIRM"].includes(order.status)) { await send(jid, `⚠️ Order *${id}* sudah diproses (status: ${order.status}).`); return; }
        order.status="AWAIT_CONFIRM"; order.paidAt=now(); await db.write(); audit(`AWAIT_CONFIRM ${id} by ${sender}`);

        await send(
          jid,
          [
            "✅ Terima kasih, bukti pembayaran kamu sudah *kami terima*! 🙏",
            `ID Order: *${order.id}*`,
            `🕒 Waktu: ${formatTs(order.paidAt)}`,
            "",
            "Status: *Menunggu verifikasi admin*",
            "Catatan: kalau kamu sudah transfer nominal *persis* (termasuk kode unik), seharusnya diverifikasi otomatis.",
            "",
            "Kalau belum auto, admin akan cek manual sebentar lagi ya. 😊"
          ].join("\n")
        );

        if (OWNER) {
          await send(OWNER + "@s.whatsapp.net",
            [
              "💸 *Pembayaran masuk (manual ack)*",
              `ID: *${order.id}*`,
              `User: ${cleanNumber(sender)} (${user.name})`,
              `Tagihan: *${formatPrice(order.expectedAmount || order.total)}*`,
              `🕒 ${formatTs(order.paidAt)}`,
              "",
              `⚙️ Ketik: *konfirmasi ${order.id} ${cleanNumber(sender)}* bila valid.`
            ].join("\n")
          );
        }
        return;
      }

      // ===== ADMIN =====
      if (isAdmin) {
        if (lower.startsWith("konfirmasi ")) {
          const [, id, numRaw] = text.split(/\s+/);
          const num = cleanNumber(numRaw || ""); if (!id || !num){ await send(jid,"Format: konfirmasi <idorder> <nomorUser>"); return; }
          const uj = num + "@s.whatsapp.net";
          const order = db.data.orders.find(o=>o.id===id && o.userJid===uj);
          if (!order) { await send(jid, "⚠️ Order tidak ditemukan atau nomor tidak cocok."); return; }
          if (!["AWAIT_CONFIRM","NEW"].includes(order.status)) { await send(jid, `⚠️ Order ${id} tidak dalam status menunggu (status: ${order.status}).`); return; }
          order.status="PAID"; order.confirmedAt=now(); order.paidAt ||= order.confirmedAt; await db.write(); audit(`PAID ${id} by admin ${sender}`);
          await send(jid, `✅ Order ${id} ditandai *PAID*. Lanjut *send ${id} ${num} ...*`);
          await send(uj, ["✅ Pembayaran kamu *sudah diverifikasi*!",`ID Order: *${id}*`,`🕒 ${formatTs(order.confirmedAt)}`,"","Produk akan segera kami proses dan kirim. Terima kasih sudah berbelanja di kami 🙏"].join("\n")); 
          return;
        }

        if (lower.startsWith("batal ")) {
          const parts = text.split(/\s+/);
          const id = parts[1];
          const num = cleanNumber(parts[2] || "");
          const reason = text.split(/\s+/).slice(3).join(" ").trim() || "Tanpa keterangan";
          if (!id || !num) { await send(jid, "Format: batal <idorder> <nomorUser> [alasan]"); return; }
          const uj = num + "@s.whatsapp.net";
          const order = db.data.orders.find(o=>o.id===id && o.userJid===uj);
          if (!order) { await send(jid, "⚠️ Order tidak ditemukan atau nomor tidak cocok."); return; }
          if (["SENT","REFUNDED","CANCELED"].includes(order.status)) { await send(jid, `⚠️ Order ${id} tidak bisa dibatalkan (status: ${order.status}).`); return; }
          order.status = "CANCELED";
          order.canceledAt = now();
          await db.write(); audit(`CANCELED ${id} by admin ${sender} reason=${reason}`);
          await send(jid, `⛔ Order ${id} *dibatalkan*.`);
          await send(uj, [`⛔ *Pesanan dibatalkan*`,`ID: *${id}*`,`Alasan: ${reason}`,`🕒 ${formatTs(order.canceledAt)}`,"","Jika butuh bantuan, silakan hubungi CS:", CS_CONTACT].join("\n"));
          return;
        }

        if (lower.startsWith("refund ")) {
          const parts = text.split(/\s+/);
          const id = parts[1];
          const num = cleanNumber(parts[2] || "");
          const reason = text.split(/\s+/).slice(3).join(" ").trim() || "Tanpa keterangan";
          if (!id || !num) { await send(jid, "Format: refund <idorder> <nomorUser> [alasan]"); return; }
          const uj = num + "@s.whatsapp.net";
          const order = db.data.orders.find(o=>o.id===id && o.userJid===uj);
          if (!order) { await send(jid, "⚠️ Order tidak ditemukan atau nomor tidak cocok."); return; }
          if (order.status === "REFUNDED") { await send(jid, `⚠️ Order ${id} sudah berstatus REFUNDED.`); return; }
          if (["PAID","AWAIT_CONFIRM","NEW","CANCELED"].includes(order.status)) {
            for (const it of order.items){ const p=productById(it.id); if(p){ p.stock=Number(p.stock||0)+it.qty; p.sold=Math.max(0, Number(p.sold||0)-it.qty); } }
            saveProducts();
          }
          order.status = "REFUNDED";
          order.refundedAt = now();
          await db.write(); audit(`REFUNDED ${id} by admin ${sender} reason=${reason}`);
          await send(jid, `💵 Order ${id} ditandai *REFUNDED*.`);
          await send(uj, [`💵 *Refund diproses/selesai*`,`ID: *${id}*`,`Alasan: ${reason}`,`🕒 ${formatTs(order.refundedAt)}`,"","Jika butuh bantuan, silakan hubungi CS:", CS_CONTACT].join("\n"));
          return;
        }

        if (lower.startsWith("send ")) {
          const tok = text.split(/\s+/);
          const id = tok[1]; const num = cleanNumber(tok[2] || "");
          const restIdx = text.indexOf(num) + String(num).length;
          const kvRaw = text.slice(restIdx + 1).trim();
          if (!id || !num || !kvRaw){ await send(jid,"Format: send <idorder> <nomorUser> email:...|password:...|other:...|note:...|rules:..."); return; }
          const uj = num + "@s.whatsapp.net";
          const order = db.data.orders.find(o=>o.id===id && o.userJid===uj);
          if (!order){ await send(jid,"⚠️ Order tidak ditemukan atau nomor tidak cocok."); return; }
          if (order.status!=="PAID"){ await send(jid,`⚠️ Order ${id} belum *PAID*. Konfirmasi dulu.`); return; }

          const fields={}; kvRaw.split("|").forEach(seg=>{ const s=seg.trim(); if(!s) return; const i=s.indexOf(":"); if(i===-1) return; const k=s.slice(0,i).trim().toLowerCase(); const v=s.slice(i+1).trim(); if(!fields[k]) fields[k]=clampText(v,500); });
          const rules = fields["rules"]; delete fields["rules"]; const note = fields["note"]||fields["notes"]; delete fields["note"]; delete fields["notes"];
          const keys=Object.keys(fields).sort((a,b)=>{ const pr={email:1,password:2,user:3,username:4}; return (pr[a]||9)-(pr[b]||9); });
          const detailLines = keys.map(k=>`${capitalize(k)}: ${fields[k]}`);

          const msg1 = [
            "🎉 *Pesanan kamu sudah terkirim!*",
            "",
            "Berikut detail akun yang bisa langsung dipakai:",
            "",
            "*🔑 Detail Akun*",
            detailLines.length ? detailLines.join("\n") : "-",
            "",
            "*🗒️ Catatan*",
            note ? ("- " + note) : "-",
            "",
            `🕒 ${formatTs(now())}`,
            "",
            "Jika ada kendala login atau butuh bantuan, balas chat ini ya. Kami siap bantu. 🙌"
          ].join("\n");
          await send(uj, msg1);
          if (rules) await send(uj, `*📏 Rules:*\n${rules}`);

          for (const it of order.items){ const p=productById(it.id); if(p){ p.stock=Math.max(0,Number(p.stock||0)-it.qty); p.sold=Number(p.sold||0)+it.qty; } } saveProducts();
          order.status="SENT"; order.sentAt=now(); await db.write(); audit(`SENT ${id} by admin ${sender}`);
          await send(jid, `✅ Produk untuk ${id} sudah dikirim ke ${num}. 🕒 ${formatTs(order.sentAt)}`); return;
        }

        if (lower === "/orders") {
          const last10 = [...db.data.orders].slice(-10).reverse();
          if (!last10.length){ await send(jid,"Belum ada order."); return; }
          await send(jid, last10.map(o=>`${o.id} — ${cleanNumber(o.userJid)} — ${o.status} — ${formatPrice(o.total)} — ${formatTs(o.createdAt)}`).join("\n")); return;
        }

        // CRUD
        if (lower.startsWith("addprod ")) {
          const raw=text.slice(8).trim();
          const [id,title,category,price,discount,stock,sold,descRaw]=raw.split("|").map(s=>s?.trim());
          if(!id||!title||!category||!price){ await send(jid,"Format: addprod id|title|category|price|discount|stock|sold|desc1;desc2"); return; }
          if (productById(id)) { await send(jid,"⚠️ ID produk sudah ada."); return; }
          const prod={ id:id.replace(/\s/g,""), title: clampText(title,100), category: clampText(category,50), price:Number(price)||0, discount:Number(discount)||0, stock:Number(stock)||0, sold:Number(sold)||0, descLines:(descRaw||"").split(";").map(s=>clampText(s.trim(),120)).filter(Boolean)};
          PRODUCTS.push(prod); saveProducts(); audit(`ADDPROD ${prod.id} by ${sender}`);
          await send(jid, `✅ Produk ${prod.id} ditambahkan.\n${prod.title} — ${formatPrice(finalPrice(prod.price, prod.discount))}\nStok: ${prod.stock} | Sold: ${prod.sold}`); return;
        }
        if (lower.startsWith("prod ")) {
          const id = text.split(/\s+/)[1]; const p=productById(id);
          if(!p){ await send(jid,"Produk tidak ditemukan."); return; }
          const price=finalPrice(p.price,p.discount); const desc=(p.descLines||[]).map(d=>`- ${d}`).join("\n")||"-";
          await send(
            jid,
            [
              `*${p.title}*`,
              `ID: *${p.id}*`,
              `Kategori: *${p.category}*`,
              `Harga: *${formatPrice(price)}*${p.discount?` (diskon *${p.discount}%*)`:""}`,
              `Stok: *${p.stock}* | Terjual: *${p.sold||0}*`,
              "Deskripsi:",
              desc
            ].join("\n")
          );
          return;
        }
        if (lower.startsWith("prods")) {
          const page = Math.max(1, Number(text.split(/\s+/)[1]||"1")||1); const per=10;
          const list=PRODUCTS.slice((page-1)*per, page*per);
          if(!list.length){ await send(jid,"Tidak ada data di halaman ini."); return; }
          const lines = list.map(p => `• [${p.id}] ${p.title} — *${formatPrice(finalPrice(p.price,p.discount))}* — stok *${p.stock}*`);
          await send(jid, `Halaman ${page}\n${lines.join("\n")}`);
          return;
        }
        if (lower.startsWith("search ")) {
          const q=text.slice(7).toLowerCase();
          const res=PRODUCTS.filter(p=>p.id.toLowerCase().includes(q)||p.title.toLowerCase().includes(q)||p.category.toLowerCase().includes(q)).slice(0,20);
          if(!res.length){ await send(jid,"Tidak ada hasil."); return; }
          await send(jid, res.map(p=>`• [${p.id}] ${p.title} — ${formatPrice(finalPrice(p.price,p.discount))} — stok ${p.stock}`).join("\n")); return;
        }
        if (lower.startsWith("setharga ")) { const raw=text.slice(9).trim(); const [id,price,discount]=raw.split("|").map(s=>s?.trim()); const p=productById(id); if(!p){ await send(jid,"Produk tidak ditemukan."); return; } if(price) p.price=Number(price)||p.price; if(discount!==undefined) p.discount=Number(discount)||0; saveProducts(); audit(`SETHARGA ${id} by ${sender}`); await send(jid, `✅ Harga ${id}: ${formatPrice(finalPrice(p.price,p.discount))} (disc ${p.discount}%)`); return; }
        if (lower.startsWith("setstok ")) { const raw=text.slice(8).trim(); const [id,qty]=raw.split("|").map(s=>s?.trim()); const p=productById(id); if(!p){ await send(jid,"Produk tidak ditemukan."); return; } p.stock=Number(qty)||p.stock; saveProducts(); audit(`SETSTOK ${id} by ${sender}`); await send(jid, `✅ Stok ${id}: ${p.stock}`); return; }
        if (lower.startsWith("settitle ")) { const raw=text.slice(9).trim(); const [id,title]=raw.split("|").map(s=>s?.trim()); const p=productById(id); if(!p){ await send(jid,"Produk tidak ditemukan."); return; } p.title=clampText(title,100)||p.title; saveProducts(); audit(`SETTITLE ${id} by ${sender}`); await send(jid, `✅ Title ${id} diubah.`); return; }
        if (lower.startsWith("setcat ")) { const raw=text.slice(7).trim(); const [id,cat]=raw.split("|").map(s=>s?.trim()); const p=productById(id); if(!p){ await send(jid,"Produk tidak ditemukan."); return; } p.category=clampText(cat,50)||p.category; saveProducts(); audit(`SETCAT ${id} by ${sender}`); await send(jid, `✅ Category ${id} diubah.`); return; }
        if (lower.startsWith("setdesc ")) { const raw=text.slice(8).trim(); const [id,descRaw]=raw.split("|"); const p=productById(id); if(!p){ await send(jid,"Produk tidak ditemukan."); return; } p.descLines=(descRaw||"").split(";").map(s=>clampText(s.trim(),120)).filter(Boolean); saveProducts(); audit(`SETDESC ${id} by ${sender}`); await send(jid, `✅ Deskripsi ${id} diubah (${p.descLines.length} baris).`); return; }
        if (lower.startsWith("delprod ")) { const id=text.split(/\s+/)[1]; const p=productById(id); if(!p){ await send(jid,"Produk tidak ditemukan."); return; } db.data._pendingDel={id,by:sender,at:Date.now()}; await db.write(); await send(jid, `⚠️ Konfirmasi hapus: ketik *ya ${id}* dalam 30 detik untuk menghapus.`); return; }
        if (lower.startsWith("ya ")) { const id=text.split(/\s+/)[1]; const pend=db.data._pendingDel; if(!pend||pend.id!==id||pend.by!==sender||(Date.now()-pend.at)>30000){ await send(jid,"⚠️ Tidak ada penghapusan tertunda / waktu habis."); return; } delete db.data._pendingDel; const idx=PRODUCTS.findIndex(p=>p.id.toLowerCase()===id.toLowerCase()); if(idx===-1){ await send(jid,"Produk tidak ditemukan."); return; } const removed=PRODUCTS.splice(idx,1)[0]; saveProducts(); await db.write(); audit(`DELPROD ${id} by ${sender}`); await send(jid, `✅ Produk ${removed.id} dihapus.`); return; }

        if (lower.startsWith("bc all ")) {
          const msg = clampText(text.slice(7).trim(), 4000);
          const userJids = Object.keys(db.data.users || {});
          let sent=0, fail=0;
          for (const uj of userJids){ try{ await safeSendText(uj, msg); sent++; await new Promise(r=>setTimeout(r,900)); }catch{ fail++; } }
          await send(jid, `📣 Broadcast selesai. Terkirim: ${sent}, Gagal: ${fail}`); audit(`BC_ALL by ${sender} sent=${sent} fail=${fail}`); return;
        }
      }

      // Fallback help
      await send(jid, userHelp());
    }catch(err){ console.error("handleMessage error:", err); }
  }

  sock.ev.on("messages.upsert", handleMessage);
}

startSock().catch(err => console.error("fatal start error:", err));
