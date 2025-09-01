// src/store-bot.js  (boleh juga simpan sebagai src/testpairing.js)
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
const os = require("os");
const { Low } = require("lowdb");
const { JSONFile } = require("lowdb/node");
const { customAlphabet } = require("nanoid");
const QRCode = require("qrcode");

// ===== ENV =====
const OWNER = (process.env.OWNER || "").replace(/[^0-9]/g, "");
const BOT_NUMBER = (process.env.BOT_NUMBER || "").replace(/[^0-9]/g, "");
const USE_PAIRING = process.env.PAIRING === "1" || process.env.PAIRING === "true";
const STORE_NAME = process.env.STORE_NAME || "Vienze Store";
const QRIS_PAYLOAD_BASE = process.env.QRIS_PAYLOAD || "";
const QRIS_DYNAMIC = process.env.QRIS_DYNAMIC === "1" || process.env.QRIS_DYNAMIC === "true";
const EXPIRE_HOURS = Math.max(0, Number(process.env.EXPIRE_HOURS || "1"));
const RATE_LIMIT_MS = Math.max(0, Number(process.env.RATE_LIMIT_MS || "1200"));
const MAX_MSG_CHARS = Math.max(50, Number(process.env.MAX_MSG_CHARS || "500"));
const AUDIT_LOG = process.env.AUDIT_LOG === "1" || process.env.AUDIT_LOG === "true";
const WEBHOOK_PORT = Number(process.env.WEBHOOK_PORT || "3001");
const CS_CONTACT = process.env.CS_CONTACT || "Hubungi admin: wa.me/62XXXXXXXXXXX";
const AUTO_SEND = process.env.AUTO_SEND === "1" || process.env.AUTO_SEND === "true";

console.log(
  `[BOOT] PAIRING=${USE_PAIRING} | BOT_NUMBER=${BOT_NUMBER || "(none)"} | OWNER=${OWNER || "(none)"} | PORT=${WEBHOOK_PORT} | AUTO_SEND=${AUTO_SEND}`
);

// ===== PATHS =====
const root = process.cwd();
const dataDir = path.join(root, "data");
const logsDir = path.join(root, "logs");
const backupsDir = path.join(root, "backups");
const stockDir = path.join(dataDir, "stock");
for (const d of [dataDir, logsDir, backupsDir, stockDir]) if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });

// ===== DB =====
const db = new Low(new JSONFile(path.join(dataDir, "db.json")), {
  users: {},
  orders: [],
  settings: { storeOpen: true, autoCloseWhenAllZero: true }
});
const productsPath = path.join(root, "products.json");
function loadProducts() { if (!fs.existsSync(productsPath)) fs.writeFileSync(productsPath, "[]"); return JSON.parse(fs.readFileSync(productsPath, "utf8")); }
let PRODUCTS = loadProducts();
function saveProducts() { fs.writeFileSync(productsPath, JSON.stringify(PRODUCTS, null, 2)); backupSnapshot(); evalAutoClose(); }

// ===== UTIL =====
const nanoID = customAlphabet("ABCDEFGHJKLMNPQRSTUVWXYZ23456789", 6);
const formatPrice = n => "Rp " + Number(n || 0).toLocaleString("id-ID");
const pct = x => Math.max(0, Math.min(100, Number(x || 0)));
const finalPrice = (p, d) => Math.round((Number(p || 0) * (100 - pct(d))) / 100);
const cleanNumber = n => String(n || "").replace(/[^0-9]/g, "");
const isAdminJid = jid => OWNER && cleanNumber(jid) === OWNER;
const capitalize = s => (s || "").charAt(0).toUpperCase() + (s || "").slice(1);
const now = () => Date.now();
const todayStr = () => new Date().toISOString().slice(0, 10);
const clampText = (s, max = 4000) => { s = String(s || ""); return s.length > max ? s.slice(0, max) + "…" : s; };
const stokLabel = n => Number(n || 0) === 0 ? "kosong" : String(n);
const DIV = "——————————";

const audit = line => {
  if (!AUDIT_LOG) return;
  const p = path.join(logsDir, `audit-${todayStr()}.log`);
  fs.appendFileSync(p, `[${new Date().toISOString()}] ${line}\n`);
};
function backupSnapshot() {
  const d = todayStr(), dir = path.join(backupsDir, d);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  try {
    fs.copyFileSync(productsPath, path.join(dir, "products.json"));
    const dbPath = path.join(dataDir, "db.json");
    if (fs.existsSync(dbPath)) fs.copyFileSync(dbPath, path.join(dir, "db.json"));
  } catch {}
}

// WIB time
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

// ===== ensure DB shape =====
function ensureUserShape(u) {
  if (!u) return;
  u.name ||= "";
  u.joinedAt ||= now();
  u.stats ||= { orders: 0, paid: 0, sent: 0, refunded: 0, canceled: 0, totalSpent: 0 };
  u.productCount ||= {};
  u.banned = !!u.banned;
  u.lastTs ||= 0;
}
async function ensureDB() {
  await db.read();
  db.data ||= {};
  db.data.users ||= {};
  db.data.orders ||= [];
  db.data.settings ||= { storeOpen: true, autoCloseWhenAllZero: true };
  for (const id of Object.keys(db.data.users)) ensureUserShape(db.data.users[id]);
  await db.write();
}
function getUser(jid) {
  const id = jidNormalizedUser(jid);
  db.data.users[id] ||= { name: "", banned: false, lastTs: 0, joinedAt: now(), stats: { orders:0, paid:0, sent:0, refunded:0, canceled:0, totalSpent:0 }, productCount: {} };
  ensureUserShape(db.data.users[id]);
  return db.data.users[id];
}
const productById = id => PRODUCTS.find(p => p.id.toLowerCase() === String(id).toLowerCase());
const makeOrderId = () => "VS-" + nanoID();

// ===== status emoji =====
function statusEmoji(s) {
  return {
    NEW: "🆕", AWAIT_CONFIRM: "⏳", PAID: "✅", SENT: "📦",
    REFUNDED: "💵", CANCELED: "⛔", EXPIRED: "⌛"
  }[s] || "•";
}

// ===== QRIS utils =====
function stripCRC(p) { return p.replace(/63(04)[0-9A-F]{4}$/i, ""); }
function findTag(p, tag) { const r = new RegExp(`(${tag})([0-9]{2})`, "g"); let m; while ((m = r.exec(p)) !== null) { const idx = m.index, len = parseInt(p.substr(idx + 2, 2), 10); const valStart = idx + 4, valEnd = valStart + len; return { start: idx, end: valEnd, len, val: p.substring(valStart, valEnd) }; } return null; }
function setTag(p, tag, valueRaw) { const val = String(valueRaw), len = String(val.length).padStart(2, "0"); const chunk = `${tag}${len}${val}`, hit = findTag(p, tag); if (hit) return p.substring(0, hit.start) + chunk + p.substring(hit.end); const without = stripCRC(p); return without + chunk; }
function setTag62BillNumber(p, billNo) { const subVal = `01${String(billNo.length).padStart(2, "0")}${billNo}`; return setTag(p, "62", subVal); }
function crc16ccitt(hexStr) { let crc = 0xffff; for (let i = 0; i < hexStr.length; i++) { crc ^= (hexStr.charCodeAt(i) & 0xff) << 8; for (let j = 0; j < 8; j++) crc = (crc & 0x8000) ? ((crc << 1) ^ 0x1021) : (crc << 1), crc &= 0xffff; } return crc.toString(16).toUpperCase().padStart(4, "0"); }
function buildDynamicQRIS(basePayload, amountIDR, orderId) { if (!basePayload) throw new Error("QRIS base payload kosong"); const amt = (Number(amountIDR) || 0).toFixed(2); let p = stripCRC(basePayload); p = setTag(p, "54", amt); p = setTag62BillNumber(p, orderId); const preCRC = p + "6304"; return preCRC + crc16ccitt(preCRC); }
async function qrisPngBufferFromPayload(payload) { return await QRCode.toBuffer(payload, { type: "png", errorCorrectionLevel: "M", margin: 2, width: 512 }); }

// ===== MENU / HELP =====
function prettyMenu() {
  const header = [
    `*🛍️  ${STORE_NAME}*`,
    "Tempat belanja akun & layanan digital ✨",
    "",
    "💡 *Cara order:*",
    "  1) Lihat *kode produk* di bawah",
    "  2) Ketik: *beli <kode> <jumlah>*  (contoh: *beli net1u 1*)",
    "  3) Kamu dapat *ID Order* + QRIS",
    "  4) Transfer — verifikasi *otomatis* ✅",
    "  5) Admin kirim produk / auto-kirim (bila aktif)",
    ""
  ].join("\n");
  const blocks = PRODUCTS.map((p, idx) => {
    const price = finalPrice(p.price, p.discount);
    const desc = (p.descLines || []).map(d => "   - " + d).join("\n") || "   -";
    return [
      `📦 *${p.title}*`,
      `   • 💸 *Harga:* ${formatPrice(price)}${p.discount ? ` (diskon *${p.discount}%*)` : ""}`,
      `   • 🏷️ *Kode:* *${p.id}*`,
      `   • 📦 *Stok:* *${stokLabel(p.stock)}*   • 🔥 *Terjual:* *${p.sold || 0}*`,
      `   • 📝 *Deskripsi:*`,
      desc,
      idx === PRODUCTS.length - 1 ? "" : DIV
    ].join("\n");
  });
  return `${header}\n${blocks.join("\n")}\n\n✍️ _Ketik: *beli <kode> <jumlah>*_`;
}
function userHelpFull() {
  return [
    "*📚 Panduan Lengkap*",
    "",
    "🧾 *menu* — lihat daftar produk + harga & stok",
    "🛒 *beli <kode> <qty>* — buat order (contoh: *beli net1u 1*)",
    "💳 *bayar <ID>* — beri tahu kami kalau sudah transfer (manual ack)",
    "🔎 *cekorder <ID>* — lihat detail order + riwayat pengiriman akun",
    "📂 *orders* — riwayat 10 order terakhir kamu",
    "👤 *myinfo* — profil & statistik belanja kamu",
    "👨‍💻 *cs* — kontak support",
    "",
    "ℹ️ *Catatan penting:*",
    "• Transfer *nominal persis* (sudah termasuk kode unik) supaya auto-verify ✅",
    "• Order kedaluwarsa otomatis setelah batas waktu (lihat di invoice pembayaran)",
    "• Jika akun sudah terkirim, detailnya akan tampil di *cekorder*",
  ].join("\n");
}
function userHelpShort() {
  return [
    "❓ *Perintah tidak dikenali.*",
    "",
    "📚 *Panduan Singkat*",
    "• 🧾 *menu* — lihat daftar produk",
    "• 🛒 *beli <kode> <jumlah>* — contoh: *beli net1u 1*",
    "• 💳 *bayar <idorder>* — opsional (manual ack)",
    "• 🔎 *cekorder <id>* — detail order",
    "• 📂 *orders* — riwayat 10 order",
    "• 👤 *myinfo* — profil & statistik",
    "• 👨‍💻 *cs* — kontak support",
    "",
    "Tips: *transfer nominal persis* biar auto-verify ✅"
  ].join("\n");
}
function adminHelp() {
  return [
    "*🛠️ Admin Help*",
    "",
    "🔐 *Order Ops*",
    "• ✅ *konfirmasi <id> <nomor>* — tandai PAID & hapus pesan QR",
    "• 📦 *send <id> <nomor> <payload>* — kirim manual (multi akun pakai '||')",
    "• ⛔ *batal <id> <nomor> [alasan]* — batalkan (stok balik)",
    "• 💵 *refund <id> <nomor> [alasan]* — tandai REFUNDED",
    "• 🧾 */orders* — list order terbaru",
    "• 🧾 */orders <status|all> [n]* — filter (contoh: */orders sent 20*)",
    "• 🧾 */orders user <nomor> [n]* — order per user",
    "• 🧾 */order <id>* — detail order (admin)",
    "• 📊 *report <today|month|all>* — rekap omzet & profit",
    "• 📊 *report range <YYYY-MM-DD> <YYYY-MM-DD>*",
    "• 📊 *monitor* — status VPS & bot",
    "• 🟢 *open* / 🔴 *close* — buka/tutup toko",
    "",
    "📦 *Produk & Stok*",
    "• *addprod id|title|category|price|discount|stock|sold|desc1;desc2*",
    "• *setharga id|price|discount*",
    "• *setstok id|qty*   (boleh 'kosong'=0)",
    "• *settitle id|title*   • *setcat id|category*",
    "• *setdesc id|desc1;desc2;...*",
    "• *setrules id|rules default*   • *setnotes id|notes default*",
    "• *setcost id|cost* — untuk hitung profit (opsional)",
    "• *delprod id* (konfirmasi: *ya id*)",
    "",
    "🗃️ *Pool Akun*",
    "• *addstock id <baris>* — pipe/keyed (contoh keyed: *email:a@b|password:pass|2fa:xxxx|notes:...|rules:...*)",
    "• *addstockmulti id <baris1>||<baris2>||...*",
    "• *stock id* — jumlah & sample mask",
    "• *stockall id* (alias: *cekallstock id*) — tampilkan seluruh data akun",
    "• *syncstock id* / *syncstock all* — set *product.stock = jumlah pool*",
    "• *sendstock id|nomor|jumlah* — kirim manual dari pool tanpa order",
    "",
    `⚙️ *AUTO_SEND=${AUTO_SEND ? "ON" : "OFF"}* — atur via .env`
  ].join("\n");
}

// ===== summaries =====
function summarizeOrderItems(items) {
  return items.map(i => {
    const p = productById(i.id); const sub = finalPrice(p.price, p.discount) * i.qty;
    return `• *${p?.title || i.id}* x${i.qty} = ${formatPrice(sub)}`;
  }).join("\n");
}
function summarizeItemsOneLine(items) {
  return items.map(i => { const p = productById(i.id); return `${p?.title || i.id} x${i.qty}`; }).join(", ");
}

// ===== auto close when all stock zero =====
function evalAutoClose() {
  try {
    const on = db.data.settings?.autoCloseWhenAllZero;
    if (!on) return;
    const anyStock = PRODUCTS.some(p => Number(p.stock || 0) > 0);
    if (!anyStock) {
      if (db.data.settings.storeOpen) {
        db.data.settings.storeOpen = false;
        db.write().catch(()=>{});
        console.log("🔴 Auto-close: semua stok 0 → toko ditutup.");
      }
    }
  } catch {}
}

// ===== stock numeric lock =====
function lockStockForOrder(items) {
  for (const it of items) {
    const p = productById(it.id);
    if (!p || Number(p.stock || 0) < Number(it.qty || 0)) return false;
  }
  for (const it of items) {
    const p = productById(it.id);
    p.stock = Number(p.stock || 0) - Number(it.qty || 0);
  }
  saveProducts();
  return true;
}
function releaseStock(order) {
  if (!order || order._stockReleased) return;
  for (const it of order.items || []) {
    const p = productById(it.id);
    if (p) p.stock = Number(p.stock || 0) + Number(it.qty || 0);
  }
  order._stockReleased = true;
  saveProducts();
}

// ===== Pool akun =====
const FIELD_ORDER = ["email","password","pin","profil","durasi","ops1","ops2","ops3","notes","rules"];
const LABELS = {
  email:"Email", user:"Email", username:"Email",
  password:"Password", pass:"Password",
  pin:"PIN",
  profil:"Profil", profile:"Profil",
  durasi:"Durasi", validity:"Durasi",
  ops1:"Opsional 1", ops2:"Opsional 2", ops3:"Opsional 3",
  notes:"Catatan", note:"Catatan",
  rules:"Rules",
  "2fa":"2FA", keypass:"Keypass"
};
const NORMALIZE = { user:"email", username:"email", pass:"password", profile:"profil", validity:"durasi", note:"notes" };

function stockFile(id) { return path.join(stockDir, `${id.toLowerCase()}.json`); }
function readStock(id) { const f = stockFile(id); if (!fs.existsSync(f)) fs.writeFileSync(f, "[]"); try { return JSON.parse(fs.readFileSync(f, "utf8")); } catch { return []; } }
function writeStock(id, arr) { fs.writeFileSync(stockFile(id), JSON.stringify(arr, null, 2)); }

function normalizeFields(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj || {})) {
    const kk = (NORMALIZE[k] || k).toLowerCase();
    if (out[kk] == null && String(v || "").trim()) out[kk] = String(v).trim();
  }
  return out;
}
// parse keyed (email:x|password:y|...) atau pipe (a@b|pass|pin|profil|durasi|ops1|ops2|ops3|notes|rules)
function parseStockPayload(payload) {
  const s = String(payload || "").trim();
  if (!s) return null;
  if (s.includes(":")) {
    // keyed — jangan buang newline; tiap seg dipisah '|'
    const obj = {};
    s.split("|").forEach(seg => {
      const t = seg.replace(/\r/g,"").trim();
      if (!t) return;
      const i = t.indexOf(":");
      if (i === -1) return;
      const k = t.slice(0,i).trim().toLowerCase();
      const v = t.slice(i+1).trim();
      if (k) obj[k] = v;
    });
    return normalizeFields(obj);
  } else {
    const parts = s.split("|").map(x => x.trim());
    const obj = {};
    for (let i = 0; i < Math.min(parts.length, FIELD_ORDER.length); i++) if (parts[i]) obj[FIELD_ORDER[i]] = parts[i];
    return normalizeFields(obj);
  }
}
function stockCount(id) { return readStock(id).length; }
function addStock(id, payload) {
  const obj = parseStockPayload(payload);
  if (!obj || !obj.email || !obj.password) return { ok:false, err:"Minimal butuh email & password" };
  const list = readStock(id); list.push(obj); writeStock(id, list);
  return { ok:true, left:list.length };
}
function addStockMulti(id, multi) {
  const rows = String(multi||"").split("||").map(s => s.trim()).filter(Boolean);
  let ok=0, fail=0; for (const r of rows) { const res = addStock(id, r); if (res.ok) ok++; else fail++; }
  return { ok, fail, left: stockCount(id) };
}
function takeStock(id, qty) {
  const list = readStock(id); if (list.length < qty) return null;
  const picked = list.splice(0, qty); writeStock(id, list); return picked;
}
function maskEmail(e) {
  const [u,d] = String(e||"").split("@"); if (!d) return e;
  const u2 = u.length <= 2 ? u[0] + "*" : u[0] + "*".repeat(Math.max(1,u.length-2)) + u.slice(-1);
  return `${u2}@${d}`;
}

// blok akun — TIDAK menduplikasi notes/rules (ditaruh di section khusus)
function formatAccountBlock(fields, idx, prod) {
  const sk = new Set(Object.keys(fields || {}));
  const keys = Array.from(sk);
  const exclude = new Set(["notes","rules"]);
  const priority = ["email","password","pin","profil","durasi","ops1","ops2","ops3","2fa","keypass"];
  keys.sort((a,b)=> (priority.indexOf(a)>=0?priority.indexOf(a):999) - (priority.indexOf(b)>=0?priority.indexOf(b):999));
  const lines = keys.filter(k => fields[k] && !exclude.has(k)).map(k => `• *${LABELS[k] || capitalize(k)}:* ${fields[k]}`);

  // show notes/rules hanya sekali — kalau tidak ada pada fields, pakai default product
  const notesToShow = fields.notes != null ? fields.notes : (prod?.defaultNotes || "");
  const rulesToShow = fields.rules != null ? fields.rules : (prod?.defaultRules || "");

  return [
    `*🔑 Detail Akun #${idx}*`,
    lines.length ? lines.join("\n") : "-",
    notesToShow ? `\n*🗒️ Catatan:*\n${notesToShow}` : "",
    rulesToShow ? `\n*📏 Rules:*\n${rulesToShow}` : ""
  ].join("\n");
}

// ===== captions =====
function buildPaymentCaption(order, itemsText) {
  const base = formatPrice(order.total), expect = formatPrice(order.expectedAmount);
  const deadline = EXPIRE_HOURS > 0 ? `⏱️ Batas waktu: ${EXPIRE_HOURS} jam (otomatis batal jika lewat)` : null;
  return [
    "*💳 Pembayaran via QRIS*",
    "",
    `*ID Order:* ${order.id}`,
    `*Dibuat:* ${formatTs(order.createdAt)}`,
    "",
    "🛍️ *Item:*",
    itemsText,
    "—",
    `*Total (sebelum kode unik):* ${base}`,
    `*Total yang harus dibayar:* ${expect}`,
    "_(termasuk kode unik 3 digit untuk verifikasi otomatis)_",
    "",
    "📌 *Cara bayar:*",
    "1) Scan QR di atas (QRIS)",
    "2) *Tidak perlu isi nominal* (QR sudah berisi jumlah yang benar)",
    "3) Selesaikan pembayaran — verifikasi otomatis ✅",
    deadline ? "\n" + deadline : ""
  ].join("\n").trim();
}

// ===== rate limit =====
function allowProcess(u) { const t = Date.now(); if (RATE_LIMIT_MS <= 0) { u.lastTs = t; return true; } if (t - (u.lastTs || 0) < RATE_LIMIT_MS) return false; u.lastTs = t; return true; }

// ===== user stats =====
function addUserStatsForOrderPaid(order) {
  const u = db.data.users[jidNormalizedUser(order.userJid)]; if (!u) return; ensureUserShape(u);
  u.stats.orders = (u.stats.orders||0) + 1;
  u.stats.paid = (u.stats.paid||0) + 1;
  u.stats.totalSpent = (u.stats.totalSpent||0) + Number(order.total||0);
  u.productCount ||= {}; for (const it of order.items) u.productCount[it.id] = (u.productCount[it.id]||0) + it.qty;
}
function addUserStatsForOrderSent(order) { const u = db.data.users[jidNormalizedUser(order.userJid)]; if (!u) return; ensureUserShape(u); u.stats.sent = (u.stats.sent||0) + 1; }
function addUserStatsForOrderRefunded(order) { const u = db.data.users[jidNormalizedUser(order.userJid)]; if (!u) return; ensureUserShape(u); u.stats.refunded = (u.stats.refunded||0) + 1; }
function addUserStatsForOrderCanceled(order) { const u = db.data.users[jidNormalizedUser(order.userJid)]; if (!u) return; ensureUserShape(u); u.stats.canceled = (u.stats.canceled||0) + 1; }

// ===== expiry =====
async function checkExpiry(sendFunc) {
  if (EXPIRE_HOURS <= 0) return;
  const limit = EXPIRE_HOURS * 3600 * 1000, nowTs = Date.now();
  let changed = false;
  for (const o of db.data.orders) {
    if (o.status === "NEW" && nowTs - o.createdAt > limit) {
      o.status = "EXPIRED"; releaseStock(o); changed = true;
      try { await sendFunc(o.userJid, `⏰ Order *${o.id}* sudah *expired*. Silakan buat order baru.\n🕒 ${formatTs(nowTs)}`); } catch {}
      audit(`EXPIRE ${o.id}`);
    }
  }
  if (changed) await db.write();
}

// ===== sock helpers =====
let currentSock = null;
function setCurrentSock(sock) { currentSock = sock; }
function getCurrentSock() { return currentSock; }
async function safeSendText(jid, text) { const sock = getCurrentSock(); if (!sock) throw new Error("Socket not ready"); return sock.sendMessage(jid, { text: clampText(text, 4000) }); }
async function safeDelete(jid, key) { const sock = getCurrentSock(); if (!sock) throw new Error("Socket not ready"); return sock.sendMessage(jid, { delete: key }); }
async function safeSendImage(jid, buffer, caption) { const sock = getCurrentSock(); if (!sock) throw new Error("Socket not ready"); return sock.sendMessage(jid, { image: buffer, caption: clampText(caption, 4000) }); }
async function sendLongText(jid, text) { const chunk = 3500; for (let i = 0; i < text.length; i += chunk) await safeSendText(jid, text.slice(i, i + chunk)); }
async function tryCleanupOldPaymentMessages() {
  const list = [...(db.data.orders || [])].slice(-60).reverse();
  for (const o of list) if (o.paymentMsgKey && ["PAID","SENT","CANCELED","REFUNDED","EXPIRED"].includes(o.status)) {
    try { await safeDelete(o.userJid, o.paymentMsgKey); o.paymentMsgKey = null; await db.write(); } catch {}
  }
}

// ===== webhook & monitor =====
let webhookStarted = false;
function countOrderStatus(list) { const acc = {}; for (const o of list) acc[o.status]=(acc[o.status]||0)+1; return acc; }
async function buildMonitorText() {
  try { await db.read(); } catch {}
  const mem = process.memoryUsage();
  const orders = db.data.orders || [];
  const stats = countOrderStatus(orders);
  const lines = [
    `*📊 Monitor — ${STORE_NAME}*`,
    `Uptime: ${Math.floor(process.uptime()/3600)}h ${Math.floor((process.uptime()%3600)/60)}m`,
    `Node: ${process.version}`,
    `CPU (1/5/15m): ${os.loadavg().map(n=>n.toFixed(2)).join(", ")}`,
    `Mem RSS: ${(mem.rss/1024/1024).toFixed(1)} MB | Heap: ${(mem.heapUsed/1024/1024).toFixed(1)} MB`,
    "",
    `Products: ${PRODUCTS.length}`,
    `Users: ${Object.keys(db.data.users || {}).length}`,
    `Orders: ${orders.length} | ${Object.entries(stats).map(([k,v])=>`${k}=${v}`).join(", ") || "-"}`,
    "",
    `Store: ${db.data.settings.storeOpen ? "OPEN" : "CLOSED"}`,
    `Waktu: ${formatTs(now())}`
  ];
  return lines.join("\n");
}
function startPaymentWebhook() {
  if (webhookStarted) return;
  webhookStarted = true;
  const app = express();
  app.use(express.urlencoded({ extended: false }));
  app.use(express.json());

  app.post("/api/transactions/pending", async (req, res) => {
    try {
      const body = req.body || {};
      const text = body.text || body.bigtext || body.title || "";
      const amount = (() => {
        if (!text) return null;
        const m = text.replace(/[\u00A0]/g, " ").match(/Rp\s*([\d\.]+)/i);
        if (!m) return null;
        const num = Number(m[1].replace(/\./g, ""));
        return Number.isFinite(num) ? num : null;
      })();
      if (!amount) return res.status(200).json({ ok: true, info: "no-amount" });

      const nowTs = Date.now(), ttl = 24 * 3600 * 1000;
      const order = (db.data.orders || []).find(
        o => (o.status === "NEW" || o.status === "AWAIT_CONFIRM") &&
             o.expectedAmount === amount &&
             nowTs - (o.createdAt || 0) < ttl
      );
      if (!order) return res.status(200).json({ ok: true, info: "no-order" });

      if (order.status !== "PAID") {
        order.status = "PAID";
        order.confirmedAt = now();
        order.paidAt = order.paidAt || now();
        await db.write();
        audit(`AUTO-PAID ${order.id} amount=${amount}`);
      }
      try { if (order.paymentMsgKey) { await safeDelete(order.userJid, order.paymentMsgKey); order.paymentMsgKey = null; await db.write(); } } catch {}

      try {
        await safeSendText(order.userJid, [
          "✅ *Pembayaran diterima!*",
          `*ID:* ${order.id}`,
          `*Item:* ${summarizeItemsOneLine(order.items)}`,
          `*Nominal:* ${formatPrice(order.expectedAmount)}`,
          `🕒 ${formatTs(order.paidAt)}`,
          "",
          AUTO_SEND ? "⏳ Pesanan akan dikirim otomatis segera..." : "Silakan tunggu admin memproses pesanan ya 🙏"
        ].join("\n"));
      } catch {}

      try {
        const u = getUser(order.userJid);
        addUserStatsForOrderPaid(order); await db.write();
        if (OWNER) {
          await safeSendText(OWNER + "@s.whatsapp.net", [
            "💸 *Auto-verify pembayaran diterima*",
            `*ID:* ${order.id}`,
            `*User:* ${cleanNumber(order.userJid)} (${u.name || "-"})`,
            `*Nominal:* ${formatPrice(order.expectedAmount)}`,
            `🕒 ${formatTs(order.paidAt)}`
          ].join("\n"));
        }
      } catch {}

      if (AUTO_SEND) { try { await maybeAutoSend(order); } catch (e) { console.error("autoSend error:", e); } }

      res.json({ ok: true, matched: order.id, amount });
    } catch (e) {
      console.error("webhook error:", e);
      res.status(200).json({ ok: false, info: "caught-error", error: String(e?.message || e) });
    }
  });

  app.get("/health", async (_req, res) => {
    try { await db.read(); } catch {}
    const mem = process.memoryUsage();
    const orders = db.data.orders || [];
    const stats = countOrderStatus(orders);
    res.json({
      ok: true, time: new Date().toISOString(), uptime_s: Math.round(process.uptime()), node: process.version,
      cpu_load: os.loadavg(), mem_rss: mem.rss, mem_heapUsed: mem.heapUsed,
      products: PRODUCTS.length, users: Object.keys(db.data.users || {}).length,
      orders_total: orders.length, orders_status: stats, auto_send: AUTO_SEND,
      store_open: !!db.data.settings.storeOpen
    });
  });

  app.get("/monitor", async (_req, res) => {
    const out = await buildMonitorText();
    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    res.end(out);
  });

  try {
    const server = app.listen(WEBHOOK_PORT, () => {
      console.log(`Server running di http://localhost:${WEBHOOK_PORT}`);
      console.log(`Endpoint: POST /api/transactions/pending | GET /health | GET /monitor`);
    });
    server.on("error", err => {
      if (err && err.code === "EADDRINUSE") console.log(`⚠️ Port ${WEBHOOK_PORT} sudah dipakai. Webhook tidak akan diduplikasi.`);
      else console.error("Webhook server error:", err);
    });
  } catch (err) {
    if (err && err.code === "EADDRINUSE") console.log(`⚠️ Port ${WEBHOOK_PORT} sudah dipakai. Webhook tidak akan diduplikasi.`);
    else console.error("Webhook start error:", err);
  }
}

// ===== AUTO SEND =====
async function maybeAutoSend(order) {
  if (!AUTO_SEND || !order || order.status !== "PAID") return;
  const bundles = [];
  for (const it of order.items) {
    const creds = takeStock(it.id, it.qty);
    if (!creds) {
      if (OWNER) { try { await safeSendText(OWNER + "@s.whatsapp.net", `⚠️ Auto-send *gagal* untuk *${order.id}* — stok akun *${it.id}* kurang. Perlu kirim manual.\nUser: ${cleanNumber(order.userJid)}\nButuh: ${it.qty}, Tersisa: ${stockCount(it.id)}`); } catch {} }
      try { await safeSendText(order.userJid, "⚠️ Stok akun sedang kosong. Admin akan mengirimkan pesanan kamu sesegera mungkin ya 🙏"); } catch {}
      return;
    }
    bundles.push({ prodId: it.id, creds });
  }

  order.delivered ||= [];
  let idx = 1;
  for (const b of bundles) {
    const prod = productById(b.prodId);
    for (const fields of b.creds) {
      const block = formatAccountBlock(fields, idx++, prod);
      const msg = [
        "🎉 *Pesanan kamu sudah terkirim!*",
        "",
        block,
        "",
        `*ID Order:* ${order.id}`,
        `*Produk:* ${summarizeItemsOneLine(order.items)}`,
        `🕒 ${formatTs(now())}`,
        "",
        "Jika ada kendala login atau butuh bantuan, balas chat ini ya. 🙌"
      ].join("\n");
      try { await safeSendText(order.userJid, msg); } catch {}
      order.delivered.push({ at: now(), prodId: b.prodId, fields });
    }
  }
  await db.write();

  for (const it of order.items) { const p = productById(it.id); if (p) p.sold = Number(p.sold || 0) + Number(it.qty || 0); }
  saveProducts();

  order.status = "SENT"; order.sentAt = now(); await db.write(); audit(`AUTO-SENT ${order.id}`);
  addUserStatsForOrderSent(order); await db.write();

  if (OWNER) { try { await safeSendText(OWNER + "@s.whatsapp.net", `✅ Auto-send selesai untuk *${order.id}* (${summarizeItemsOneLine(order.items)}) — ${formatTs(order.sentAt)}`); } catch {} }
}

// ===== BOOT =====
async function startSock() {
  await ensureDB();
  PRODUCTS = loadProducts();

  const { state, saveCreds } = await useMultiFileAuthState("./auth");
  const { version } = await fetchLatestBaileysVersion();
  let reconnectTimer = null;

  const sock = makeWASocket({
    version,
    logger: pino({ level: "silent" }),
    printQRInTerminal: false,
    browser: Browsers.ubuntu("Chrome"),
    auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, pino({ level: "silent" })) },
    markOnlineOnConnect: false
  });

  setCurrentSock(sock);

  async function requestPairingCodeWithRetry() {
    if (!USE_PAIRING || sock.authState.creds.registered) return;
    let n = (BOT_NUMBER || "").replace(/[^0-9]/g, "");
    if (!n) { console.log("❗ BOT_NUMBER belum valid. Contoh: 62xxxxxxxxxxx"); return; }
    if (n.startsWith("0")) n = "62" + n.slice(1);
    if (!n.startsWith("62")) n = "62" + n;
    for (let attempt = 1; attempt <= 5; attempt++) {
      try {
        const code = await sock.requestPairingCode(n);
        console.log(`\n👉 Pairing Code: ${code}\nBuka WhatsApp > Perangkat Tautan > Tautkan dengan nomor telepon\n`);
        break;
      } catch (e) {
        const msg = e?.output?.payload?.message || e.message || String(e);
        console.log(`Gagal pairing (attempt ${attempt}): ${msg}`);
        if (attempt === 5) console.log("❌ Coba: rm -rf auth && npm start (atau cek koneksi/nomor).");
        else await new Promise(r => setTimeout(r, 1000 * attempt));
      }
    }
  }
  requestPairingCodeWithRetry().catch(()=>{});

  function safeReconnect(delayMs = 1500) {
    if (reconnectTimer) return;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      console.log("🔁 Reconnecting…");
      startSock().catch(err => console.error("reconnect start error:", err));
    }, delayMs);
  }

  sock.ev.on("creds.update", async () => {
    try { await saveCreds(); }
    catch (e) { if (e && e.code === "ENOENT") { try { fs.mkdirSync("./auth", { recursive: true }); } catch {} ; await saveCreds(); } else console.error("saveCreds error:", e); }
  });

  sock.ev.on("connection.update", async u => {
    const { connection, lastDisconnect, qr, pairingCode } = u;
    if (pairingCode && USE_PAIRING) console.log(`\n👉 Pairing Code: ${pairingCode}\nBuka WhatsApp > Perangkat Tautan > Tautkan dengan nomor telepon\n`);
    if (qr && !USE_PAIRING) { console.log("\nScan QR ini dari WhatsApp:"); try { qrcodeTerminal.generate(qr, { small: true }); } catch {} }
    if (connection === "open") { console.log("✅ Connected."); setCurrentSock(sock); try { await tryCleanupOldPaymentMessages(); } catch {} }
    if (connection === "close") {
      const err = lastDisconnect?.error;
      const status = err?.output?.statusCode;
      const reason = err?.output?.payload?.message || err?.message || `Code ${status || "unknown"}`;
      console.log("❌ Disconnected:", reason);
      const isLoggedOut = Number(status) === 401;
      if (isLoggedOut) { console.log("🚪 Session logged out / belum tertaut. Reset auth & reconnect…"); try { fs.rmSync("./auth", { recursive: true, force: true }); fs.mkdirSync("./auth", { recursive: true }); } catch {} ; safeReconnect(1200); return; }
      if (USE_PAIRING && !sock.authState.creds.registered) setTimeout(() => requestPairingCodeWithRetry().catch(()=>{}), 1200);
      safeReconnect(1500);
    }
  });

  // start web server
  startPaymentWebhook();

  const send = (jid, text) => safeSendText(jid, text);
  const sendImage = (jid, buf, cap) => safeSendImage(jid, buf, cap);

  setInterval(() => checkExpiry(send), 5 * 60 * 1000);

  // ===== message handler =====
  sock.ev.on("messages.upsert", async mUp => {
    try {
      const m = mUp.messages?.[0]; if (!m || !m.message || m.key.fromMe) return;
      const jid = m.key.remoteJid; if (jid.endsWith("@g.us")) return;

      const sender = jidNormalizedUser(m.key.participant || jid);
      const isAdmin = isAdminJid(sender);
      const user = getUser(sender);
      const firstTime = !user._welcomed; user._welcomed = true;

      const body =
        m.message.conversation ||
        m.message.extendedTextMessage?.text ||
        m.message.imageMessage?.caption ||
        m.message.videoMessage?.caption || "";
      let text = (body || "").trim(); if (!text) return;
      if (text.length > MAX_MSG_CHARS) text = text.slice(0, MAX_MSG_CHARS);
      if (user.banned) { await send(jid, "🚫 Akses dibatasi. Hubungi admin."); return; }
      if (!allowProcess(user)) return;

      if (firstTime) { await db.write(); await send(jid, [`👋 *Selamat datang di ${STORE_NAME}!*`, "Ketik *menu* untuk melihat produk, atau *help* untuk panduan lengkap.", ""].join("\n")); }

      const lower = text.toLowerCase();

      // help & menu
      if (["help","/help"].includes(lower)) { await send(jid, userHelpFull()); return; }
      if (["menu","/menu","/start","start"].includes(lower)) { await send(jid, prettyMenu()); return; }

      if (lower === "adminhelp" || lower === "adminhelp@") { if (!isAdmin) { await send(jid, "Perintah khusus admin."); return; } await send(jid, adminHelp()); return; }
      if (isAdmin && lower === "open") { db.data.settings.storeOpen = true; await db.write(); await send(jid, "🟢 Toko *OPEN*."); return; }
      if (isAdmin && lower === "close") { db.data.settings.storeOpen = false; await db.write(); await send(jid, "🔴 Toko *CLOSED*."); return; }

      if (lower === "ping") { await send(jid, "pong"); return; }
      if (["cs","kontak","contact","support"].includes(lower)) { await send(jid, `👨‍💻 *Customer Support*\n${CS_CONTACT}\n🕒 ${formatTs(now())}`); return; }

      // myinfo
      if (lower === "myinfo") {
        const stats = user.stats || {};
        const pc = user.productCount || {};
        const top = Object.entries(pc).sort((a,b)=> (b[1]||0)-(a[1]||0)).slice(0,3).map(([id,c])=>{
          const p = productById(id); return `• ${p?.title || id} x${c}`;
        });
        await send(jid, [
          "*👤 Info Akun Kamu*",
          `• Nama: ${user.name || "Customer"}`,
          `• Bergabung: ${formatTs(user.joinedAt)}`,
          "",
          "*📈 Statistik*",
          `• Order (PAID): ${stats.paid || 0}`,
          `• Order terkirim (SENT): ${stats.sent || 0}`,
          `• Refund: ${stats.refunded || 0}`,
          `• Canceled: ${stats.canceled || 0}`,
          `• Total dibelanjakan: ${formatPrice(stats.totalSpent || 0)}`,
          "",
          "*🏆 Top produk:*",
          top.length ? top.join("\n") : "-",
        ].join("\n")); return;
      }

      // cekorder
      if (lower.startsWith("cekorder ")) {
        const id = text.split(/\s+/)[1];
        const o = db.data.orders.find(x => x.id === id && x.userJid === sender);
        if (!o) { await send(jid, "⚠️ Order tidak ditemukan."); return; }
        const itemsText = summarizeOrderItems(o.items);
        const head = [
          `🧾 *Detail Order*`,
          `1) *ID:* ${o.id}  ${statusEmoji(o.status)} *${o.status}*`,
          `2) *Item:*`,
          itemsText,
          "—",
          `3) *Total:* ${formatPrice(o.total)}`,
          o.expectedAmount ? `   *Tagihan:* ${formatPrice(o.expectedAmount)}` : null,
          "",
          `🕒 *Waktu:*`,
          `• Dibuat: ${formatTs(o.createdAt)}`,
          o.paidAt ? `• Dibayar: ${formatTs(o.paidAt)}` : null,
          o.confirmedAt ? `• Dikonfirmasi: ${formatTs(o.confirmedAt)}` : null,
          o.sentAt ? `• Dikirim: ${formatTs(o.sentAt)}` : null,
          o.canceledAt ? `• Dibatalkan: ${formatTs(o.canceledAt)}` : null,
          o.refundedAt ? `• Refund: ${formatTs(o.refundedAt)}` : null
        ].filter(Boolean).join("\n");

        if (o.delivered && o.delivered.length) {
          const blocks = o.delivered.map((d, i) => {
            const prod = productById(d.prodId);
            return formatAccountBlock(d.fields || {}, i+1, prod);
          }).join("\n\n");
          await sendLongText(jid, head + "\n\n" + "*📦 Riwayat Pengiriman Akun:*\n" + blocks);
        } else {
          await send(jid, head);
        }
        return;
      }

      // orders (USER)
      if (lower === "orders") {
        const list = [...db.data.orders].filter(o => o.userJid === sender).reverse().slice(0, 10);
        if (!list.length) { await send(jid, "Belum ada order."); return; }
        const out = list.map((o, i) => [
          `${i+1}. ${statusEmoji(o.status)} *${o.id}*`,
          `   • *Item:* ${summarizeItemsOneLine(o.items)}`,
          `   • *Tagihan:* ${formatPrice(o.expectedAmount || o.total)}`,
          `   🕒 ${formatTs(o.createdAt)}`
        ].join("\n")).join("\n\n");
        await send(jid, `🧾 *Riwayat Order Kamu*\n\n${out}`);
        return;
      }

      // beli
      if (lower.startsWith("beli ")) {
        if (!db.data.settings.storeOpen) { await send(jid, "🛑 Maaf, toko sedang *CLOSED*. Coba lagi nanti ya."); return; }
        const parts = text.split(/\s+/); const code = parts[1]; const qty = Math.max(1, Number(parts[2] || "1") || 1);
        const p = productById(code);
        if (!p) { await send(jid, "⚠️ Kode produk tidak ditemukan. Ketik *menu* untuk melihat daftar."); return; }
        if (Number(p.stock || 0) < qty) { await send(jid, `⚠️ Stok tidak cukup. Sisa: ${stokLabel(p.stock)}.`); return; }

        const orderId = makeOrderId();
        const items = [{ id: p.id, qty }];

        if (!lockStockForOrder(items)) { await send(jid, `⚠️ Stok berubah. Coba ulang, sisa: ${productById(p.id)?.stock ?? 0}.`); return; }

        const total = items.reduce((acc, it) => { const _p = productById(it.id); return acc + finalPrice(_p.price, _p.discount) * it.qty; }, 0);

        const taken = new Set(db.data.orders.filter(o => (o.status === "NEW" || o.status === "AWAIT_CONFIRM") && o.expectedAmount).map(o => o.expectedAmount));
        let add = Math.floor(Math.random() * 300) + 1, tries = 0; while (taken.has(total + add) && tries < 350) { add = (add % 300) + 1; tries++; }
        const expectedAmount = total + add;

        const order = {
          id: orderId, userJid: sender, name: user.name,
          items, total, uniqueAdd: add, expectedAmount,
          paymentMsgKey: null, qrisPayload: null,
          status: "NEW",
          createdAt: now(), paidAt: null, confirmedAt: null, sentAt: null, canceledAt: null, refundedAt: null,
          delivered: [],
          _stockReleased: false
        };
        db.data.orders.push(order); await db.write(); audit(`NEW ${order.id} by ${sender} total=${total} expected=${expectedAmount}`);

        const itemsText = summarizeOrderItems(items);
        const caption = buildPaymentCaption(order, itemsText);

        try {
          if (!QRIS_PAYLOAD_BASE) {
            const sent = await send(jid, "⚠️ QRIS belum disetel. Hubungi admin."); order.paymentMsgKey = sent?.key || null;
          } else {
            let payloadToUse = QRIS_PAYLOAD_BASE;
            if (QRIS_DYNAMIC) { try { payloadToUse = buildDynamicQRIS(QRIS_PAYLOAD_BASE, expectedAmount, order.id); order.qrisPayload = payloadToUse; } catch (e) { console.error("buildDynamicQRIS error:", e); } }
            const buf = await qrisPngBufferFromPayload(payloadToUse);
            const sent = await safeSendImage(jid, buf, caption); order.paymentMsgKey = sent?.key || null;
          }
          await db.write();
        } catch (e) {
          console.error("QRIS send error:", e);
          const sent = await send(jid, caption + "\n\n⚠️ Gagal memuat gambar QR. Minta admin kirim ulang QR.");
          order.paymentMsgKey = sent?.key || null; await db.write();
        }

        if (OWNER) {
          await send(OWNER + "@s.whatsapp.net", [
            "🛒 *Order baru masuk*",
            `*ID:* ${order.id}`,
            `*User:* ${cleanNumber(sender)} (${user.name})`,
            `*Item:* ${summarizeItemsOneLine(items)}`,
            `*Total:* ${formatPrice(total)}`,
            `*Tagihan:* ${formatPrice(expectedAmount)}`,
            `🕒 ${formatTs(order.createdAt)}`,
            "",
            `⏳ Menunggu transfer (auto-verify).`
          ].join("\n"));
        }
        return;
      }

      // bayar
      if (lower.startsWith("bayar ")) {
        const id = text.split(/\s+/)[1];
        const order = db.data.orders.find(o => o.id === id && o.userJid === sender);
        if (!order) { await send(jid, "⚠️ ID order tidak ditemukan."); return; }
        if (!["NEW","AWAIT_CONFIRM"].includes(order.status)) { await send(jid, `⚠️ Order *${id}* sudah diproses (status: ${order.status}).`); return; }
        order.status = "AWAIT_CONFIRM"; order.paidAt = now(); await db.write();
        audit(`AWAIT_CONFIRM ${id} by ${sender}`);

        await send(jid, [
          "✅ Terima kasih, bukti pembayaran kamu *kami terima*!",
          `*ID:* ${order.id}`,
          `*Item:* ${summarizeItemsOneLine(order.items)}`,
          `🕒 ${formatTs(order.paidAt)}`,
          "",
          "Status: *Menunggu verifikasi admin*",
          "Jika nominal *persis*, biasanya auto-verify segera ✅"
        ].join("\n"));

        if (OWNER) {
          await send(OWNER + "@s.whatsapp.net", [
            "💸 *Pembayaran masuk (manual ack)*",
            `*ID:* ${order.id}`,
            `*User:* ${cleanNumber(sender)} (${user.name})`,
            `*Tagihan:* ${formatPrice(order.expectedAmount || order.total)}`,
            `🕒 ${formatTs(order.paidAt)}`,
            "",
            `⚙️ Ketik: *konfirmasi ${order.id} ${cleanNumber(sender)}* bila valid.`
          ].join("\n"));
        }
        return;
      }

      // ===== ADMIN OPS =====
      if (isAdmin) {
        if (lower === "monitor") { await send(jid, await buildMonitorText()); return; }

        // /orders variants
        if (lower.startsWith("/orders")) {
          const parts = text.trim().split(/\s+/);
          let mode = parts[1]?.toLowerCase() || "";
          let limit = Math.max(1, Number(parts[2] || "10") || 10);
          let list = [...db.data.orders];

          if (mode === "user") {
            const num = cleanNumber(parts[2] || "");
            if (!num) { await send(jid, "Format: /orders user <nomor> [limit]"); return; }
            limit = Math.max(1, Number(parts[3] || "10") || 10);
            list = list.filter(o => cleanNumber(o.userJid) === num);
          } else if (mode && mode !== "all") {
            const target = mode.toUpperCase();
            list = list.filter(o => o.status === target);
          }

          list = list.slice(-limit).reverse();
          if (!list.length) { await send(jid, "Tidak ada data."); return; }

          const out = list.map(o => {
            const u = db.data.users[jidNormalizedUser(o.userJid)];
            return `${statusEmoji(o.status)} *${o.id}* — ${cleanNumber(o.userJid)}${u?.name?` (${u.name})`:""} — *${o.status}* — *${formatPrice(o.total)}* — ${formatTs(o.createdAt)}`;
          }).join("\n");
          await send(jid, `🧾 *Orders*\n${out}`);
          return;
        }

        // /order <id> (admin lihat detail siapa pun)
        if (lower.startsWith("/order ")) {
          const id = text.split(/\s+/)[1];
          const o = db.data.orders.find(x => x.id === id);
          if (!o) { await send(jid, "Order tidak ditemukan."); return; }
          const itemsText = summarizeOrderItems(o.items);
          const head = [
            `🧾 *Detail Order (Admin)*`,
            `• ID: ${o.id}  ${statusEmoji(o.status)} ${o.status}`,
            `• User: ${cleanNumber(o.userJid)}`,
            `• Item: ${summarizeItemsOneLine(o.items)}`,
            `• Total: ${formatPrice(o.total)}  • Tagihan: ${formatPrice(o.expectedAmount || o.total)}`,
            `• Waktu: dibuat ${formatTs(o.createdAt)}` +
              (o.paidAt? ` | dibayar ${formatTs(o.paidAt)}`:"") +
              (o.sentAt? ` | dikirim ${formatTs(o.sentAt)}`:"")
          ].join("\n");

          if (o.delivered && o.delivered.length) {
            const blocks = o.delivered.map((d,i)=> {
              const prod = productById(d.prodId);
              return formatAccountBlock(d.fields || {}, i+1, prod);
            }).join("\n\n");
            await sendLongText(jid, head + "\n\n" + blocks);
          } else {
            await send(jid, head + "\n\n" + itemsText);
          }
          return;
        }

        if (lower.startsWith("konfirmasi ")) {
          const [, id, numRaw] = text.split(/\s+/);
          const num = cleanNumber(numRaw || "");
          if (!id || !num) { await send(jid, "Format: konfirmasi <idorder> <nomorUser>"); return; }
          const uj = num + "@s.whatsapp.net";
          const order = db.data.orders.find(o => o.id === id && o.userJid === uj);
          if (!order) { await send(jid, "⚠️ Order tidak ditemukan atau nomor tidak cocok."); return; }
          if (!["AWAIT_CONFIRM","NEW","PAID"].includes(order.status)) { await send(jid, `⚠️ Order ${id} tidak dalam status menunggu (status: ${order.status}).`); return; }

          order.status = "PAID"; order.confirmedAt = now(); order.paidAt ||= order.confirmedAt; await db.write();
          try { if (order.paymentMsgKey) { await safeDelete(uj, order.paymentMsgKey); order.paymentMsgKey = null; await db.write(); } } catch {}
          audit(`PAID ${id} by admin ${sender}`);
          addUserStatsForOrderPaid(order); await db.write();

          await send(jid, `✅ Order ${id} ditandai *PAID*.${AUTO_SEND ? " (AUTO_SEND aktif — mencoba kirim...)" : " Lanjut *send* untuk kirim detil."}`);
          await safeSendText(uj, [
            "✅ Pembayaran kamu *sudah diverifikasi*!",
            `*ID:* ${id}`,
            `*Item:* ${summarizeItemsOneLine(order.items)}`,
            `🕒 ${formatTs(order.confirmedAt)}`,
            "",
            AUTO_SEND ? "⏳ Pesanan akan dikirim otomatis..." : "Kami segera kirim detail akun ya 🙏"
          ].join("\n"));

          if (AUTO_SEND) { try { await maybeAutoSend(order); } catch (e) { console.error("autoSend error:", e); } }
          return;
        }

        if (lower.startsWith("batal ")) {
          const tok = text.split(/\s+/), id = tok[1], num = cleanNumber(tok[2] || "");
          const reason = tok.slice(3).join(" ").trim() || "Tanpa keterangan";
          if (!id || !num) { await send(jid, "Format: batal <idorder> <nomorUser> [alasan]"); return; }
          const uj = num + "@s.whatsapp.net";
          const order = db.data.orders.find(o => o.id === id && o.userJid === uj);
          if (!order) { await send(jid, "⚠️ Order tidak ditemukan atau nomor tidak cocok."); return; }
          if (["SENT","REFUNDED","CANCELED"].includes(order.status)) { await send(jid, `⚠️ Order ${id} tidak bisa dibatalkan (status: ${order.status}).`); return; }

          order.status = "CANCELED"; order.canceledAt = now(); releaseStock(order); await db.write();
          audit(`CANCELED ${id} by admin ${sender} reason=${reason}`);
          addUserStatsForOrderCanceled(order); await db.write();

          await send(jid, `⛔ Order ${id} *dibatalkan*.`);
          await safeSendText(uj, [
            `⛔ *Pesanan dibatalkan*`,
            `*ID:* ${id}`,
            `*Item:* ${summarizeItemsOneLine(order.items)}`,
            `*Alasan:* ${reason}`,
            `🕒 ${formatTs(order.canceledAt)}`,
            "",
            "Jika butuh bantuan, silakan hubungi CS:",
            CS_CONTACT
          ].join("\n"));
          return;
        }

        if (lower.startsWith("refund ")) {
          const tok = text.split(/\s+/), id = tok[1], num = cleanNumber(tok[2] || "");
          const reason = tok.slice(3).join(" ").trim() || "Tanpa keterangan";
          if (!id || !num) { await send(jid, "Format: refund <idorder> <nomorUser> [alasan]"); return; }
          const uj = num + "@s.whatsapp.net";
          const order = db.data.orders.find(o => o.id === id && o.userJid === uj);
          if (!order) { await send(jid, "⚠️ Order tidak ditemukan atau nomor tidak cocok."); return; }

          if (["PAID","AWAIT_CONFIRM","NEW","CANCELED"].includes(order.status)) releaseStock(order);
          else if (order.status === "SENT") { for (const it of order.items) { const p = productById(it.id); if (p) p.sold = Math.max(0, Number(p.sold || 0) - it.qty); } saveProducts(); }
          order.status = "REFUNDED"; order.refundedAt = now(); await db.write();
          audit(`REFUNDED ${id} by admin ${sender} reason=${reason}`);
          addUserStatsForOrderRefunded(order); await db.write();

          await send(jid, `💵 Order ${id} ditandai *REFUNDED*.`);
          await safeSendText(uj, [
            `💵 *Refund diproses/selesai*`,
            `*ID:* ${id}`,
            `*Item:* ${summarizeItemsOneLine(order.items)}`,
            `*Alasan:* ${reason}`,
            `🕒 ${formatTs(order.refundedAt)}`,
            "",
            "Jika butuh bantuan, silakan hubungi CS:",
            CS_CONTACT
          ].join("\n"));
          return;
        }

        // send manual (payload custom)
        if (lower.startsWith("send ")) {
          const tok = text.split(/\s+/), id = tok[1], num = cleanNumber(tok[2] || "");
          const restIdx = text.indexOf(num) + String(num).length;
          const payload = text.slice(restIdx + 1).trim();
          if (!id || !num || !payload) { await send(jid, "Format: send <idorder> <nomor> email:..|password:..||email:..|password:..|pin:..|profil:..|durasi:..|notes:..|rules:.."); return; }

          const uj = num + "@s.whatsapp.net";
          const order = db.data.orders.find(o => o.id === id && o.userJid === uj);
          if (!order) { await send(jid, "⚠️ Order tidak ditemukan atau nomor tidak cocok."); return; }
          if (order.status !== "PAID") { await send(jid, `⚠️ Order ${id} belum *PAID*. Konfirmasi dulu.`); return; }

          const blocks = payload.split("||").map(s => s.trim()).filter(Boolean);
          if (!blocks.length) { await send(jid, "⚠️ Payload kosong. Gunakan pemisah '||' untuk multi item."); return; }

          const prod = productById(order.items[0]?.id);
          order.delivered ||= [];
          let i = 1;
          for (const b of blocks) {
            const fields = normalizeFields(parseStockPayload(b) || {});
            const msg = [
              "🎉 *Pesanan kamu sudah terkirim!*",
              "",
              formatAccountBlock(fields, i++, prod),
              "",
              `*ID Order:* ${order.id}`,
              `*Produk:* ${summarizeItemsOneLine(order.items)}`,
              `🕒 ${formatTs(now())}`,
              "",
              "Jika ada kendala login atau butuh bantuan, balas chat ini ya. 🙌"
            ].join("\n");
            await safeSendText(uj, msg);
            order.delivered.push({ at: now(), prodId: prod?.id || order.items[0]?.id, fields });
          }
          await db.write();

          for (const it of order.items) { const p = productById(it.id); if (p) p.sold = Number(p.sold || 0) + Number(it.qty || 0); }
          saveProducts();
          order.status = "SENT"; order.sentAt = now(); await db.write();
          addUserStatsForOrderSent(order); await db.write();

          await send(jid, `✅ Dikirim ${blocks.length}/${order.items.reduce((a,i)=>a+i.qty,0)} unit untuk *${id}* ke ${num}. 🕒 ${formatTs(order.sentAt)}`);
          return;
        }

        // ===== CRUD & Pool =====
        if (lower.startsWith("addprod ")) {
          const raw = text.slice(8).trim();
          const [id, title, category, price, discount, stock, sold, descRaw] = raw.split("|").map(s => s?.trim());
          if (!id || !title || !category || price === undefined) { await send(jid, "Format: addprod id|title|category|price|discount|stock|sold|desc1;desc2"); return; }
          if (productById(id)) { await send(jid, "⚠️ ID produk sudah ada."); return; }
          const prod = {
            id: id.replace(/\s/g,""), title: clampText(title,100), category: clampText(category,50),
            price: Number(price)||0, discount: Number(discount)||0,
            stock: Math.max(0, Number(stock)||0), sold: Math.max(0, Number(sold)||0),
            descLines: (descRaw||"").split(";").map(s => clampText(s.trim(),120)).filter(Boolean),
            defaultRules: "", defaultNotes: "", cost: 0
          };
          PRODUCTS.push(prod); saveProducts(); audit(`ADDPROD ${prod.id} by ${sender}`);
          await send(jid, `✅ Produk ${prod.id} ditambahkan.\n${prod.title} — ${formatPrice(finalPrice(prod.price, prod.discount))}\nStok: ${stokLabel(prod.stock)} | Sold: ${prod.sold}`);
          return;
        }

        if (lower.startsWith("setrules ")) {
          const raw = text.slice(9).trim(); const [id, rules] = raw.split("|");
          const p = productById((id||"").trim()); if (!p) { await send(jid, "Produk tidak ditemukan."); return; }
          p.defaultRules = clampText((rules||"").trim(), 1500); saveProducts(); await send(jid, `✅ Rules default untuk *${p.id}* diperbarui.`); return;
        }
        if (lower.startsWith("setnotes ")) {
          const raw = text.slice(9).trim(); const [id, notes] = raw.split("|");
          const p = productById((id||"").trim()); if (!p) { await send(jid, "Produk tidak ditemukan."); return; }
          p.defaultNotes = clampText((notes||"").trim(), 1500); saveProducts(); await send(jid, `✅ Notes default untuk *${p.id}* diperbarui.`); return;
        }
        if (lower.startsWith("setcost ")) {
          const raw = text.slice(8).trim(); const [id, costRaw] = raw.split("|").map(s=>s?.trim());
          const p = productById(id); if (!p) { await send(jid, "Produk tidak ditemukan."); return; }
          const c = Number(costRaw); if (!Number.isFinite(c) || c < 0) { await send(jid, "⚠️ Cost harus angka ≥ 0."); return; }
          p.cost = c; saveProducts(); await send(jid, `✅ Cost ${id} diset: ${formatPrice(p.cost)}.`); return;
        }

        if (lower.startsWith("prod ")) {
          const id = text.split(/\s+/)[1]; const p = productById(id);
          if (!p) { await send(jid, "Produk tidak ditemukan."); return; }
          const price = finalPrice(p.price, p.discount);
          const desc = (p.descLines || []).map(d => `- ${d}`).join("\n") || "-";
          await send(jid, [
            `*${p.title}*`,
            `*ID:* ${p.id}`,
            `*Kategori:* ${p.category}`,
            `*Harga:* ${formatPrice(price)}${p.discount ? ` (diskon *${p.discount}%*)` : ""}`,
            `*Stok:* ${stokLabel(p.stock)} | *Terjual:* ${p.sold || 0}`,
            p.defaultNotes ? `*Notes (default):*\n${p.defaultNotes}` : "",
            p.defaultRules ? `*Rules (default):*\n${p.defaultRules}` : "",
            p.cost ? `*Cost:* ${formatPrice(p.cost)}` : "",
            `*Deskripsi:*`,
            desc
          ].filter(Boolean).join("\n")); return;
        }

        if (lower.startsWith("prods")) {
          const page = Math.max(1, Number(text.split(/\s+/)[1] || "1") || 1);
          const per = 10; const list = PRODUCTS.slice((page - 1) * per, page * per);
          if (!list.length) { await send(jid, "Tidak ada data di halaman ini."); return; }
          const lines = list.map(p => `• [${p.id}] ${p.title} — *${formatPrice(finalPrice(p.price, p.discount))}* — stok *${stokLabel(p.stock)}*`);
          await send(jid, `Halaman ${page}\n${lines.join("\n")}`); return;
        }

        if (lower.startsWith("search ")) {
          const q = text.slice(7).toLowerCase();
          const res = PRODUCTS.filter(p => p.id.toLowerCase().includes(q) || p.title.toLowerCase().includes(q) || p.category.toLowerCase().includes(q)).slice(0, 20);
          if (!res.length) { await send(jid, "Tidak ada hasil."); return; }
          await send(jid, res.map(p => `• [${p.id}] ${p.title} — ${formatPrice(finalPrice(p.price, p.discount))} — stok ${stokLabel(p.stock)}`).join("\n")); return;
        }

        if (lower.startsWith("setharga ")) {
          const raw = text.slice(9).trim(); const [id, price, discount] = raw.split("|").map(s => s?.trim());
          const p = productById(id); if (!p) { await send(jid, "Produk tidak ditemukan."); return; }
          if (price !== undefined && price !== "") { const v = Number(price); if (!Number.isFinite(v) || v < 0) { await send(jid, "⚠️ Harga harus angka ≥ 0."); return; } p.price = v; }
          if (discount !== undefined) { const d = Number(discount); if (!Number.isFinite(d) || d < 0 || d > 100) { await send(jid, "⚠️ Diskon harus 0–100."); return; } p.discount = d; }
          saveProducts(); audit(`SETHARGA ${id} by ${sender}`); await send(jid, `✅ Harga ${id}: ${formatPrice(finalPrice(p.price, p.discount))} (disc ${p.discount}%)`); return;
        }

        if (lower.startsWith("setstok ")) {
          const raw = text.slice(8).trim(); const [id, qtyRaw] = raw.split("|").map(s => s?.trim());
          const p = productById(id); if (!p) { await send(jid, "Produk tidak ditemukan."); return; }
          let qtyParsed; if (/^kosong$/i.test(qtyRaw || "")) qtyParsed = 0; else qtyParsed = Number(qtyRaw);
          if (!Number.isFinite(qtyParsed) || qtyParsed < 0) { await send(jid, "⚠️ Nilai stok harus angka ≥ 0. Contoh: setstok test|0"); return; }
          p.stock = qtyParsed; saveProducts(); audit(`SETSTOK ${id} by ${sender}`); await send(jid, `✅ Stok ${id}: ${stokLabel(p.stock)}`); return;
        }

        if (lower.startsWith("settitle ")) {
          const raw = text.slice(9).trim(); const [id, title] = raw.split("|").map(s => s?.trim());
          const p = productById(id); if (!p) { await send(jid, "Produk tidak ditemukan."); return; }
          p.title = clampText(title, 100) || p.title; saveProducts(); audit(`SETTITLE ${id} by ${sender}`); await send(jid, `✅ Title ${id} diubah.`); return;
        }

        if (lower.startsWith("setcat ")) {
          const raw = text.slice(7).trim(); const [id, cat] = raw.split("|").map(s => s?.trim());
          const p = productById(id); if (!p) { await send(jid, "Produk tidak ditemukan."); return; }
          p.category = clampText(cat, 50) || p.category; saveProducts(); audit(`SETCAT ${id} by ${sender}`); await send(jid, `✅ Category ${id} diubah.`); return;
        }

        if (lower.startsWith("setdesc ")) {
          const raw = text.slice(8).trim(); const [id, descRaw] = raw.split("|");
          const p = productById(id); if (!p) { await send(jid, "Produk tidak ditemukan."); return; }
          p.descLines = (descRaw || "").split(";").map(s => clampText(s.trim(), 120)).filter(Boolean);
          saveProducts(); audit(`SETDESC ${id} by ${sender}`); await send(jid, `✅ Deskripsi ${id} diubah (${p.descLines.length} baris).`); return;
        }

        if (lower.startsWith("delprod ")) {
          const id = text.split(/\s+/)[1]; const p = productById(id);
          if (!p) { await send(jid, "Produk tidak ditemukan."); return; }
          db.data._pendingDel = { id, by: sender, at: Date.now() }; await db.write();
          await send(jid, `⚠️ Konfirmasi hapus: ketik *ya ${id}* dalam 30 detik untuk menghapus.`); return;
        }

        if (lower.startsWith("ya ")) {
          const id = text.split(/\s+/)[1];
          const pend = db.data._pendingDel;
          if (!pend || pend.id !== id || pend.by !== sender || Date.now() - pend.at > 30000) { await send(jid, "⚠️ Tidak ada penghapusan tertunda / waktu habis."); return; }
          delete db.data._pendingDel;
          const idx = PRODUCTS.findIndex(p => p.id.toLowerCase() === id.toLowerCase());
          if (idx === -1) { await send(jid, "Produk tidak ditemukan."); return; }
          const removed = PRODUCTS.splice(idx, 1)[0]; saveProducts(); await db.write(); audit(`DELPROD ${id} by ${sender}`);
          await send(jid, `✅ Produk ${removed.id} dihapus.`); return;
        }

        // pool admin
        if (lower.startsWith("addstock ")) {
          const id = text.split(/\s+/)[1];
          const payload = text.slice(text.indexOf(id) + id.length + 1).trim();
          if (!id || !payload) { await send(jid, "Format: addstock <id> <email|password|pin|profil|durasi|ops1|ops2|ops3|notes|rules>"); return; }
          const res = addStock(id, payload);
          if (!res.ok) { await send(jid, "⚠️ " + res.err); return; }
          await send(jid, `✅ 1 akun ditambahkan ke pool *${id}*. Sisa di pool: *${res.left}*`);
          return;
        }

        if (lower.startsWith("addstockmulti ")) {
          const id = text.split(/\s+/)[1];
          const payload = text.slice(text.indexOf(id) + id.length + 1).trim();
          if (!id || !payload) { await send(jid, "Format: addstockmulti <id> <baris1>||<baris2>||<baris3>"); return; }
          const res = addStockMulti(id, payload);
          await send(jid, `✅ Import pool *${id}*: tambah ${res.ok}, gagal ${res.fail}. Total pool sekarang: *${res.left}*`);
          return;
        }

        if (lower.startsWith("stock ")) {
          const id = text.split(/\s+/)[1];
          if (!id) { await send(jid, "Format: stock <id>"); return; }
          const n = stockCount(id);
          const sample = readStock(id).slice(0, Math.min(3, n)).map((o,i)=> `${i+1}. ${maskEmail(o.email||"-")} | **** | ${o.profil?("Profil:"+o.profil):""}`);
          await send(jid, [`📦 *Pool ${id}*`,`Jumlah: *${n}*`, sample.length ? "Contoh:\n" + sample.join("\n") : ""].join("\n"));
          return;
        }

        if (lower.startsWith("stockall ") || lower.startsWith("cekallstock ")) {
          const id = text.split(/\s+/)[1];
          if (!id) { await send(jid, "Format: stockall <id>"); return; }
          const list = readStock(id);
          if (!list.length) { await send(jid, `Pool *${id}* kosong.`); return; }
          const lines = list.map((o, i) => {
            const ent = Object.entries(o).map(([k,v]) => `${(LABELS[k]||capitalize(k))}: ${v}`).join(" | ");
            return `${i+1}. ${ent}`;
          }).join("\n");
          await sendLongText(jid, `📦 *Pool Detail ${id}* (total ${list.length})\n` + lines);
          return;
        }

        if (lower.startsWith("syncstock ")) {
          const arg = text.split(/\s+/)[1];
          if (!arg) { await send(jid, "Format: syncstock <id>|all"); return; }
          if (arg.toLowerCase() === "all") {
            for (const p of PRODUCTS) { p.stock = stockCount(p.id); }
            saveProducts(); await send(jid, "✅ Sinkron semua produk: *stock = jumlah pool*.");
          } else {
            const p = productById(arg);
            if (!p) { await send(jid, "Produk tidak ditemukan."); return; }
            p.stock = stockCount(p.id); saveProducts(); await send(jid, `✅ Sinkron stok *${p.id}* = ${stokLabel(p.stock)}`);
          }
          return;
        }

        if (lower.startsWith("sendstock ")) {
          // sendstock <id>|<nomor>|<jumlah>  atau sendstock <id> <nomor> <jumlah>
          let id, num, qty;
          const after = text.slice(9).trim();
          if (after.includes("|")) {
            const [a,b,c] = after.split("|").map(s => s.trim());
            id = a; num = cleanNumber(b||""); qty = Number(c||"1");
          } else {
            const parts = after.split(/\s+/);
            id = parts[0]; num = cleanNumber(parts[1]||""); qty = Number(parts[2]||"1");
          }
          if (!id || !num || !Number.isFinite(qty) || qty <= 0) { await send(jid, "Format: sendstock <id>|<nomor>|<jumlah>"); return; }
          const uj = num + "@s.whatsapp.net";

          const creds = takeStock(id, qty);
          if (!creds || creds.length < qty) { await send(jid, `⚠️ Pool *${id}* tidak cukup. Sisa: ${stockCount(id)}`); return; }

          const prod = productById(id);
          let i = 1;
          for (const fields of creds) {
            const block = formatAccountBlock(fields, i++, prod);
            const msg = [
              "📦 *Kirim Manual*",
              "",
              block,
              "",
              `*Produk:* ${prod?.title || id}`,
              `🕒 ${formatTs(now())}`,
              "",
              "Jika ada kendala login, balas chat ini ya. 🙌"
            ].join("\n");
            try { await safeSendText(uj, msg); } catch {}
          }

          if (prod) { prod.stock = stockCount(id); saveProducts(); }
          await send(jid, `✅ Kirim manual *${qty}* akun *${id}* ke ${num} selesai. Sisa pool: *${stockCount(id)}*`);
          return;
        }

        // ===== REPORT =====
        if (lower.startsWith("report")) {
          const parts = text.trim().split(/\s+/);
          let mode = parts[1]?.toLowerCase() || "today";
          let from = 0, to = Date.now();

          const startOfDay = (ts)=>{ const d=new Date(ts); d.setHours(0,0,0,0); return d.getTime(); };
          const endOfDay = (ts)=>{ const d=new Date(ts); d.setHours(23,59,59,999); return d.getTime(); };

          if (mode === "today") { from = startOfDay(Date.now()); to = endOfDay(Date.now()); }
          else if (mode === "month") { const d=new Date(); d.setDate(1); d.setHours(0,0,0,0); from=d.getTime(); to=Date.now(); }
          else if (mode === "all") { from = 0; to = Date.now(); }
          else if (mode === "range") {
            const a=parts[2], b=parts[3];
            if (!a || !b) { await send(jid, "Format: report range <YYYY-MM-DD> <YYYY-MM-DD>"); return; }
            from = new Date(a+"T00:00:00").getTime(); to = new Date(b+"T23:59:59").getTime();
          }

          // aggregate
          const inRange = o => (o.createdAt >= from && o.createdAt <= to);
          const ls = (db.data.orders || []).filter(inRange);
          const countBy = st => ls.filter(o => o.status === st).length;

          // GMV & revenue (paid)
          let gmv = 0, revenue = 0, profit = 0;
          for (const o of ls) {
            // GMV = total (tanpa kode unik)
            gmv += Number(o.total || 0);
            if (["PAID","SENT"].includes(o.status)) {
              // revenue pakai total (tanpa kode unik) agar relevan
              revenue += Number(o.total || 0);
              // profit jika ada cost per produk
              for (const it of o.items) {
                const p = productById(it.id); const price = finalPrice(p.price, p.discount);
                const cost = Number(p.cost || 0); profit += Math.max(0, price - cost) * Number(it.qty || 0);
              }
            }
          }

          // top produk
          const soldMap = {};
          for (const o of ls) for (const it of o.items) soldMap[it.id] = (soldMap[it.id]||0) + it.qty;
          const top = Object.entries(soldMap).sort((a,b)=>b[1]-a[1]).slice(0,5).map(([id,c])=> `• ${productById(id)?.title || id} x${c}`);

          const lines = [
            `*📊 Report* (${new Date(from).toISOString().slice(0,10)} → ${new Date(to).toISOString().slice(0,10)})`,
            `• Orders: ${ls.length}  (${["NEW","AWAIT_CONFIRM","PAID","SENT","REFUNDED","CANCELED","EXPIRED"].map(s=> `${s}=${countBy(s)}`).join(", ")})`,
            `• GMV: ${formatPrice(gmv)}`,
            `• Revenue dibayar (PAID+SENT): ${formatPrice(revenue)}`,
            `• Profit (jika cost diisi): ${profit>0?formatPrice(profit):"-"}`,
            "",
            "*🏆 Top Produk:*",
            top.length ? top.join("\n") : "-"
          ];
          await send(jid, lines.join("\n"));
          return;
        }

        if (lower.startsWith("bc all ")) {
          const msg = clampText(text.slice(7).trim(), 4000);
          const userJids = Object.keys(db.data.users || {});
          let sent = 0, fail = 0;
          for (const uj of userJids) { try { await safeSendText(uj, msg); sent++; await new Promise(r => setTimeout(r, 900)); } catch { fail++; } }
          await send(jid, `📣 Broadcast selesai. Terkirim: ${sent}, Gagal: ${fail}`); audit(`BC_ALL by ${sender} sent=${sent} fail=${fail}`); return;
        }
      }

      // fallback help
      await send(jid, userHelpShort());
    } catch (err) { console.error("handleMessage error:", err); }
  });
}

startSock().catch(err => console.error("fatal start error:", err));
process.on("unhandledRejection", err => { console.error("unhandledRejection:", err); });
process.on("uncaughtException", err => { console.error("uncaughtException:", err); });