// src/store-bot.js  — versi fix queue + legend + sendstock(orderId) + report guard + autobackup pCloud + BROADCAST(all/buyers/nobuy)
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
const axios = require("axios");
const FormData = require("form-data");
const archiver = require("archiver");

// ===== ENV (default awal; semua bisa di-set ulang via command admin) =====
const OWNER = (process.env.OWNER || "").replace(/[^0-9]/g, "");
const BOT_NUMBER = (process.env.BOT_NUMBER || "").replace(/[^0-9]/g, "");
const USE_PAIRING = /^(1|true)$/i.test(process.env.PAIRING || "");
const STORE_NAME = process.env.STORE_NAME || "Vienze Store";
const QRIS_PAYLOAD_BASE = process.env.QRIS_PAYLOAD || "";
const QRIS_DYNAMIC = /^(1|true)$/i.test(process.env.QRIS_DYNAMIC || "");
const EXPIRE_HOURS = Math.max(0, Number(process.env.EXPIRE_HOURS || "1"));
const RATE_LIMIT_MS = Math.max(0, Number(process.env.RATE_LIMIT_MS || "1200"));
const MAX_MSG_CHARS = Math.max(50, Number(process.env.MAX_MSG_CHARS || "500"));
const AUDIT_LOG = /^(1|true)$/i.test(process.env.AUDIT_LOG || "");
const WEBHOOK_PORT = Number(process.env.WEBHOOK_PORT || "3001");
const CS_CONTACT =
    process.env.CS_CONTACT || "Hubungi admin: wa.me/62XXXXXXXXXXX";
const AUTO_SEND = /^(1|true)$/i.test(process.env.AUTO_SEND || "");

// ===== PATHS =====
const root = process.cwd();
const dataDir = path.join(root, "data");
const logsDir = path.join(root, "logs");
const backupsDir = path.join(root, "backups");
const stockDir = path.join(dataDir, "stock");
for (const d of [dataDir, logsDir, backupsDir, stockDir])
    if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });

// ===== DB =====
const db = new Low(new JSONFile(path.join(dataDir, "db.json")), {
    users: {},
    orders: [],
    settings: {
        storeOpen: true,
        autoCloseWhenAllZero: true,
        backup: {
            enabled: true,
            interval: "daily", // daily|weekly
            hour: 3, // WIB
            lastRunDay: "",
            pcloud: { token: "", folder: "/VienzeStoreBackups" }
        }
    }
});
const productsPath = path.join(root, "products.json");
function loadProducts() {
    if (!fs.existsSync(productsPath)) fs.writeFileSync(productsPath, "[]");
    return JSON.parse(fs.readFileSync(productsPath, "utf8"));
}
let PRODUCTS = loadProducts();
function saveProducts() {
    fs.writeFileSync(productsPath, JSON.stringify(PRODUCTS, null, 2));
    backupSnapshot();
    evalAutoClose();
}

// ===== UTIL =====
const nanoID = customAlphabet("ABCDEFGHJKLMNPQRSTUVWXYZ23456789", 6);
const formatPrice = n => "Rp " + Number(n || 0).toLocaleString("id-ID");
const pct = x => Math.max(0, Math.min(100, Number(x || 0)));
const finalPrice = (p, d) =>
    Math.round((Number(p || 0) * (100 - pct(d))) / 100);
const cleanNumber = n => String(n || "").replace(/[^0-9]/g, "");
const isAdminJid = jid => OWNER && cleanNumber(jid) === OWNER;
const capitalize = s => (s || "").charAt(0).toUpperCase() + (s || "").slice(1);
const now = () => Date.now();
const todayStr = () => new Date().toISOString().slice(0, 10);
const clampText = (s, max = 4000) => {
    s = String(s || "");
    return s.length > max ? s.slice(0, max) + "…" : s;
};
const stokLabel = n => (Number(n || 0) === 0 ? "kosong" : String(n));
const DIV = "——————————";

const audit = line => {
    if (!AUDIT_LOG) return;
    const p = path.join(logsDir, `audit-${todayStr()}.log`);
    fs.appendFileSync(p, `[${new Date().toISOString()}] ${line}\n`);
};
function backupSnapshot() {
    const d = todayStr(),
        dir = path.join(backupsDir, d);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    try {
        fs.copyFileSync(productsPath, path.join(dir, "products.json"));
        const dbPath = path.join(dataDir, "db.json");
        if (fs.existsSync(dbPath))
            fs.copyFileSync(dbPath, path.join(dir, "db.json"));
    } catch {}
}

// WIB
function formatTs(ts) {
    if (!ts) return "-";
    const dt = new Date(ts);
    const pad = n => String(n).padStart(2, "0");
    const y = dt.toLocaleString("id-ID", {
        timeZone: "Asia/Jakarta",
        year: "numeric"
    });
    const M = pad(
        parseInt(
            dt.toLocaleString("id-ID", {
                timeZone: "Asia/Jakarta",
                month: "2-digit"
            }),
            10
        )
    );
    const d = pad(
        parseInt(
            dt.toLocaleString("id-ID", {
                timeZone: "Asia/Jakarta",
                day: "2-digit"
            }),
            10
        )
    );
    const h = pad(
        parseInt(
            dt.toLocaleString("id-ID", {
                timeZone: "Asia/Jakarta",
                hour: "2-digit",
                hour12: false
            }),
            10
        )
    );
    const m = pad(
        parseInt(
            dt.toLocaleString("id-ID", {
                timeZone: "Asia/Jakarta",
                minute: "2-digit"
            }),
            10
        )
    );
    const s = pad(
        parseInt(
            dt.toLocaleString("id-ID", {
                timeZone: "Asia/Jakarta",
                second: "2-digit"
            }),
            10
        )
    );
    return `${y}-${M}-${d} ${h}:${m}:${s} WIB`;
}

// ===== ensure DB shape =====
function ensureUserShape(u) {
    if (!u) return;
    u.name ||= "";
    u.joinedAt ||= now();
    u.stats ||= {
        orders: 0,
        paid: 0,
        sent: 0,
        refunded: 0,
        canceled: 0,
        totalSpent: 0
    };
    u.productCount ||= {};
    u.banned = !!u.banned;
    u.lastTs ||= 0;
}
async function ensureDB() {
    await db.read();
    db.data ||= {};
    db.data.users ||= {};
    db.data.orders ||= [];
    db.data.settings ||= {
        storeOpen: true,
        autoCloseWhenAllZero: true,
        backup: {
            enabled: true,
            interval: "daily",
            hour: 3,
            lastRunDay: "",
            pcloud: { token: "", folder: "/VienzeStoreBackups" }
        }
    };
    for (const id of Object.keys(db.data.users))
        ensureUserShape(db.data.users[id]);
    await db.write();
}
function getUser(jid) {
    const id = jidNormalizedUser(jid);
    db.data.users[id] ||= {
        name: "",
        banned: false,
        lastTs: 0,
        joinedAt: now(),
        stats: {
            orders: 0,
            paid: 0,
            sent: 0,
            refunded: 0,
            canceled: 0,
            totalSpent: 0
        },
        productCount: {}
    };
    ensureUserShape(db.data.users[id]);
    return db.data.users[id];
}
const productById = id =>
    PRODUCTS.find(p => p.id.toLowerCase() === String(id).toLowerCase());
const makeOrderId = () => "VS-" + nanoID();

// ===== status & legend =====
function statusEmoji(s) {
    return (
        {
            NEW: "🆕",
            AWAIT_CONFIRM: "⏳",
            PAID: "✅",
            SENT: "📦",
            REFUNDED: "💵",
            CANCELED: "⛔",
            EXPIRED: "⌛"
        }[s] || "•"
    );
}
const STATUS_LEGEND = [
    "🆕 NEW  | pesanan dibuat",
    "⏳ AWAIT_CONFIRM | menunggu verifikasi admin",
    "✅ PAID | sudah dibayar/terverifikasi",
    "📦 SENT | akun sudah dikirim",
    "💵 REFUNDED | dikembalikan",
    "⛔ CANCELED | dibatalkan",
    "⌛ EXPIRED | kadaluarsa"
].join("\n");

// ===== QRIS utils =====
function stripCRC(p) {
    return p.replace(/63(04)[0-9A-F]{4}$/i, "");
}
function findTag(p, tag) {
    const r = new RegExp(`(${tag})([0-9]{2})`, "g");
    let m;
    while ((m = r.exec(p)) !== null) {
        const idx = m.index,
            len = parseInt(p.substr(idx + 2, 2), 10);
        const valStart = idx + 4,
            valEnd = valStart + len;
        return {
            start: idx,
            end: valEnd,
            len,
            val: p.substring(valStart, valEnd)
        };
    }
    return null;
}
function setTag(p, tag, valueRaw) {
    const val = String(valueRaw),
        len = String(val.length).padStart(2, "0");
    const chunk = `${tag}${len}${val}`,
        hit = findTag(p, tag);
    if (hit) return p.substring(0, hit.start) + chunk + p.substring(hit.end);
    const without = stripCRC(p);
    return without + chunk;
}
function setTag62BillNumber(p, billNo) {
    const subVal = `01${String(billNo.length).padStart(2, "0")}${billNo}`;
    return setTag(p, "62", subVal);
}
function crc16ccitt(hexStr) {
    let crc = 0xffff;
    for (let i = 0; i < hexStr.length; i++) {
        crc ^= (hexStr.charCodeAt(i) & 0xff) << 8;
        for (let j = 0; j < 8; j++)
            (crc = crc & 0x8000 ? (crc << 1) ^ 0x1021 : crc << 1),
                (crc &= 0xffff);
    }
    return crc.toString(16).toUpperCase().padStart(4, "0");
}
function buildDynamicQRIS(basePayload, amountIDR, orderId) {
    if (!basePayload) throw new Error("QRIS base payload kosong");
    const amt = (Number(amountIDR) || 0).toFixed(2);
    let p = stripCRC(basePayload);
    p = setTag(p, "54", amt);
    p = setTag62BillNumber(p, orderId);
    const preCRC = p + "6304";
    return preCRC + crc16ccitt(preCRC);
}
async function qrisPngBufferFromPayload(payload) {
    return await QRCode.toBuffer(payload, {
        type: "png",
        errorCorrectionLevel: "M",
        margin: 2,
        width: 512
    });
}

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
        const desc =
            (p.descLines || []).map(d => "   - " + d).join("\n") || "   -";
        return [
            `📦 *${p.title}*`,
            `   • 💸 *Harga:* ${formatPrice(price)}${
                p.discount ? ` (diskon *${p.discount}%*)` : ""
            }`,
            `   • 🏷️ *Kode:* *${p.id}*`,
            `   • 📦 *Stok:* *${stokLabel(p.stock)}*   • 🔥 *Terjual:* *${
                p.sold || 0
            }*`,
            `   • 📝 *Deskripsi:*`,
            desc,
            idx === PRODUCTS.length - 1 ? "" : DIV
        ].join("\n");
    });
    return `${header}\n${blocks.join(
        "\n"
    )}\n\n✍️ _Ketik: *beli <kode> <jumlah>*_`;
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
        "👨‍💻 *cs* — kontak support"
    ].join("\n");
}
function userHelpShort() {
    return [
        "❓ *Perintah tidak dikenali.*",
        "",
        "📚 *Panduan Singkat*",
        "• 🧾 *menu* — daftar produk",
        "• 🛒 *beli <kode> <jumlah>*",
        "• 💳 *bayar <idorder>*",
        "• 🔎 *cekorder <id>*",
        "• 📂 *orders* — riwayat 10 order",
        "• 👤 *myinfo* — profil & statistik",
        "• 👨‍💻 *cs* — support"
    ].join("\n");
}
function adminHelp() {
    return [
        "*🛠️ Admin Help*",
        "",
        "🔐 *Operasional Order*",
        "• ✅ *konfirmasi <ID> <nomor>* — tandai PAID & hapus pesan QR",
        "  Contoh: `konfirmasi VS-AB12CD 6281234567890`",
        "• 📦 *send <ID> <nomor> <payload>* — kirim manual (keyed/pipe, pisah multi pakai `||`)",
        "  Contoh: `send VS-AB12CD 62812 email:a@b|password:p||email:c@d|password:q|pin:1234`",
        "• 📦 *sendstock <ID|orderID>|<nomor>|<jumlah>* — ambil dari pool & kirim",
        "  Contoh: `sendstock net1u|62812|2` atau `sendstock VS-AB12CD|62812|1`",
        "• ⛔ *batal <ID> <nomor> [alasan]* — batalkan (stok balik bila perlu)",
        "• 💵 *refund <ID> <nomor> [alasan]* — tandai REFUNDED",
        "",
        "📋 *List & Detail*",
        "• 🧾 */orders [status|all] [n]* — list order (admin)",
        "  Contoh: `/orders paid 20` atau `/orders all 50`",
        "• 🧾 */orders user <nomor> [n]* — order per user",
        "  Contoh: `/orders user 62812 30`",
        "• 🧾 */order <ID>* — detail satu order (admin)",
        "• 🗂️ *cekpending [n|all]* — antrian PAID dari yang paling lama",
        "",
        "📦 *Produk & Stok*",
        "• addprod id|title|category|price|discount|stock|sold|desc1;desc2",
        "• setharga id|price|discount",
        "• setstok id|qty   (boleh `kosong`=0)",
        "• settitle id|title   • setcat id|category",
        "• setdesc id|desc1;desc2;...",
        "• setrules id|rules default   • setnotes id|notes default",
        "• setcost id|cost",
        "• delprod id   (konfirmasi: `ya id`)",
        "• addstock id <baris>  /  addstockmulti id <b1>||<b2>",
        "• stock id  — jumlah & sample",
        "• cekallstock id — tampilkan *semua* akun lengkap (hati-hati panjang)",
        "• syncstock id  /  syncstock all",
        "",
        "📊 *Laporan*",
        "• report today | month | all",
        "• report range YYYY-MM-DD YYYY-MM-DD",
        "",
        "📣 *Broadcast*",
        "• broadcast <all|buyers|nobuy> | <pesan>",
        "• broadcastimg <all|buyers|nobuy> | <urlGambar> | <caption?>",
        "• broadcastvid <all|buyers|nobuy> | <urlVideo> | <caption?>",
        "",
        "🟢🔴 *Toko*",
        "• open  /  close",
        "",
        "💾 *Backup pCloud*",
        "• backup status",
        "• backup on / off",
        "• backup interval daily|weekly",
        "• backup hour <0-23>",
        "• backup folder <path>",
        "• setpcloud token <ACCESS_TOKEN>",
        "• setpcloud folder <path>",
        "• testpcloud",
        "• backup now"
    ].join("\n");
}

// ===== summaries =====
function summarizeOrderItems(items) {
    return items
        .map(i => {
            const p = productById(i.id);
            const price = p ? finalPrice(p.price, p.discount) : 0;
            const sub = price * i.qty;
            return `• *${p?.title || i.id}* x${i.qty} = ${formatPrice(sub)}`;
        })
        .join("\n");
}
function summarizeItemsOneLine(items) {
    return items
        .map(i => {
            const p = productById(i.id);
            return `${p?.title || i.id} x${i.qty}`;
        })
        .join(", ");
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
                db.write().catch(() => {});
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
const FIELD_ORDER = [
    "email",
    "password",
    "pin",
    "profil",
    "durasi",
    "ops1",
    "ops2",
    "ops3",
    "notes",
    "rules"
];
const LABELS = {
    email: "Email",
    user: "Email",
    username: "Email",
    password: "Password",
    pass: "Password",
    pin: "PIN",
    profil: "Profil",
    profile: "Profil",
    durasi: "Durasi",
    validity: "Durasi",
    ops1: "Opsional 1",
    ops2: "Opsional 2",
    ops3: "Opsional 3",
    notes: "Catatan",
    note: "Catatan",
    rules: "Rules",
    "2fa": "2FA",
    keypass: "Keypass",
    license: "License",
    code: "Code",
    key: "Key"
};
const NORMALIZE = {
    user: "email",
    username: "email",
    pass: "password",
    profile: "profil",
    validity: "durasi",
    note: "notes"
};

function stockFile(id) {
    return path.join(stockDir, `${id.toLowerCase()}.json`);
}
function readStock(id) {
    const f = stockFile(id);
    if (!fs.existsSync(f)) fs.writeFileSync(f, "[]");
    try {
        return JSON.parse(fs.readFileSync(f, "utf8"));
    } catch {
        return [];
    }
}
function writeStock(id, arr) {
    fs.writeFileSync(stockFile(id), JSON.stringify(arr, null, 2));
}

function normalizeFields(obj) {
    const out = {};
    for (const [k, v] of Object.entries(obj || {})) {
        const kk = (NORMALIZE[k] || k).toLowerCase();
        if (out[kk] == null && String(v || "").trim())
            out[kk] = String(v).trim();
    }
    return out;
}
function parseStockPayload(payload) {
    const s = String(payload || "").trim();
    if (!s) return null;
    if (s.includes(":")) {
        const obj = {};
        s.split("|").forEach(seg => {
            const t = seg.replace(/\r/g, "").trim();
            if (!t) return;
            const i = t.indexOf(":");
            if (i === -1) return;
            const k = t.slice(0, i).trim().toLowerCase();
            const v = t.slice(i + 1).trim();
            if (k) obj[k] = v;
        });
        return normalizeFields(obj);
    } else {
        const parts = s.split("|").map(x => x.trim());
        const obj = {};
        for (let i = 0; i < Math.min(parts.length, FIELD_ORDER.length); i++)
            if (parts[i]) obj[FIELD_ORDER[i]] = parts[i];
        return normalizeFields(obj);
    }
}
function stockCount(id) {
    return readStock(id).length;
}
function addStock(id, payload) {
    const obj = parseStockPayload(payload);
    if (!obj) return { ok: false, err: "Format kosong/tidak valid" };
    // tidak wajib email & password — bisa license/code saja
    const list = readStock(id);
    list.push(obj);
    writeStock(id, list);
    return { ok: true, left: list.length };
}
function addStockMulti(id, multi) {
    const rows = String(multi || "")
        .split("||")
        .map(s => s.trim())
        .filter(Boolean);
    let ok = 0,
        fail = 0;
    for (const r of rows) {
        const res = addStock(id, r);
        if (res.ok) ok++;
        else fail++;
    }
    return { ok, fail, left: stockCount(id) };
}
function takeStock(id, qty) {
    const list = readStock(id);
    if (list.length < qty) return null;
    const picked = list.splice(0, qty);
    writeStock(id, list);
    return picked;
}
function maskEmail(e) {
    const [u, d] = String(e || "").split("@");
    if (!d) return e;
    const u2 =
        u.length <= 2
            ? u[0] + "*"
            : u[0] + "*".repeat(Math.max(1, u.length - 2)) + u.slice(-1);
    return `${u2}@${d}`;
}
function formatAccountBlock(fields, idx, prod) {
    const sk = new Set(Object.keys(fields || {}));
    const keys = Array.from(sk);
    const exclude = new Set(["notes", "rules"]);
    const priority = [
        "email",
        "password",
        "pin",
        "profil",
        "durasi",
        "license",
        "code",
        "key",
        "ops1",
        "ops2",
        "ops3",
        "2fa",
        "keypass"
    ];
    keys.sort(
        (a, b) =>
            (priority.indexOf(a) >= 0 ? priority.indexOf(a) : 999) -
            (priority.indexOf(b) >= 0 ? priority.indexOf(b) : 999)
    );
    const lines = keys
        .filter(k => fields[k] && !exclude.has(k))
        .map(k => `- ${LABELS[k] || capitalize(k)}: ${fields[k]}`);

    const notesToShow =
        fields.notes != null ? fields.notes : prod?.defaultNotes || "";
    // rules DIKIRIM TERPISAH

    const blockLines = [
        "📝 *Detail Akun:*",
        lines.length ? lines.join("\n") : "-"
    ];
    if (notesToShow) blockLines.push("\n*🗒️ Catatan:*\n" + notesToShow);
    return blockLines.join("\n");
}

// ===== captions =====
function buildPaymentCaption(order, itemsText) {
    const base = formatPrice(order.total),
        expect = formatPrice(order.expectedAmount);
    const deadline =
        EXPIRE_HOURS > 0
            ? `⏱️ Batas waktu: ${EXPIRE_HOURS} jam (otomatis batal jika lewat)`
            : null;
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
    ]
        .join("\n")
        .trim();
}

// ===== rate limit =====
function allowProcess(u) {
    const t = Date.now();
    if (RATE_LIMIT_MS <= 0) {
        u.lastTs = t;
        return true;
    }
    if (t - (u.lastTs || 0) < RATE_LIMIT_MS) return false;
    u.lastTs = t;
    return true;
}

// ===== user stats =====
function addUserStatsForOrderPaid(order) {
    const u = db.data.users[jidNormalizedUser(order.userJid)];
    if (!u) return;
    ensureUserShape(u);
    u.stats.orders = (u.stats.orders || 0) + 1;
    u.stats.paid = (u.stats.paid || 0) + 1;
    u.stats.totalSpent = (u.stats.totalSpent || 0) + Number(order.total || 0);
    u.productCount ||= {};
    for (const it of order.items)
        u.productCount[it.id] = (u.productCount[it.id] || 0) + it.qty;
}
function addUserStatsForOrderSent(order) {
    const u = db.data.users[jidNormalizedUser(order.userJid)];
    if (!u) return;
    ensureUserShape(u);
    u.stats.sent = (u.stats.sent || 0) + 1;
}
function addUserStatsForOrderRefunded(order) {
    const u = db.data.users[jidNormalizedUser(order.userJid)];
    if (!u) return;
    ensureUserShape(u);
    u.stats.refunded = (u.stats.refunded || 0) + 1;
}
function addUserStatsForOrderCanceled(order) {
    const u = db.data.users[jidNormalizedUser(order.userJid)];
    if (!u) return;
    ensureUserShape(u);
    u.stats.canceled = (u.stats.canceled || 0) + 1;
}

// ===== expiry =====
async function checkExpiry(sendFunc) {
    if (EXPIRE_HOURS <= 0) return;
    const limit = EXPIRE_HOURS * 3600 * 1000,
        nowTs = Date.now();
    let changed = false;
    for (const o of db.data.orders) {
        if (o.status === "NEW" && nowTs - o.createdAt > limit) {
            o.status = "EXPIRED";
            releaseStock(o);
            changed = true;
            try {
                await sendFunc(
                    o.userJid,
                    `⏰ Order *${
                        o.id
                    }* sudah *expired*. Silakan buat order baru.\n🕒 ${formatTs(
                        nowTs
                    )}`
                );
            } catch {}
            audit(`EXPIRE ${o.id}`);
        }
    }
    if (changed) await db.write();
}

// ===== sock helpers =====
let currentSock = null;
function setCurrentSock(sock) {
    currentSock = sock;
}
function getCurrentSock() {
    return currentSock;
}
async function safeSendText(jid, text) {
    const sock = getCurrentSock();
    if (!sock) throw new Error("Socket not ready");
    return sock.sendMessage(jid, { text: clampText(text, 4000) });
}
async function safeDelete(jid, key) {
    const sock = getCurrentSock();
    if (!sock) throw new Error("Socket not ready");
    return sock.sendMessage(jid, { delete: key });
}
async function safeSendImage(jid, buffer, caption) {
    const sock = getCurrentSock();
    if (!sock) throw new Error("Socket not ready");
    return sock.sendMessage(jid, {
        image: buffer,
        caption: clampText(caption, 4000)
    });
}
async function sendLongText(jid, text) {
    const chunk = 3500;
    for (let i = 0; i < text.length; i += chunk)
        await safeSendText(jid, text.slice(i, i + chunk));
}

// ==== BROADCAST HELPERS ====
function pickAudience(segment) {
    segment = (segment || "all").toLowerCase();
    const out = [];
    for (const [jid, u] of Object.entries(db.data.users || {})) {
        if (!/@s\.whatsapp\.net$/.test(jid)) continue;
        if (OWNER && cleanNumber(jid) === OWNER) continue; // skip owner
        if (u?.banned) continue; // skip banned
        const paid = Number(u?.stats?.paid || 0);
        if (segment === "buyers" && paid <= 0) continue;
        if (segment === "nobuy" && paid > 0) continue;
        out.push(jid);
    }
    return out;
}
async function fetchAsBuffer(url) {
    const res = await axios.get(url, { responseType: "arraybuffer" });
    const ct = (res.headers["content-type"] || "").toLowerCase();
    return { buffer: Buffer.from(res.data), contentType: ct };
}
async function broadcastSequential(jids, sendFn, { delayMs = 600 } = {}) {
    let ok = 0, fail = 0;
    for (const j of jids) {
        try { await sendFn(j); ok++; } catch { fail++; }
        await new Promise(r => setTimeout(r, delayMs));
    }
    return { ok, fail, total: jids.length };
}

async function tryCleanupOldPaymentMessages() {
    const list = [...(db.data.orders || [])].slice(-100).reverse();
    for (const o of list)
        if (
            o.paymentMsgKey &&
            ["PAID", "SENT", "CANCELED", "REFUNDED", "EXPIRED"].includes(
                o.status
            )
        ) {
            try {
                await safeDelete(o.userJid, o.paymentMsgKey);
                o.paymentMsgKey = null;
                await db.write();
            } catch {}
        }
}

// ===== webhook & monitor =====
let webhookStarted = false;
function countOrderStatus(list) {
    const acc = {};
    for (const o of list) acc[o.status] = (acc[o.status] || 0) + 1;
    return acc;
}
async function buildMonitorText() {
    try {
        await db.read();
    } catch {}
    const mem = process.memoryUsage();
    const orders = db.data.orders || [];
    const stats = countOrderStatus(orders);
    const lines = [
        `*📊 Monitor — ${STORE_NAME}*`,
        `Uptime: ${Math.floor(process.uptime() / 3600)}h ${Math.floor(
            (process.uptime() % 3600) / 60
        )}m`,
        `Node: ${process.version}`,
        `CPU (1/5/15m): ${os
            .loadavg()
            .map(n => n.toFixed(2))
            .join(", ")}`,
        `Mem RSS: ${(mem.rss / 1024 / 1024).toFixed(1)} MB | Heap: ${(
            mem.heapUsed /
            1024 /
            1024
        ).toFixed(1)} MB`,
        "",
        `Products: ${PRODUCTS.length}`,
        `Users: ${Object.keys(db.data.users || {}).length}`,
        `Orders: ${orders.length} | ${
            Object.entries(stats)
                .map(([k, v]) => `${k}=${v}`)
                .join(", ") || "-"
        }`,
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
                const m = text
                    .replace(/[\u00A0]/g, " ")
                    .match(/Rp\s*([\d\.]+)/i);
                if (!m) return null;
                const num = Number(m[1].replace(/\./g, ""));
                return Number.isFinite(num) ? num : null;
            })();
            if (!amount)
                return res.status(200).json({ ok: true, info: "no-amount" });

            const nowTs = Date.now(),
                ttl = 24 * 3600 * 1000;
            const order = (db.data.orders || []).find(
                o =>
                    (o.status === "NEW" || o.status === "AWAIT_CONFIRM") &&
                    o.expectedAmount === amount &&
                    nowTs - (o.createdAt || 0) < ttl
            );
            if (!order)
                return res.status(200).json({ ok: true, info: "no-order" });

            if (order.status !== "PAID") {
                order.status = "PAID";
                order.confirmedAt = now();
                order.paidAt = order.paidAt || now();
                await db.write();
                audit(`AUTO-PAID ${order.id} amount=${amount}`);
            }
            try {
                if (order.paymentMsgKey) {
                    await safeDelete(order.userJid, order.paymentMsgKey);
                    order.paymentMsgKey = null;
                    await db.write();
                }
            } catch {}

            try {
                await safeSendText(
                    order.userJid,
                    [
                        "✅ *Pembayaran diterima!*",
                        `*ID:* ${order.id}`,
                        `*Item:* ${summarizeItemsOneLine(order.items)}`,
                        `*Nominal:* ${formatPrice(order.expectedAmount)}`,
                        `🕒 ${formatTs(order.paidAt)}`,
                        "",
                        AUTO_SEND
                            ? "⏳ Pesanan akan dikirim otomatis segera..."
                            : "Silakan tunggu admin memproses pesanan ya 🙏"
                    ].join("\n")
                );
            } catch {}

            try {
                const u = getUser(order.userJid);
                addUserStatsForOrderPaid(order);
                await db.write();
                if (OWNER) {
                    await safeSendText(
                        OWNER + "@s.whatsapp.net",
                        [
                            "💸 *Auto-verify pembayaran diterima*",
                            `*ID:* ${order.id}`,
                            `*User:* ${cleanNumber(order.userJid)} (${
                                u.name || "-"
                            })`,
                            `*Nominal:* ${formatPrice(order.expectedAmount)}`,
                            `🕒 ${formatTs(order.paidAt)}`
                        ].join("\n")
                    );
                }
            } catch {}

            if (AUTO_SEND) {
                try {
                    await maybeAutoSend(order);
                } catch (e) {
                    console.error("autoSend error:", e);
                }
            }

            res.json({ ok: true, matched: order.id, amount });
        } catch (e) {
            console.error("webhook error:", e);
            res.status(200).json({
                ok: false,
                info: "caught-error",
                error: String(e?.message || e)
            });
        }
    });

    app.get("/health", async (_req, res) => {
        try {
            await db.read();
        } catch {}
        const mem = process.memoryUsage();
        const orders = db.data.orders || [];
        const stats = countOrderStatus(orders);
        res.json({
            ok: true,
            time: new Date().toISOString(),
            uptime_s: Math.round(process.uptime()),
            node: process.version,
            cpu_load: os.loadavg(),
            mem_rss: mem.rss,
            mem_heapUsed: mem.heapUsed,
            products: PRODUCTS.length,
            users: Object.keys(db.data.users || {}).length,
            orders_total: orders.length,
            orders_status: stats,
            auto_send: AUTO_SEND,
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
            console.log(
                `Endpoint: POST /api/transactions/pending | GET /health | GET /monitor`
            );
        });
        server.on("error", err => {
            if (err && err.code === "EADDRINUSE")
                console.log(
                    `⚠️ Port ${WEBHOOK_PORT} sudah dipakai. Webhook tidak akan diduplikasi.`
                );
            else console.error("Webhook server error:", err);
        });
    } catch (err) {
        if (err && err.code === "EADDRINUSE")
            console.log(
                `⚠️ Port ${WEBHOOK_PORT} sudah dipakai. Webhook tidak akan diduplikasi.`
            );
        else console.error("Webhook start error:", err);
    }
}

// ===== pengiriman (rules pesan terpisah) =====
async function sendOrderDeliveryMessages(order, prod, fields, idx) {
    // 1) Pesan header + detail akun (TANPA rules)
    const header = [
        "📢 *PENGIRIMAN PESANAN*",
        "",
        "Hai Kak,",
        `Pesanan Kakak untuk *${summarizeItemsOneLine(
            order.items
        )}* telah kami proses. Detail pesanan sebagai berikut:`,
        "",
        formatAccountBlock(fields, idx, prod),
        "",
        "Silakan gunakan detail ini untuk mengakses produk.",
        "",
        `Terima kasih telah berbelanja di *${STORE_NAME}* 👾`,
        "",
        `*ID Order:* ${order.id}`,
        `🕒 ${formatTs(now())}`
    ].join("\n");
    await safeSendText(order.userJid, header);

    // 2) Jika ada RULES — kirim di pesan terpisah
    const rulesToShow = (
        fields.rules != null ? fields.rules : prod?.defaultRules || ""
    ).trim();
    if (rulesToShow) {
        await safeSendText(order.userJid, "*📏 Rules:*\n" + rulesToShow);
    }
}

// ===== AUTO SEND =====
async function maybeAutoSend(order) {
    if (!AUTO_SEND || !order || order.status !== "PAID") return;
    const bundles = [];
    for (const it of order.items) {
        const creds = takeStock(it.id, it.qty);
        if (!creds) {
            if (OWNER) {
                try {
                    await safeSendText(
                        OWNER + "@s.whatsapp.net",
                        `⚠️ Auto-send *gagal* untuk *${
                            order.id
                        }* — stok akun *${
                            it.id
                        }* kurang. Perlu kirim manual.\nUser: ${cleanNumber(
                            order.userJid
                        )}\nButuh: ${it.qty}, Tersisa: ${stockCount(it.id)}`
                    );
                } catch {}
            }
            try {
                await safeSendText(
                    order.userJid,
                    "⚠️ Stok akun sedang kosong. Admin akan mengirimkan pesanan kamu sesegera mungkin ya 🙏"
                );
            } catch {}
            return;
        }
        bundles.push({ prodId: it.id, creds });
    }

    order.delivered ||= [];
    let idx = 1;
    for (const b of bundles) {
        const prod = productById(b.prodId);
        for (const fields of b.creds) {
            try {
                await sendOrderDeliveryMessages(order, prod, fields, idx++);
                order.delivered.push({ at: now(), prodId: b.prodId, fields });
            } catch {}
        }
    }
    await db.write();

    for (const it of order.items) {
        const p = productById(it.id);
        if (p) p.sold = Number(p.sold || 0) + Number(it.qty || 0);
    }
    saveProducts();

    order.status = "SENT";
    order.sentAt = now();
    await db.write();
    audit(`AUTO-SENT ${order.id}`);
    addUserStatsForOrderSent(order);
    await db.write();

    if (OWNER) {
        try {
            await safeSendText(
                OWNER + "@s.whatsapp.net",
                `✅ Auto-send selesai untuk *${
                    order.id
                }* (${summarizeItemsOneLine(order.items)}) — ${formatTs(
                    order.sentAt
                )}`
            );
        } catch {}
    }
}

// ===== BACKUP pCloud =====
async function pcloudUpload(localFilePath, remoteFolderPath, token) {
    const url = `https://api.pcloud.com/uploadfile?path=${encodeURIComponent(
        remoteFolderPath
    )}&access_token=${encodeURIComponent(token)}`;

    const form = new FormData();
    form.append("filename", fs.createReadStream(localFilePath));

    try {
        const res = await axios.post(url, form, {
            headers: form.getHeaders(),
            maxContentLength: Infinity,
            maxBodyLength: Infinity
        });
        if (!res.data || res.data.result !== 0) {
            throw new Error("pCloud upload gagal: " + JSON.stringify(res.data));
        }
        return res.data;
    } catch (err) {
        throw new Error("pCloud upload error: " + err.message);
    }
}

async function pcloudUserInfo(token) {
    const url = `https://api.pcloud.com/userinfo?access_token=${encodeURIComponent(
        token
    )}`;
    try {
        const res = await axios.get(url);
        return res.data;
    } catch (err) {
        throw new Error("pCloud userinfo error: " + err.message);
    }
}
async function createBackupZip() {
    if (!fs.existsSync(backupsDir))
        fs.mkdirSync(backupsDir, { recursive: true });
    const ts = new Date().toISOString().replace(/[:.]/g, "-");
    const fileName = `backup-${ts}.zip`;
    const filePath = path.join(backupsDir, fileName);

    await new Promise((resolve, reject) => {
        const output = fs.createWriteStream(filePath);
        const zip = archiver("zip", { zlib: { level: 9 } });
        output.on("close", resolve);
        zip.on("error", reject);
        zip.pipe(output);
        zip.file(path.join(dataDir, "db.json"), { name: "db.json" });
        zip.file(productsPath, { name: "products.json" });
        zip.directory(stockDir, "stock"); // sekalian dump stok pool
        zip.finalize();
    });

    return filePath;
}

async function runBackup(reason = "manual") {
    try {
        const conf = db.data.settings.backup || {};
        if (!conf.enabled) return false;
        const zipFile = await createBackupZip();
        if (conf.pcloud?.token) {
            await pcloudUpload(
                zipFile,
                conf.pcloud.folder || "/VienzeStoreBackups",
                conf.pcloud.token
            );
        }
        db.data.settings.backup.lastRunDay = todayStr();
        await db.write();
        console.log(`💾 Backup selesai (${reason})`);
        return true;
    } catch (e) {
        console.error("Backup error:", e);
        return false;
    }
}
function isItBackupTime() {
    const b = db.data.settings.backup || {};
    const hourWIB = Number(
        new Date().toLocaleString("en-US", {
            timeZone: "Asia/Jakarta",
            hour12: false,
            hour: "2-digit"
        })
    );
    const today = todayStr();
    if (!b.enabled) return false;
    if (Number(b.hour) !== hourWIB) return false;
    if (b.lastRunDay === today) return false;
    if ((b.interval || "daily") === "weekly") {
        const day = new Date().toLocaleString("en-US", {
            timeZone: "Asia/Jakarta",
            weekday: "short"
        });
        if (!/^Mon$/i.test(day)) return false;
    }
    return true;
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
        auth: {
            creds: state.creds,
            keys: makeCacheableSignalKeyStore(
                state.keys,
                pino({ level: "silent" })
            )
        },
        markOnlineOnConnect: false
    });

    setCurrentSock(sock);

    async function requestPairingCodeWithRetry() {
        if (!USE_PAIRING || sock.authState.creds.registered) return;
        let n = (BOT_NUMBER || "").replace(/[^0-9]/g, "");
        if (!n) {
            console.log("❗ BOT_NUMBER belum valid. Contoh: 62xxxxxxxxxxx");
            return;
        }
        if (n.startsWith("0")) n = "62" + n.slice(1);
        if (!n.startsWith("62")) n = "62" + n;
        for (let attempt = 1; attempt <= 5; attempt++) {
            try {
                const code = await sock.requestPairingCode(n);
                console.log(
                    `\n👉 Pairing Code: ${code}\nBuka WhatsApp > Perangkat Tautan > Tautkan dengan nomor telepon\n`
                );
                break;
            } catch (e) {
                const msg =
                    e?.output?.payload?.message || e.message || String(e);
                console.log(`Gagal pairing (attempt ${attempt}): ${msg}`);
                if (attempt === 5)
                    console.log(
                        "❌ Coba: rm -rf auth && npm start (atau cek koneksi/nomor)."
                    );
                else await new Promise(r => setTimeout(r, 1000 * attempt));
            }
        }
    }
    requestPairingCodeWithRetry().catch(() => {});

    function safeReconnect(delayMs = 1500) {
        if (reconnectTimer) return;
        reconnectTimer = setTimeout(() => {
            reconnectTimer = null;
            console.log("🔁 Reconnecting…");
            startSock().catch(err =>
                console.error("reconnect start error:", err)
            );
        }, delayMs);
    }

    sock.ev.on("creds.update", async () => {
        try {
            await saveCreds();
        } catch (e) {
            if (e && e.code === "ENOENT") {
                try {
                    fs.mkdirSync("./auth", { recursive: true });
                } catch {}
                await saveCreds();
            } else console.error("saveCreds error:", e);
        }
    });

    sock.ev.on("connection.update", async u => {
        const { connection, lastDisconnect, qr, pairingCode } = u;
        if (pairingCode && USE_PAIRING)
            console.log(
                `\n👉 Pairing Code: ${pairingCode}\nBuka WhatsApp > Perangkat Tautan > Tautkan dengan nomor telepon\n`
            );
        if (qr && !USE_PAIRING) {
            console.log("\nScan QR ini dari WhatsApp:");
            try {
                qrcodeTerminal.generate(qr, { small: true });
            } catch {}
        }
        if (connection === "open") {
            console.log("✅ Connected.");
            setCurrentSock(sock);
            try {
                await tryCleanupOldPaymentMessages();
            } catch {}
        }
        if (connection === "close") {
            const err = lastDisconnect?.error;
            const status = err?.output?.statusCode;
            const reason =
                err?.output?.payload?.message ||
                err?.message ||
                `Code ${status || "unknown"}`;
            console.log("❌ Disconnected:", reason);
            const isLoggedOut = Number(status) === 401;
            if (isLoggedOut) {
                console.log(
                    "🚪 Session logged out / belum tertaut. Reset auth & reconnect…"
                );
                try {
                    fs.rmSync("./auth", { recursive: true, force: true });
                    fs.mkdirSync("./auth", { recursive: true });
                } catch {}
                safeReconnect(1200);
                return;
            }
            if (USE_PAIRING && !sock.authState.creds.registered)
                setTimeout(
                    () => requestPairingCodeWithRetry().catch(() => {}),
                    1200
                );
            safeReconnect(1500);
        }
    });

    // start web server
    startPaymentWebhook();

    const send = (jid, text) => safeSendText(jid, text);
    const sendImage = (jid, buf, cap) => safeSendImage(jid, buf, cap);

    // expiry checker
    setInterval(() => checkExpiry(send), 5 * 60 * 1000);

    // auto-backup scheduler (cek tiap 10 menit)
    setInterval(
        async () => {
            try {
                if (isItBackupTime()) await runBackup("scheduled");
            } catch (e) {
                console.error("scheduled backup error:", e);
            }
        },
        10 * 60 * 1000
    );

    // ===== message handler =====
    sock.ev.on("messages.upsert", async mUp => {
        try {
            const m = mUp.messages?.[0];
            if (!m || !m.message || m.key.fromMe) return;
            const jid = m.key.remoteJid;
            if (jid.endsWith("@g.us")) return;

            const sender = jidNormalizedUser(m.key.participant || jid);
            const isAdmin = isAdminJid(sender);
            const user = getUser(sender);
            const firstTime = !user._welcomed;
            user._welcomed = true;

            const body =
                m.message.conversation ||
                m.message.extendedTextMessage?.text ||
                m.message.imageMessage?.caption ||
                m.message.videoMessage?.caption ||
                "";
            let text = (body || "").trim();
            if (!text) return;
            if (text.length > MAX_MSG_CHARS)
                text = text.slice(0, MAX_MSG_CHARS);
            if (user.banned) {
                await send(jid, "🚫 Akses dibatasi. Hubungi admin.");
                return;
            }
            if (!allowProcess(user)) return;

            if (firstTime) {
                await db.write();
                await send(
                    jid,
                    [
                        `👋 *Selamat datang di ${STORE_NAME}!*`,
                        "Ketik *menu* untuk melihat produk, atau *help* untuk panduan lengkap.",
                        ""
                    ].join("\n")
                );
            }

            const lower = text.toLowerCase();

            // help & menu
            if (["help", "/help"].includes(lower)) {
                await send(jid, userHelpFull());
                return;
            }
            if (["menu", "/menu", "/start", "start"].includes(lower)) {
                await send(jid, prettyMenu());
                return;
            }

            if (lower === "adminhelp" || lower === "adminhelp@") {
                if (!isAdmin) {
                    await send(jid, "Perintah khusus admin.");
                    return;
                }
                await send(jid, adminHelp());
                return;
            }
            if (isAdmin && lower === "open") {
                db.data.settings.storeOpen = true;
                await db.write();
                await send(jid, "🟢 Toko *OPEN*.");
                return;
            }
            if (isAdmin && lower === "close") {
                db.data.settings.storeOpen = false;
                await db.write();
                await send(jid, "🔴 Toko *CLOSED*.");
                return;
            }

            if (lower === "ping") {
                await send(jid, "pong");
                return;
            }
            if (["cs", "kontak", "contact", "support"].includes(lower)) {
                await send(
                    jid,
                    `👨‍💻 *Customer Support*\n${CS_CONTACT}\n🕒 ${formatTs(
                        now()
                    )}`
                );
                return;
            }

            // myinfo
            if (lower === "myinfo") {
                const stats = user.stats || {};
                const pc = user.productCount || {};
                const top = Object.entries(pc)
                    .sort((a, b) => (b[1] || 0) - (a[1] || 0))
                    .slice(0, 3)
                    .map(([id, c]) => {
                        const p = productById(id);
                        return `• ${p?.title || id} x${c}`;
                    });
                await send(
                    jid,
                    [
                        "*👤 Info Akun Kamu*",
                        `• Nama: ${user.name || "Customer"}`,
                        `• Bergabung: ${formatTs(user.joinedAt)}`,
                        "",
                        "*📈 Statistik*",
                        `• Order (PAID): ${stats.paid || 0}`,
                        `• Order terkirim (SENT): ${stats.sent || 0}`,
                        `• Refund: ${stats.refunded || 0}`,
                        `• Canceled: ${stats.canceled || 0}`,
                        `• Total dibelanjakan: ${formatPrice(
                            stats.totalSpent || 0
                        )}`,
                        "",
                        "*🏆 Top produk:*",
                        top.length ? top.join("\n") : "-"
                    ].join("\n")
                );
                return;
            }

            // cekorder
            if (lower.startsWith("cekorder ")) {
                const id = text.split(/\s+/)[1];
                const o = db.data.orders.find(
                    x => x.id === id && x.userJid === sender
                );
                if (!o) {
                    await send(jid, "⚠️ Order tidak ditemukan.");
                    return;
                }
                const itemsText = summarizeOrderItems(o.items);
                const head = [
                    `🧾 *Detail Order*`,
                    `1) *ID:* ${o.id}  ${statusEmoji(o.status)} *${o.status}*`,
                    `2) *Item:*`,
                    itemsText,
                    "—",
                    `3) *Total:* ${formatPrice(o.total)}`,
                    o.expectedAmount
                        ? `   *Tagihan:* ${formatPrice(o.expectedAmount)}`
                        : null,
                    "",
                    `🕒 *Waktu:*`,
                    `• Dibuat: ${formatTs(o.createdAt)}`,
                    o.paidAt ? `• Dibayar: ${formatTs(o.paidAt)}` : null,
                    o.confirmedAt
                        ? `• Dikonfirmasi: ${formatTs(o.confirmedAt)}`
                        : null,
                    o.sentAt ? `• Dikirim: ${formatTs(o.sentAt)}` : null,
                    o.canceledAt
                        ? `• Dibatalkan: ${formatTs(o.canceledAt)}` : null,
                    o.refundedAt ? `• Refund: ${formatTs(o.refundedAt)}` : null
                ]
                    .filter(Boolean)
                    .join("\n");

                if (o.delivered && o.delivered.length) {
                    const blocks = o.delivered
                        .map((d, i) => {
                            const prod = productById(d.prodId);
                            const msg = formatAccountBlock(
                                d.fields || {},
                                i + 1,
                                prod
                            );
                            const rules = (
                                d.fields?.rules ??
                                prod?.defaultRules ??
                                ""
                            ).trim();
                            return (
                                msg + (rules ? "\n\n*📏 Rules:*\n" + rules : "")
                            );
                        })
                        .join("\n\n");
                    await sendLongText(
                        jid,
                        head +
                            "\n\n" +
                            "*📦 Riwayat Pengiriman Akun:*\n" +
                            blocks
                    );
                } else {
                    await send(jid, head);
                }
                return;
            }

            // orders (USER)
            if (lower === "orders") {
                const list = [...db.data.orders]
                    .filter(o => o.userJid === sender)
                    .reverse()
                    .slice(0, 10);
                if (!list.length) {
                    await send(jid, "Belum ada order.");
                    return;
                }
                const out = list
                    .map((o, i) =>
                        [
                            `${i + 1}. ${statusEmoji(o.status)} *${o.id}*`,
                            `   • *Item:* ${summarizeItemsOneLine(o.items)}`,
                            `   • *Tagihan:* ${formatPrice(
                                o.expectedAmount || o.total
                            )}`,
                            `   🕒 ${formatTs(o.createdAt)}`
                        ].join("\n")
                    )
                    .join("\n\n");
                await send(
                    jid,
                    `🧾 *Riwayat Order Kamu*\n\n${out}\n\n*Legenda:*\n${STATUS_LEGEND}`
                );
                return;
            }

            // beli
            if (lower.startsWith("beli ")) {
                if (!db.data.settings.storeOpen) {
                    await send(
                        jid,
                        "🛑 Maaf, toko sedang *CLOSED*. Coba lagi nanti ya."
                    );
                    return;
                }
                const parts = text.split(/\s+/);
                const code = parts[1];
                const qty = Math.max(1, Number(parts[2] || "1") || 1);
                const p = productById(code);
                if (!p) {
                    await send(
                        jid,
                        "⚠️ Kode produk tidak ditemukan. Ketik *menu* untuk melihat daftar."
                    );
                    return;
                }
if (Number(p.stock || 0) < qty) {
                    await send(
                        jid,
                        `⚠️ Stok tidak cukup. Sisa: ${stokLabel(p.stock)}.`
                    );
                    return;
                }

                const orderId = makeOrderId();
                const items = [{ id: p.id, qty }];

                if (!lockStockForOrder(items)) {
                    await send(
                        jid,
                        `⚠️ Stok berubah. Coba ulang, sisa: ${
                            productById(p.id)?.stock ?? 0
                        }.`
                    );
                    return;
                }

                const total = items.reduce((acc, it) => {
                    const _p = productById(it.id);
                    const price = _p ? finalPrice(_p.price, _p.discount) : 0;
                    return acc + price * it.qty;
                }, 0);

                // pilih kode unik yang belum dipakai order lain (NEW/AWAIT)
                const taken = new Set(
                    db.data.orders
                        .filter(
                            o =>
                                (o.status === "NEW" ||
                                    o.status === "AWAIT_CONFIRM") &&
                                o.expectedAmount
                        )
                        .map(o => o.expectedAmount)
                );
                let add = Math.floor(Math.random() * 300) + 1,
                    tries = 0;
                while (taken.has(total + add) && tries < 350) {
                    add = (add % 300) + 1;
                    tries++;
                }
                const expectedAmount = total + add;

                const order = {
                    id: orderId,
                    userJid: sender,
                    name: user.name,
                    items,
                    total,
                    uniqueAdd: add,
                    expectedAmount,
                    paymentMsgKey: null,
                    qrisPayload: null,
                    status: "NEW",
                    createdAt: now(),
                    paidAt: null,
                    confirmedAt: null,
                    sentAt: null,
                    canceledAt: null,
                    refundedAt: null,
                    delivered: [],
                    _stockReleased: false
                };
                db.data.orders.push(order);
                await db.write();
                audit(
                    `NEW ${order.id} by ${sender} total=${total} expected=${expectedAmount}`
                );

                const itemsText = summarizeOrderItems(items);
                const caption = buildPaymentCaption(order, itemsText);

                try {
                    if (!QRIS_PAYLOAD_BASE) {
                        const sent = await send(
                            jid,
                            "⚠️ QRIS belum disetel. Hubungi admin.\n\n" + caption
                        );
                        order.paymentMsgKey = sent?.key || null;
                    } else {
                        let payloadToUse = QRIS_PAYLOAD_BASE;
                        if (QRIS_DYNAMIC) {
                            try {
                                payloadToUse = buildDynamicQRIS(
                                    QRIS_PAYLOAD_BASE,
                                    expectedAmount,
                                    order.id
                                );
                                order.qrisPayload = payloadToUse;
                            } catch (e) {
                                console.error("buildDynamicQRIS error:", e);
                            }
                        }
                        const buf =
                            await qrisPngBufferFromPayload(payloadToUse);
                        const sent = await safeSendImage(jid, buf, caption);
                        order.paymentMsgKey = sent?.key || null;
                    }
                    await db.write();
                } catch (e) {
                    console.error("QRIS send error:", e);
                    const sent = await send(
                        jid,
                        caption +
                            "\n\n⚠️ Gagal memuat gambar QR. Minta admin kirim ulang QR."
                    );
                    order.paymentMsgKey = sent?.key || null;
                    await db.write();
                }

                if (OWNER) {
                    await send(
                        OWNER + "@s.whatsapp.net",
                        [
                            "🛒 *Order baru masuk*",
                            `*ID:* ${order.id}`,
                            `*User:* ${cleanNumber(sender)} (${user.name})`,
                            `*Item:* ${summarizeItemsOneLine(items)}`,
                            `*Total:* ${formatPrice(total)}`,
                            `*Tagihan:* ${formatPrice(expectedAmount)}`,
                            `🕒 ${formatTs(order.createdAt)}`,
                            "",
                            `⏳ Menunggu transfer (auto-verify).`
                        ].join("\n")
                    );
                }
                return;
            }

            // bayar
            if (lower.startsWith("bayar ")) {
                const id = text.split(/\s+/)[1];
                const order = db.data.orders.find(
                    o => o.id === id && o.userJid === sender
                );
                if (!order) {
                    await send(jid, "⚠️ ID order tidak ditemukan.");
                    return;
                }
                if (!["NEW", "AWAIT_CONFIRM"].includes(order.status)) {
                    await send(
                        jid,
                        `⚠️ Order *${id}* sudah diproses (status: ${order.status}).`
                    );
                    return;
                }
                order.status = "AWAIT_CONFIRM";
                order.paidAt = now();
                await db.write();
                audit(`AWAIT_CONFIRM ${id} by ${sender}`);

                await send(
                    jid,
                    [
                        "✅ Terima kasih, bukti pembayaran kamu *kami terima*!",
                        `*ID:* ${order.id}`,
                        `*Item:* ${summarizeItemsOneLine(order.items)}`,
                        `🕒 ${formatTs(order.paidAt)}`,
                        "",
                        "Status: *Menunggu verifikasi admin*",
                        "Jika nominal *persis*, biasanya auto-verify segera ✅"
                    ].join("\n")
                );

                if (OWNER) {
                    await send(
                        OWNER + "@s.whatsapp.net",
                        [
                            "💸 *Pembayaran masuk (manual ack)*",
                            `*ID:* ${order.id}`,
                            `*User:* ${cleanNumber(sender)} (${user.name})`,
                            `*Tagihan:* ${formatPrice(
                                order.expectedAmount || order.total
                            )}`,
                            `🕒 ${formatTs(order.paidAt)}`,
                            "",
                            `⚙️ Ketik: *konfirmasi ${order.id} ${cleanNumber(
                                sender
                            )}* bila valid.`
                        ].join("\n")
                    );
                }
                return;
            }

            // ===== ADMIN OPS =====
            if (isAdmin) {
                if (lower === "monitor") {
                    await send(jid, await buildMonitorText());
                    return;
                }

                // /orders variants (admin)
                if (lower.startsWith("/orders")) {
                    const parts = text.trim().split(/\s+/);
                    let mode = parts[1]?.toLowerCase() || "";
                    let limit = Math.max(1, Number(parts[2] || "10") || 10);
                    let list = [...db.data.orders];

                    if (mode === "user") {
                        const num = cleanNumber(parts[2] || "");
                        if (!num) {
                            await send(
                                jid,
                                "Format: /orders user <nomor> [limit]"
                            );
                            return;
                        }
                        limit = Math.max(1, Number(parts[3] || "10") || 10);
                        list = list.filter(o => cleanNumber(o.userJid) === num);
                    } else if (mode && mode !== "all") {
                        const target = mode.toUpperCase();
                        list = list.filter(o => o.status === target);
                    }

                    list = list.slice(-limit).reverse();
                    if (!list.length) {
                        await send(jid, "Tidak ada data.");
                        return;
                    }

                    const out = list
                        .map(o => {
                            const u =
                                db.data.users[jidNormalizedUser(o.userJid)];
                            return `${statusEmoji(o.status)} *${
                                o.id
                            }* — ${cleanNumber(o.userJid)}${
                                u?.name ? ` (${u.name})` : ""
                            } — *${o.status}* — *${formatPrice(
                                o.total
                            )}* — ${formatTs(o.createdAt)}`;
                        })
                        .join("\n");
                    await send(
                        jid,
                        `🧾 *Orders*\n${out}\n\n*Legenda:*\n${STATUS_LEGEND}`
                    );
                    return;
                }

                // /order <id> (admin)
                if (lower.startsWith("/order ")) {
                    const id = text.split(/\s+/)[1];
                    const o = db.data.orders.find(x => x.id === id);
                    if (!o) {
                        await send(jid, "Order tidak ditemukan.");
                        return;
                    }
                    const itemsText = summarizeOrderItems(o.items);
                    const head = [
                        `🧾 *Detail Order (Admin)*`,
                        `• ID: ${o.id}  ${statusEmoji(o.status)} ${o.status}`,
                        `• User: ${cleanNumber(o.userJid)}`,
                        `• Item: ${summarizeItemsOneLine(o.items)}`,
                        `• Total: ${formatPrice(
                            o.total
                        )}  • Tagihan: ${formatPrice(
                            o.expectedAmount || o.total
                        )}`,
                        `• Waktu: dibuat ${formatTs(o.createdAt)}` +
                            (o.paidAt
                                ? ` | dibayar ${formatTs(o.paidAt)}`
                                : "") +
                            (o.sentAt ? ` | dikirim ${formatTs(o.sentAt)}` : "")
                    ].join("\n");

                    if (o.delivered && o.delivered.length) {
                        const blocks = o.delivered
                            .map((d, i) => {
                                const prod = productById(d.prodId);
                                const msg = formatAccountBlock(
                                    d.fields || {},
                                    i + 1,
                                    prod
                                );
                                const rules = (
                                    d.fields?.rules ??
                                    prod?.defaultRules ??
                                    ""
                                ).trim();
                                return (
                                    msg +
                                    (rules ? "\n\n*📏 Rules:*\n" + rules : "")
                                );
                            })
                            .join("\n\n");
                        await sendLongText(jid, head + "\n\n" + blocks);
                    } else {
                        await send(jid, head + "\n\n" + itemsText);
                    }
                    return;
                }

                // antrian pending (PAID terurut paling lama)
                if (lower.startsWith("cekpending")) {
                    const parts = text.trim().split(/\s+/);
                    let limit = 10;
                    if (parts[1]) {
                        limit =
                            parts[1].toLowerCase() === "all"
                                ? 9999
                                : Math.max(1, Number(parts[1]) || 10);
                    }
                    const list = [...db.data.orders]
                        .filter(o => o.status === "PAID")
                        .sort(
                            (a, b) =>
                                (a.paidAt || a.createdAt) -
                                (b.paidAt || b.createdAt)
                        )
                        .slice(0, limit);
                    if (!list.length) {
                        await send(jid, "✅ Tidak ada antrian PAID.");
                        return;
                    }
                    const lines = list
                        .map((o, i) => {
                            const waitMin = Math.max(
                                0,
                                Math.round(
                                    (Date.now() - (o.paidAt || o.createdAt)) /
                                        60000
                                )
                            );
                            return `${i + 1}. ${o.id} — ${cleanNumber(
                                o.userJid
                            )} — ${summarizeItemsOneLine(o.items)} — ${formatTs(
                                o.paidAt || o.createdAt
                            )} — ⏱️ ${waitMin}m`;
                        })
                        .join("\n");
                    await send(
                        jid,
                        `🗂️ *Antrian PAID (lama→baru)*\n${lines}\n\n*Kirim & tandai SENT maka otomatis keluar dari daftar.*`
                    );
                    return;
                }

                if (lower.startsWith("konfirmasi ")) {
                    const [, id, numRaw] = text.split(/\s+/);
                    const num = cleanNumber(numRaw || "");
                    if (!id || !num) {
                        await send(
                            jid,
                            "Format: konfirmasi <idorder> <nomorUser>"
                        );
                        return;
                    }
                    const uj = num + "@s.whatsapp.net";
                    const order = db.data.orders.find(
                        o => o.id === id && o.userJid === uj
                    );
                    if (!order) {
                        await send(
                            jid,
                            "⚠️ Order tidak ditemukan atau nomor tidak cocok."
                        );
                        return;
                    }
                    if (
                        !["AWAIT_CONFIRM", "NEW", "PAID"].includes(order.status)
                    ) {
                        await send(
                            jid,
                            `⚠️ Order ${id} tidak dalam status menunggu (status: ${order.status}).`
                        );
                        return;
                    }

                    order.status = "PAID";
                    order.confirmedAt = now();
                    order.paidAt ||= order.confirmedAt;
                    await db.write();
                    try {
                        if (order.paymentMsgKey) {
                            await safeDelete(uj, order.paymentMsgKey);
                            order.paymentMsgKey = null;
                            await db.write();
                        }
                    } catch {}
                    audit(`PAID ${id} by admin ${sender}`);
                    addUserStatsForOrderPaid(order);
                    await db.write();

                    await send(
                        jid,
                        `✅ Order ${id} ditandai *PAID*.${
                            AUTO_SEND
                                ? " (AUTO_SEND aktif — mencoba kirim...)"
                                : " Lanjut *send* untuk kirim detil."
                        }`
                    );
                    await safeSendText(
                        uj,
                        [
                            "✅ Pembayaran kamu *sudah diverifikasi*!",
                            `*ID:* ${id}`,
                            `*Item:* ${summarizeItemsOneLine(order.items)}`,
                            `🕒 ${formatTs(order.confirmedAt)}`,
                            "",
                            AUTO_SEND
                                ? "⏳ Pesanan akan dikirim otomatis..."
                                : "Kami segera kirim detail akun ya 🙏"
                        ].join("\n")
                    );

                    if (AUTO_SEND) {
                        try {
                            await maybeAutoSend(order);
                        } catch (e) {
                            console.error("autoSend error:", e);
                        }
                    }
                    return;
                }

                if (lower.startsWith("batal ")) {
                    const tok = text.split(/\s+/),
                        id = tok[1],
                        num = cleanNumber(tok[2] || "");
                    const reason =
                        tok.slice(3).join(" ").trim() || "Tanpa keterangan";
                    if (!id || !num) {
                        await send(
                            jid,
                            "Format: batal <idorder> <nomorUser> [alasan]"
                        );
                        return;
                    }
                    const uj = num + "@s.whatsapp.net";
                    const order = db.data.orders.find(
                        o => o.id === id && o.userJid === uj
                    );
                    if (!order) {
                        await send(
                            jid,
                            "⚠️ Order tidak ditemukan atau nomor tidak cocok."
                        );
                        return;
                    }
                    if (
                        ["SENT", "REFUNDED", "CANCELED"].includes(order.status)
                    ) {
                        await send(
                            jid,
                            `⚠️ Order ${id} tidak bisa dibatalkan (status: ${order.status}).`
                        );
                        return;
                    }

                    order.status = "CANCELED";
                    order.canceledAt = now();
                    releaseStock(order);
                    await db.write();
                    audit(`CANCELED ${id} by admin ${sender} reason=${reason}`);
                    addUserStatsForOrderCanceled(order);
                    await db.write();

                    await send(jid, `⛔ Order ${id} *dibatalkan*.`);
                    await safeSendText(
                        uj,
                        [
                            `⛔ *Pesanan dibatalkan*`,
                            `*ID:* ${id}`,
                            `*Item:* ${summarizeItemsOneLine(order.items)}`,
                            `*Alasan:* ${reason}`,
                            `🕒 ${formatTs(order.canceledAt)}`,
                            "",
                            "Jika butuh bantuan, silakan hubungi CS:",
                            CS_CONTACT
                        ].join("\n")
                    );
                    return;
                }

                if (lower.startsWith("refund ")) {
                    const tok = text.split(/\s+/),
                        id = tok[1],
                        num = cleanNumber(tok[2] || "");
                    const reason =
                        tok.slice(3).join(" ").trim() || "Tanpa keterangan";
                    if (!id || !num) {
                        await send(
                            jid,
                            "Format: refund <idorder> <nomorUser> [alasan]"
                        );
                        return;
                    }
                    const uj = num + "@s.whatsapp.net";
                    const order = db.data.orders.find(
                        o => o.id === id && o.userJid === uj
                    );
                    if (!order) {
                        await send(
                            jid,
                            "⚠️ Order tidak ditemukan atau nomor tidak cocok."
                        );
                        return;
                    }

                    if (
                        ["PAID", "AWAIT_CONFIRM", "NEW", "CANCELED"].includes(
                            order.status
                        )
                    )
                        releaseStock(order);
                    else if (order.status === "SENT") {
                        for (const it of order.items) {
                            const p = productById(it.id);
                            if (p)
                                p.sold = Math.max(
                                    0,
                                    Number(p.sold || 0) - it.qty
                                );
                        }
                        saveProducts();
                    }
                    order.status = "REFUNDED";
                    order.refundedAt = now();
                    await db.write();
                    audit(`REFUNDED ${id} by admin ${sender} reason=${reason}`);
                    addUserStatsForOrderRefunded(order);
                    await db.write();

                    await send(jid, `💵 Order ${id} ditandai *REFUNDED*.`);
                    await safeSendText(
                        uj,
                        [
                            `💵 *Refund diproses/selesai*`,
                            `*ID:* ${id}`,
                            `*Item:* ${summarizeItemsOneLine(order.items)}`,
                            `*Alasan:* ${reason}`,
                            `🕒 ${formatTs(order.refundedAt)}`,
                            "",
                            "Jika butuh bantuan, silakan hubungi CS:",
                            CS_CONTACT
                        ].join("\n")
                    );
                    return;
                }

                // send manual (payload custom) — RULES DI PESAN TERPISAH
                if (lower.startsWith("send ")) {
                    const tok = text.split(/\s+/),
                        id = tok[1],
                        num = cleanNumber(tok[2] || "");
                    const restIdx = text.indexOf(num) + String(num).length;
                    const payload = text.slice(restIdx + 1).trim();
                    if (!id || !num || !payload) {
                        await send(
                            jid,
                            "Format: send <idorder> <nomor> email:..|password:..||license:..|code:..|notes:..|rules:.."
                        );
                        return;
                    }

                    const uj = num + "@s.whatsapp.net";
                    const order = db.data.orders.find(
                        o => o.id === id && o.userJid === uj
                    );
                    if (!order) {
                        await send(
                            jid,
                            "⚠️ Order tidak ditemukan atau nomor tidak cocok."
                        );
                        return;
                    }
                    if (order.status !== "PAID") {
                        await send(
                            jid,
                            `⚠️ Order ${id} belum *PAID*. Konfirmasi dulu.`
                        );
                        return;
                    }

                    const blocks = payload
                        .split("||")
                        .map(s => s.trim())
                        .filter(Boolean);
                    if (!blocks.length) {
                        await send(
                            jid,
                            "⚠️ Payload kosong. Gunakan pemisah '||' untuk multi item."
                        );
                        return;
                    }

                    const prod = productById(order.items[0]?.id);
                    order.delivered ||= [];
                    let i = 1;
                    for (const b of blocks) {
                        const fields = normalizeFields(
                            parseStockPayload(b) || {}
                        );
                        await sendOrderDeliveryMessages(
                            order,
                            prod,
                            fields,
                            i++
                        );
                        order.delivered.push({
                            at: now(),
                            prodId: prod?.id || order.items[0]?.id,
                            fields
                        });
                    }
                    await db.write();

                    for (const it of order.items) {
                        const p = productById(it.id);
                        if (p)
                            p.sold = Number(p.sold || 0) + Number(it.qty || 0);
                    }
                    saveProducts();
                    order.status = "SENT";
                    order.sentAt = now();
                    await db.write();
                    addUserStatsForOrderSent(order);
                    await db.write();

                    await send(
                        jid,
                        `✅ Dikirim ${blocks.length}/${order.items.reduce(
                            (a, i) => a + i.qty,
                            0
                        )} unit untuk *${id}* ke ${num}. 🕒 ${formatTs(
                            order.sentAt
                        )}`
                    );
                    return;
                }

                // SEND dari pool: sendstock <id|orderId>|<nomor>|<jumlah>
                if (lower.startsWith("sendstock ")) {
                    const raw = text.slice(10).trim();
                    const [idOrOrder, numRaw, qtyRaw] = raw
                        .split("|")
                        .map(s => (s || "").trim());
                    const num = cleanNumber(numRaw || "");
                    const qty = Math.max(1, Number(qtyRaw || "1") || 1);
                    if (!idOrOrder || !num) {
                        await send(
                            jid,
                            "Format: sendstock <idProduk|orderId>|<nomor>|<jumlah>"
                        );
                        return;
                    }
                    const uj = num + "@s.whatsapp.net";

                    let prodId = idOrOrder;
                    let order = null;
                    if (/^VS-/i.test(idOrOrder)) {
                        order = db.data.orders.find(o => o.id === idOrOrder);
                        if (!order) {
                            await send(jid, "⚠️ OrderId tidak ditemukan.");
                            return;
                        }
                        if (order.userJid !== uj) {
                            await send(
                                jid,
                                "⚠️ Nomor tidak cocok dengan order."
                            );
                            return;
                        }
                        if (order.status !== "PAID") {
                            await send(
                                jid,
                                `⚠️ Status order ${order.id} harus PAID.`
                            );
                            return;
                        }
                        prodId = order.items[0]?.id || prodId;
                    }
                    const creds = takeStock(prodId, qty);
                    if (!creds) {
                        await send(
                            jid,
                            `⚠️ Pool *${prodId}* tidak cukup. Sisa: ${stockCount(
                                prodId
                            )}`
                        );
                        return;
                    }

                    const prod = productById(prodId);
                    let i = 1;
                    for (const fields of creds) {
                        const fakeOrder = order || {
                            id: "MANUAL-" + nanoID(),
                            items: [{ id: prodId, qty: 1 }],
                            userJid: uj
                        };
                        await sendOrderDeliveryMessages(
                            fakeOrder,
                            prod,
                            fields,
                            i++
                        );
                        if (order) {
                            order.delivered ||= [];
                            order.delivered.push({ at: now(), prodId, fields });
                        }
                    }
                    if (order) {
                        order.status = "SENT";
                        order.sentAt = now();
                        await db.write();
                        addUserStatsForOrderSent(order);
                        await db.write();
                    }
                    for (const it of order?.items || [
                        { id: prodId, qty: qty }
                    ]) {
                        const p = productById(it.id);
                        if (p)
                            p.sold = Number(p.sold || 0) + Number(it.qty || 0);
                    }
                    saveProducts();

                    await send(
                        jid,
                        `✅ Dikirim ${qty} unit dari pool *${prodId}* ke ${num}.`
                    );
                    return;
                }

                // ====== CRUD & Pool =====
                if (lower.startsWith("addprod ")) {
                    const raw = text.slice(8).trim();
                    const [
                        id,
                        title,
                        category,
                        price,
                        discount,
                        stock,
                        sold,
                        descRaw
                    ] = raw.split("|").map(s => s?.trim());
                    if (!id || !title || !category || price === undefined) {
                        await send(
                            jid,
                            "Format: addprod id|title|category|price|discount|stock|sold|desc1;desc2"
                        );
                        return;
                    }
                    if (productById(id)) {
                        await send(jid, "⚠️ ID produk sudah ada.");
                        return;
                    }
                    const prod = {
                        id: id.replace(/\s/g, ""),
                        title: clampText(title, 100),
                        category: clampText(category, 50),
                        price: Number(price) || 0,
                        discount: Number(discount) || 0,
                        stock: Math.max(0, Number(stock) || 0),
                        sold: Math.max(0, Number(sold) || 0),
                        descLines: (descRaw || "")
                            .split(";")
                            .map(s => clampText(s.trim(), 120))
                            .filter(Boolean),
                        defaultRules: "",
                        defaultNotes: "",
                        cost: 0
                    };
                    PRODUCTS.push(prod);
                    saveProducts();
                    audit(`ADDPROD ${prod.id} by ${sender}`);
                    await send(
                        jid,
                        `✅ Produk ${prod.id} ditambahkan.\n${
                            prod.title
                        } — ${formatPrice(
                            finalPrice(prod.price, prod.discount)
                        )}\nStok: ${stokLabel(prod.stock)} | Sold: ${prod.sold}`
                    );
                    return;
                }

                if (lower.startsWith("setrules ")) {
                    const raw = text.slice(9).trim();
                    const [id, rules] = raw.split("|");
                    const p = productById((id || "").trim());
                    if (!p) {
                        await send(jid, "Produk tidak ditemukan.");
                        return;
                    }
                    p.defaultRules = clampText((rules || "").trim(), 1500);
                    saveProducts();
                    await send(
                        jid,
                        `✅ Rules default untuk *${p.id}* diperbarui.`
                    );
                    return;
                }
                if (lower.startsWith("setnotes ")) {
                    const raw = text.slice(9).trim();
                    const [id, notes] = raw.split("|");
                    const p = productById((id || "").trim());
                    if (!p) {
                        await send(jid, "Produk tidak ditemukan.");
                        return;
                    }
                    p.defaultNotes = clampText((notes || "").trim(), 1500);
                    saveProducts();
                    await send(
                        jid,
                        `✅ Notes default untuk *${p.id}* diperbarui.`
                    );
                    return;
                }
                if (lower.startsWith("setcost ")) {
                    const raw = text.slice(8).trim();
                    const [id, costRaw] = raw.split("|").map(s => s?.trim());
                    const p = productById(id);
                    if (!p) {
                        await send(jid, "Produk tidak ditemukan.");
                        return;
                    }
                    const c = Number(costRaw);
                    if (!Number.isFinite(c) || c < 0) {
                        await send(jid, "⚠️ Cost harus angka ≥ 0.");
                        return;
                    }
                    p.cost = c;
                    saveProducts();
                    await send(
                        jid,
                        `✅ Cost ${id} diset: ${formatPrice(p.cost)}.`
                    );
                    return;
                }

                if (lower.startsWith("prod ")) {
                    const id = text.split(/\s+/)[1];
                    const p = productById(id);
                    if (!p) {
                        await send(jid, "Produk tidak ditemukan.");
                        return;
                    }
                    const price = finalPrice(p.price, p.discount);
                    const desc =
                        (p.descLines || []).map(d => `- ${d}`).join("\n") ||
                        "-";
                    await send(
                        jid,
                        [
                            `*${p.title}*`,
                            `*ID:* ${p.id}`,
                            `*Kategori:* ${p.category}`,
                            `*Harga:* ${formatPrice(price)}${
                                p.discount ? ` (diskon *${p.discount}%*)` : ""
                            }`,
                            `*Stok:* ${stokLabel(p.stock)} | *Terjual:* ${
                                p.sold || 0
                            }`,
                            `*Deskripsi:*`,
                            desc
                        ].join("\n")
                    );
                    return;
                }

                if (lower.startsWith("prods")) {
                    const page = Math.max(
                        1,
                        Number(text.split(/\s+/)[1] || "1") || 1
                    );
                    const per = 10;
                    const list = PRODUCTS.slice((page - 1) * per, page * per);
                    if (!list.length) {
                        await send(jid, "Tidak ada data di halaman ini.");
                        return;
                    }
                    const lines = list.map(
                        p =>
                            `• [${p.id}] ${p.title} — *${formatPrice(
                                finalPrice(p.price, p.discount)
                            )}* — stok *${stokLabel(p.stock)}*`
                    );
                    await send(jid, `Halaman ${page}\n${lines.join("\n")}`);
                    return;
                }

                if (lower.startsWith("search ")) {
                    const q = text.slice(7).toLowerCase();
                    const res = PRODUCTS.filter(
                        p =>
                            p.id.toLowerCase().includes(q) ||
                            p.title.toLowerCase().includes(q) ||
                            p.category.toLowerCase().includes(q)
                    ).slice(0, 20);
                    if (!res.length) {
                        await send(jid, "Tidak ada hasil.");
                        return;
                    }
                    await send(
                        jid,
                        res
                            .map(
                                p =>
                                    `• [${p.id}] ${p.title} — ${formatPrice(
                                        finalPrice(p.price, p.discount)
                                    )} — stok ${stokLabel(p.stock)}`
                            )
                            .join("\n")
                    );
                    return;
                }

                if (lower.startsWith("setharga ")) {
                    const raw = text.slice(9).trim();
                    const [id, price, discount] = raw
                        .split("|")
                        .map(s => s?.trim());
                    const p = productById(id);
                    if (!p) {
                        await send(jid, "Produk tidak ditemukan.");
                        return;
                    }
                    if (price !== undefined && price !== "") {
                        const v = Number(price);
                        if (!Number.isFinite(v) || v < 0) {
                            await send(jid, "⚠️ Harga harus angka ≥ 0.");
                            return;
                        }
                        p.price = v;
                    }
                    if (discount !== undefined) {
                        const d = Number(discount);
                        if (!Number.isFinite(d) || d < 0 || d > 100) {
                            await send(jid, "⚠️ Diskon harus 0–100.");
                            return;
                        }
                        p.discount = d;
                    }
                    saveProducts();
                    audit(`SETHARGA ${id} by ${sender}`);
                    await send(
                        jid,
                        `✅ Harga ${id}: ${formatPrice(
                            finalPrice(p.price, p.discount)
                        )} (disc ${p.discount}%)`
                    );
                    return;
                }

                if (lower.startsWith("setstok ")) {
                    const raw = text.slice(8).trim();
                    const [id, qtyRaw] = raw.split("|").map(s => s?.trim());
                    const p = productById(id);
                    if (!p) {
                        await send(jid, "Produk tidak ditemukan.");
                        return;
                    }
                    let qtyParsed;
                    if (/^kosong$/i.test(qtyRaw || "")) qtyParsed = 0;
                    else qtyParsed = Number(qtyRaw);
                    if (!Number.isFinite(qtyParsed) || qtyParsed < 0) {
                        await send(
                            jid,
                            "⚠️ Nilai stok harus angka ≥ 0. Contoh: setstok test|0"
                        );
                        return;
                    }
                    p.stock = qtyParsed;
                    saveProducts();
                    audit(`SETSTOK ${id} by ${sender}`);
                    await send(jid, `✅ Stok ${id}: ${stokLabel(p.stock)}`);
                    return;
                }

                if (lower.startsWith("settitle ")) {
                    const raw = text.slice(9).trim();
                    const [id, title] = raw.split("|").map(s => s?.trim());
                    const p = productById(id);
                    if (!p) {
                        await send(jid, "Produk tidak ditemukan.");
                        return;
                    }
                    p.title = clampText(title || "", 100) || p.title;
                    saveProducts();
                    audit(`SETTITLE ${id} by ${sender}`);
                    await send(jid, `✅ Title ${id} diubah.`);
                    return;
                }

                if (lower.startsWith("setcat ")) {
                    const raw = text.slice(7).trim();
                    const [id, cat] = raw.split("|").map(s => s?.trim());
                    const p = productById(id);
                    if (!p) {
                        await send(jid, "Produk tidak ditemukan.");
                        return;
                    }
                    p.category = clampText(cat || "", 50) || p.category;
                    saveProducts();
                    audit(`SETCAT ${id} by ${sender}`);
                    await send(jid, `✅ Category ${id} diubah.`);
                    return;
                }

                if (lower.startsWith("setdesc ")) {
                    const raw = text.slice(8).trim();
                    const [id, descRaw] = raw.split("|");
                    const p = productById((id || "").trim());
                    if (!p) {
                        await send(jid, "Produk tidak ditemukan.");
                        return;
                    }
                    p.descLines = (descRaw || "")
                        .split(";")
                        .map(s => clampText(s.trim(), 120))
                        .filter(Boolean);
                    saveProducts();
                    audit(`SETDESC ${id} by ${sender}`);
                    await send(
                        jid,
                        `✅ Deskripsi ${id} diubah (${p.descLines.length} baris).`
                    );
                    return;
                }

                if (lower.startsWith("delprod ")) {
                    const id = text.split(/\s+/)[1];
                    const p = productById(id);
                    if (!p) {
                        await send(jid, "Produk tidak ditemukan.");
                        return;
                    }
                    db.data._pendingDel = { id, by: sender, at: Date.now() };
                    await db.write();
                    await send(
                        jid,
                        `⚠️ Konfirmasi hapus: ketik *ya ${id}* dalam 30 detik untuk menghapus.`
                    );
                    return;
                }

                if (lower.startsWith("ya ")) {
                    const id = text.split(/\s+/)[1];
                    const pend = db.data._pendingDel;
                    if (
                        !pend ||
                        pend.id !== id ||
                        pend.by !== sender ||
                        Date.now() - pend.at > 30000
                    ) {
                        await send(
                            jid,
                            "⚠️ Tidak ada penghapusan tertunda / waktu habis."
                        );
                        return;
                    }
                    delete db.data._pendingDel;
                    const idx = PRODUCTS.findIndex(
                        p => p.id.toLowerCase() === id.toLowerCase()
                    );
                    if (idx === -1) {
                        await send(jid, "Produk tidak ditemukan.");
                        return;
                    }
                    const removed = PRODUCTS.splice(idx, 1)[0];
                    saveProducts();
                    await db.write();
                    audit(`DELPROD ${id} by ${sender}`);
                    await send(jid, `✅ Produk ${removed.id} dihapus.`);
                    return;
                }

                if (lower.startsWith("addstock ")) {
                    const parts = text.trim().split(/\s+/);
                    const id = parts[1];
                    const payload = text
                        .slice(text.indexOf(id) + id.length + 1)
                        .trim();
                    if (!id || !payload) {
                        await send(jid, "Format: addstock <id> <baris>");
                        return;
                    }
                    const res = addStock(id, payload);
                    if (!res.ok) {
                        await send(jid, "⚠️ " + res.err);
                        return;
                    }
                    await send(
                        jid,
                        `✅ 1 akun ditambahkan ke pool *${id}*. Sisa di pool: *${res.left}*`
                    );
                    return;
                }

                if (lower.startsWith("addstockmulti ")) {
                    const parts = text.trim().split(/\s+/);
                    const id = parts[1];
                    const payload = text
                        .slice(text.indexOf(id) + id.length + 1)
                        .trim();
                    if (!id || !payload) {
                        await send(
                            jid,
                            "Format: addstockmulti <id> <baris1>||<baris2>||..."
                        );
                        return;
                    }
                    const res = addStockMulti(id, payload);
                    await send(
                        jid,
                        `✅ Import pool *${id}*: tambah ${res.ok}, gagal ${res.fail}. Total pool sekarang: *${res.left}*`
                    );
                    return;
                }

                if (lower.startsWith("stock ")) {
                    const id = text.split(/\s+/)[1];
                    if (!id) {
                        await send(jid, "Format: stock <id>");
                        return;
                    }
                    const n = stockCount(id);
                    const sample = readStock(id)
                        .slice(0, Math.min(3, n))
                        .map((o, i) => {
                            const lead = o.email
                                ? maskEmail(o.email)
                                : o.license || o.code || o.key || "-";
                            return `${i + 1}. ${lead}${
                                o.profil ? " | Profil:" + o.profil : ""
                            }`;
                        });
                    await send(
                        jid,
                        [
                            `📦 *Pool ${id}*`,
                            `Jumlah: *${n}*`,
                            sample.length ? "Contoh:\n" + sample.join("\n") : ""
                        ].join("\n")
                    );
                    return;
                }

                if (lower.startsWith("cekallstock ")) {
                    const id = text.split(/\s+/)[1];
                    if (!id) {
                        await send(jid, "Format: cekallstock <id>");
                        return;
                    }
                    const arr = readStock(id);
                    if (!arr.length) {
                        await send(jid, `Pool *${id}* kosong.`);
                        return;
                    }
                    const lines = arr.map(
                        (o, i) =>
                            `${i + 1}. ` +
                            Object.keys(o)
                                .map(k => `${k}:${o[k]}`)
                                .join(" | ")
                    );
                    await sendLongText(
                        jid,
                        `📦 *Pool ${id} — ${arr.length} akun*\n\n${lines.join(
                            "\n"
                        )}`
                    );
                    return;
                }

                if (lower.startsWith("syncstock ")) {
                    const arg = text.split(/\s+/)[1];
                    if (!arg) {
                        await send(jid, "Format: syncstock <id>|all");
                        return;
                    }
                    if (arg.toLowerCase() === "all") {
                        for (const p of PRODUCTS) {
                            p.stock = stockCount(p.id);
                        }
                        saveProducts();
                        await send(
                            jid,
                            "✅ Sinkron semua produk: *stock = jumlah pool*."
                        );
                    } else {
                        const p = productById(arg);
                        if (!p) {
                            await send(jid, "Produk tidak ditemukan.");
                            return;
                        }
                        p.stock = stockCount(p.id);
                        saveProducts();
                        await send(
                            jid,
                            `✅ Sinkron *${p.id}*: stock = ${p.stock}`
                        );
                    }
                    return;
                }

                // ====== REPORT ======
                if (lower.startsWith("report ")) {
                    const args = text.split(/\s+/).slice(1);
                    const mode = (args[0] || "").toLowerCase();

                    const sumOrders = arr => {
                        const total = arr.reduce(
                            (a, o) => a + Number(o.total || 0),
                            0
                        );
                        const byStatus = arr.reduce(
                            (acc, o) => (
                                (acc[o.status] = (acc[o.status] || 0) + 1), acc
                            ),
                            {}
                        );
                        const profit = arr.reduce((a, o) => {
                            // kasar: (harga jual - cost) per item
                            let gain = 0;
                            for (const it of o.items || []) {
                                const p = productById(it.id);
                                if (!p) continue;
                                const sell =
                                    finalPrice(p.price, p.discount) * it.qty;
                                const cost = Number(p.cost || 0) * it.qty;
                                gain += sell - cost;
                            }
                            return a + (o.status === "SENT" ? gain : 0);
                        }, 0);
                        return { total, byStatus, profit };
                    };

                    const inRange = (o, a, b) => {
                        const t = o.createdAt || 0;
                        return t >= a && t <= b;
                    };

                    const today0 = new Date(
                        new Date().toLocaleString("en-US", {
                            timeZone: "Asia/Jakarta"
                        })
                    );
                    today0.setHours(0, 0, 0, 0);
                    const today1 = new Date(today0);
                    today1.setDate(today1.getDate() + 1);

                    const firstOfMonth = new Date(today0);
                    firstOfMonth.setDate(1);
                    const nextMonth = new Date(firstOfMonth);
                    nextMonth.setMonth(nextMonth.getMonth() + 1);

                    let list = db.data.orders || [];

                    if (mode === "today") {
                        list = list.filter(o =>
                            inRange(o, today0.getTime(), today1.getTime() - 1)
                        );
                    } else if (mode === "month") {
                        list = list.filter(o =>
                            inRange(
                                o,
                                firstOfMonth.getTime(),
                                nextMonth.getTime() - 1
                            )
                        );
                    } else if (mode === "all") {
                        // keep all
                    } else if (mode === "range") {
                        const d1 = args[1],
                            d2 = args[2];
                        if (
                            !/^\d{4}-\d{2}-\d{2}$/.test(d1 || "") ||
                            !/^\d{4}-\d{2}-\d{2}$/.test(d2 || "")
                        ) {
                            await send(
                                jid,
                                "Format: report range YYYY-MM-DD YYYY-MM-DD"
                            );
                            return;
                        }
                        const s = new Date(`${d1}T00:00:00+07:00`).getTime();
                        const e = new Date(`${d2}T23:59:59+07:00`).getTime();
                        list = list.filter(o => inRange(o, s, e));
                    } else {
                        await send(
                            jid,
                            "Format: report today | month | all | range YYYY-MM-DD YYYY-MM-DD"
                        );
                        return;
                    }

                    const { total, byStatus, profit } = sumOrders(list);
                    const lines = [
                        `📊 *Report* (${mode})`,
                        `• Total order: *${list.length}*`,
                        `• NEW:${byStatus.NEW || 0}  AWAIT:${
                            byStatus.AWAIT_CONFIRM || 0
                        }  PAID:${byStatus.PAID || 0}  SENT:${
                            byStatus.SENT || 0
                        }  REFUND:${byStatus.REFUNDED || 0}  CANCEL:${
                            byStatus.CANCELED || 0
                        }  EXP:${byStatus.EXPIRED || 0}`,
                        `• Omzet (sum harga): *${formatPrice(total)}*`,
                        `• Est. profit (SENT): *${formatPrice(profit)}*`
                    ];
                    await send(jid, lines.join("\n"));
                    return;
                }

                // ====== BROADCAST ======
                if (lower.startsWith("broadcastimg ")) {
                    const raw = text.slice("broadcastimg ".length).trim();
                    const [segmentRaw, url, ...capParts] = raw.split("|");
                    const segment = (segmentRaw || "all").trim().toLowerCase();
                    const caption = (capParts.join("|") || "").trim();

                    if (!url) {
                        await send(
                            jid,
                            "Format: broadcastimg <all|buyers|nobuy> | <urlGambar> | <caption?>"
                        );
                        return;
                    }

                    const jids = pickAudience(segment);
                    if (!jids.length) {
                        await send(jid, "Tidak ada target untuk broadcast.");
                        return;
                    }

                    try {
                        const { buffer, contentType } = await fetchAsBuffer(
                            url.trim()
                        );
                        if (!/^image\//.test(contentType || "")) {
                            await send(
                                jid,
                                "⚠️ URL bukan gambar (Content-Type tidak cocok)."
                            );
                            return;
                        }
                        const res = await broadcastSequential(
                            jids,
                            async j =>
                                getCurrentSock().sendMessage(j, {
                                    image: buffer,
                                    caption: clampText(caption, 4000)
                                }),
                            { delayMs: 700 }
                        );
                        audit(
                            `BROADCAST_IMG seg=${segment} ok=${res.ok} fail=${res.fail} total=${res.total}`
                        );
                        await send(
                            jid,
                            `📣 Broadcast IMG (${segment}) selesai — OK:${res.ok} Fail:${res.fail} Total:${res.total}`
                        );
                    } catch (e) {
                        await send(
                            jid,
                            "❌ Gagal unduh/kirim gambar: " +
                                (e?.message || String(e))
                        );
                    }
                    return;
                }

                if (lower.startsWith("broadcastvid ")) {
                    const raw = text.slice("broadcastvid ".length).trim();
                    const [segmentRaw, url, ...capParts] = raw.split("|");
                    const segment = (segmentRaw || "all").trim().toLowerCase();
                    const caption = (capParts.join("|") || "").trim();

                    if (!url) {
                        await send(
                            jid,
                            "Format: broadcastvid <all|buyers|nobuy> | <urlVideo> | <caption?>"
                        );
                        return;
                    }

                    const jids = pickAudience(segment);
                    if (!jids.length) {
                        await send(jid, "Tidak ada target untuk broadcast.");
                        return;
                    }

                    try {
                        const { buffer, contentType } = await fetchAsBuffer(
                            url.trim()
                        );
                        if (!/^video\//.test(contentType || "")) {
                            await send(
                                jid,
                                "⚠️ URL bukan video (Content-Type tidak cocok)."
                            );
                            return;
                        }
                        const res = await broadcastSequential(
                            jids,
                            async j =>
                                getCurrentSock().sendMessage(j, {
                                    video: buffer,
                                    caption: clampText(caption, 4000)
                                }),
                            { delayMs: 1200 } // video lebih berat
                        );
                        audit(
                            `BROADCAST_VID seg=${segment} ok=${res.ok} fail=${res.fail} total=${res.total}`
                        );
                        await send(
                            jid,
                            `📣 Broadcast VID (${segment}) selesai — OK:${res.ok} Fail:${res.fail} Total:${res.total}`
                        );
                    } catch (e) {
                        await send(
                            jid,
                            "❌ Gagal unduh/kirim video: " +
                                (e?.message || String(e))
                        );
                    }
                    return;
                }

                if (lower.startsWith("broadcast ")) {
                    const raw = text.slice("broadcast ".length).trim();
                    const [segmentRaw, ...msgParts] = raw.split("|");
                    const segment = (segmentRaw || "all").trim().toLowerCase();
                    const message = (msgParts.join("|") || "").trim();
                    if (!message) {
                        await send(
                            jid,
                            "Format: broadcast <all|buyers|nobuy> | <pesan>"
                        );
                        return;
                    }
                    const jids = pickAudience(segment);
                    if (!jids.length) {
                        await send(jid, "Tidak ada target untuk broadcast.");
                        return;
                    }
                    const res = await broadcastSequential(
                        jids,
                        async j => safeSendText(j, clampText(message, 4000)),
                        { delayMs: 500 }
                    );
                    audit(
                        `BROADCAST_TXT seg=${segment} ok=${res.ok} fail=${res.fail} total=${res.total}`
                    );
                    await send(
                        jid,
                        `📣 Broadcast TXT (${segment}) selesai — OK:${res.ok} Fail:${res.fail} Total:${res.total}`
                    );
                    return;
                }

                // ====== BACKUP / PCLOUD COMMANDS ======
                if (lower === "backup status") {
                    const b = db.data.settings.backup || {};
                    await send(
                        jid,
                        [
                            "💾 *Backup Status*",
                            `• enabled: ${b.enabled ? "ON" : "OFF"}`,
                            `• interval: ${b.interval || "daily"}`,
                            `• hour (WIB): ${b.hour ?? "-"}`,
                            `• lastRunDay: ${b.lastRunDay || "-"}`,
                            `• pCloud folder: ${b.pcloud?.folder || "-"}`,
                            `• pCloud token: ${
                                b.pcloud?.token ? "(tersimpan)" : "(kosong)"
                            }`
                        ].join("\n")
                    );
                    return;
                }

                if (lower === "backup on") {
                    db.data.settings.backup.enabled = true;
                    await db.write();
                    await send(jid, "✅ Backup: *ON*");
                    return;
                }
                if (lower === "backup off") {
                    db.data.settings.backup.enabled = false;
                    await db.write();
                    await send(jid, "✅ Backup: *OFF*");
                    return;
                }

                if (lower.startsWith("backup interval ")) {
                    const v = text.split(/\s+/)[2]?.toLowerCase();
                    if (!["daily", "weekly"].includes(v)) {
                        await send(jid, "Format: backup interval daily|weekly");
                        return;
                    }
                    db.data.settings.backup.interval = v;
                    await db.write();
                    await send(jid, `✅ Interval backup: *${v}*`);
                    return;
                }

                if (lower.startsWith("backup hour ")) {
                    const h = Number(text.split(/\s+/)[2]);
                    if (!Number.isInteger(h) || h < 0 || h > 23) {
                        await send(jid, "Format: backup hour <0-23>");
                        return;
                    }
                    db.data.settings.backup.hour = h;
                    await db.write();
                    await send(jid, `✅ Jam backup WIB: *${h}:00*`);
                    return;
                }

                if (lower.startsWith("backup folder ")) {
                    const folder =
                        text.slice("backup folder ".length).trim() ||
                        "/VienzeStoreBackups";
                    db.data.settings.backup.pcloud =
                        db.data.settings.backup.pcloud || {};
                    db.data.settings.backup.pcloud.folder = folder.startsWith(
                        "/"
                    )
                        ? folder
                        : "/" + folder;
                    await db.write();
                    await send(
                        jid,
                        `✅ Folder pCloud: *${db.data.settings.backup.pcloud.folder}*`
                    );
                    return;
                }

                if (lower.startsWith("setpcloud ")) {
                    const parts = text.split(/\s+/);
                    const sub = parts[1]?.toLowerCase();
                    if (sub === "token") {
                        const token = text
                            .slice(text.indexOf("token") + 5)
                            .trim();
                        if (!token) {
                            await send(
                                jid,
                                "Format: setpcloud token <ACCESS_TOKEN>"
                            );
                            return;
                        }
                        db.data.settings.backup.pcloud =
                            db.data.settings.backup.pcloud || {};
                        db.data.settings.backup.pcloud.token = token;
                        await db.write();
                        await send(jid, "✅ Token pCloud disimpan.");
                        return;
                    } else if (sub === "folder") {
                        const folder = text
                            .slice(text.indexOf("folder") + 6)
                            .trim();
                        db.data.settings.backup.pcloud =
                            db.data.settings.backup.pcloud || {};
                        db.data.settings.backup.pcloud.folder = folder
                            ? folder.startsWith("/")
                                ? folder
                                : "/" + folder
                            : "/VienzeStoreBackups";
                        await db.write();
                        await send(
                            jid,
                            `✅ Folder pCloud disetel ke *${db.data.settings.backup.pcloud.folder}*.`
                        );
                        return;
                    } else {
                        await send(
                            jid,
                            "Perintah: setpcloud token <ACCESS_TOKEN>  |  setpcloud folder <path>"
                        );
                        return;
                    }
                }

                if (lower === "testpcloud") {
                    const tok = db.data.settings.backup?.pcloud?.token || "";
                    if (!tok) {
                        await send(
                            jid,
                            "❌ Token pCloud kosong. Set dulu: setpcloud token <ACCESS_TOKEN>"
                        );
                        return;
                    }
                    try {
                        const ui = await pcloudUserInfo(tok);
                        if (ui?.result === 0 || ui?.auth) {
                            await send(
                                jid,
                                `✅ pCloud OK. User: *${
                                    ui?.email || ui?.userid || "-"
                                }*`
                            );
                        } else {
                            await send(
                                jid,
                                "❌ pCloud gagal: " + JSON.stringify(ui)
                            );
                        }
                    } catch (e) {
                        await send(
                            jid,
                            "❌ pCloud error: " + (e?.message || String(e))
                        );
                    }
                    return;
                }

                if (lower === "backup now") {
                    const ok = await runBackup("manual");
                    await send(
                        jid,
                        ok ? "✅ Backup sukses (manual)." : "❌ Backup gagal."
                    );
                    return;
                }
            } // end isAdmin

            // ===== USER-FACING FALLBACK =====
            // set nama (opsional): nama <nama>
            if (lower.startsWith("nama ")) {
                const nm = text.slice(5).trim();
                user.name = clampText(nm, 40);
                await db.write();
                await send(jid, `✅ Nama kamu diset: *${user.name}*`);
                return;
            }

            // fallback unknown
            await send(jid, userHelpShort());
            return;
        } catch (err) {
            console.error("messages.upsert handler error:", err);
            try {
                await safeSendText(
                    mUp?.messages?.[0]?.key?.remoteJid,
                    "⚠️ Terjadi kesalahan memproses pesan. Coba lagi ya."
                );
            } catch {}
        }
    });
} // end startSock

// ===== START =====
startSock().catch(err => {
    console.error("Fatal start error:", err);
    process.exitCode = 1;
});

// optional: graceful shutdown
process.on("unhandledRejection", e => console.error("UNHANDLED REJECTION:", e));
process.on("SIGINT", () => {
    console.log("SIGINT received, exiting...");
    process.exit(0);
});
process.on("SIGTERM", () => {
    console.log("SIGTERM received, exiting...");
    process.exit(0);
});