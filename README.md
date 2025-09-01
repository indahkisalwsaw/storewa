# WhatsApp Store Bot — v2 (Pairing Fix)
- Pairing code flow diperkuat (auto-retry) + fallback QR (ASCII) jika kamu set `PAIRING=0`
- Nonaktifkan `printQRInTerminal` bawaan (hindari warning deprecation)
- Handler `connection.update` menampilkan QR (mode QR) & status koneksi

## Quickstart
cp .env.example .env
# edit OWNER & QRIS_PAYLOAD
npm i
npm start

## Reset pairing (kalau gagal)
rm -rf auth
npm start
# storewa
