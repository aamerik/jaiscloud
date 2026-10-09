#!/usr/bin/env python3
"""crypto-medallion leaderboard — Cloud Run page + JSON API (design §5.4).

Reads the per-symbol Firestore documents the publisher writes and serves a live
leaderboard. The page re-fetches `/api/leaderboard` once a second and re-sorts,
so the recording never has a still second (design §9).
"""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

EMULATOR = os.environ.get("EMULATOR", "http://jaiscloud-gcp.jaiscloud.svc.cluster.local:8080")
PROJECT = os.environ.get("PROJECT", "crypto-medallion")
COLLECTION = os.environ.get("FIRESTORE_COLLECTION", "leaderboard")
PORT = int(os.environ.get("PORT", "8080"))

PAGE = """<!doctype html><html><head><meta charset="utf-8">
<title>crypto medallion · live leaderboard</title>
<style>
 body{margin:0;font-family:system-ui,-apple-system,sans-serif;background:#0b1220;color:#e6edf3}
 header{padding:20px 28px;border-bottom:1px solid #1e2a44;display:flex;align-items:baseline;gap:14px}
 h1{font-size:20px;margin:0;font-weight:650}
 .tag{font-size:12px;color:#7d8db1}
 .dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:#2ecc71;margin-right:6px;
      animation:pulse 1.2s infinite}
 @keyframes pulse{0%,100%{opacity:1}50%{opacity:.25}}
 table{border-collapse:collapse;width:100%}
 th,td{text-align:right;padding:12px 22px;border-bottom:1px solid #16203a;font-variant-numeric:tabular-nums}
 th:first-child,td:first-child{text-align:left}
 th{font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#7d8db1;font-weight:600}
 td{font-size:16px}
 tr.up{animation:flash .8s ease-out}
 @keyframes flash{from{background:#12233f}to{background:transparent}}
 .sym{font-weight:650}
 footer{padding:12px 28px;color:#7d8db1;font-size:12px}
</style></head>
<body><header><span class="dot"></span><h1>Live crypto leaderboard</h1>
<span class="tag">jaiscloud-gcp · Firestore read model · Eventarc-driven</span></header>
<table><thead><tr><th>Symbol</th><th>VWAP</th><th>Volume</th><th>Trades</th><th>Window end</th></tr></thead>
<tbody id="rows"><tr><td colspan="5" style="text-align:center;color:#7d8db1">waiting for the first window…</td></tr></tbody></table>
<footer id="stamp">updated —</footer>
<script>
let prev={};
async function tick(){
  try{
    const r=await fetch('/api/leaderboard',{cache:'no-store'});
    const rows=await r.json();
    const tb=document.getElementById('rows');
    tb.innerHTML=rows.map(x=>{
      const moved = prev[x.symbol]!==undefined && prev[x.symbol]!==x.volume;
      return `<tr class="${moved?'up':''}"><td class="sym">${x.symbol}</td>`+
        `<td>${Number(x.vwap).toLocaleString(undefined,{maximumFractionDigits:4})}</td>`+
        `<td>${Number(x.volume).toLocaleString(undefined,{maximumFractionDigits:4})}</td>`+
        `<td>${x.trades}</td><td>${(x.window_end||'').replace('T',' ').slice(0,19)}</td></tr>`;
    }).join('')||document.getElementById('rows').innerHTML;
    rows.forEach(x=>prev[x.symbol]=x.volume);
    document.getElementById('stamp').textContent='updated '+new Date().toLocaleTimeString()+
      ' · '+rows.length+' symbols';
  }catch(e){document.getElementById('stamp').textContent='feed error: '+e;}
}
tick();setInterval(tick,1000);
</script></body></html>"""


def http_get(path: str):
    req = urllib.request.Request(EMULATOR + path)
    req.add_header("Authorization", "Bearer demo-token")
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read() or b"{}")


def value(field: dict):
    for key in ("stringValue", "doubleValue", "integerValue", "timestampValue",
                "booleanValue"):
        if key in field:
            v = field[key]
            if key == "integerValue":
                return int(v)
            return v
    return None


def leaderboard_rows() -> list[dict]:
    path = f"/v1/projects/{PROJECT}/databases/(default)/documents/{COLLECTION}"
    data = http_get(path)
    rows = []
    for doc in data.get("documents", []):
        fields = {k: value(v) for k, v in doc.get("fields", {}).items()}
        rows.append(fields)
    rows.sort(key=lambda r: float(r.get("volume") or 0), reverse=True)
    return rows


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        return

    def _send(self, code, body: bytes, ctype="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/api/leaderboard"):
            try:
                body = json.dumps(leaderboard_rows()).encode()
            except Exception as exc:
                body = json.dumps({"error": str(exc)}).encode()
            self._send(200, body)
        elif self.path.startswith("/health"):
            self._send(200, b'{"status":"ok"}')
        else:
            self._send(200, PAGE.encode(), "text/html; charset=utf-8")


def main() -> None:
    print(f"[leaderboard] listening on :{PORT}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
