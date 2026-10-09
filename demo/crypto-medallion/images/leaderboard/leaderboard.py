#!/usr/bin/env python3
"""crypto-medallion leaderboard — Cloud Run page + JSON API (design §5.4).

Reads the per-symbol Firestore documents the publisher writes and serves a live
market board: price (last), change vs the previous window, notional volume in
USD (comparable across symbols), VWAP, trades and window. Two tabs (Volume /
Movers) auto-rotate; bars use a stable share-of-total scale. Below the board a
pane tails the demo's application logs from Cloud Logging, so the page shows the
pipeline working, not just the results.
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
LOG_NAME = f"projects/{PROJECT}/logs/crypto-medallion"

PAGE = """<!doctype html><html><head><meta charset="utf-8">
<title>crypto medallion · live market board</title>
<style>
 :root{--row:68px}
 *{box-sizing:border-box}
 html,body{margin:0;height:100%}
 body{font-family:system-ui,-apple-system,sans-serif;background:#0b1220;color:#e6edf3;
      display:flex;flex-direction:column;height:100vh;overflow:hidden}
 header{padding:14px 24px 8px;display:flex;align-items:baseline;gap:10px}
 h1{font-size:18px;margin:0;font-weight:650}
 .sub{font-size:12px;color:#7d8db1}
 .dot{width:9px;height:9px;border-radius:50%;background:#2ecc71;display:inline-block;margin-right:6px;
      box-shadow:0 0 0 0 rgba(46,204,113,.7);animation:pulse 1.1s infinite}
 @keyframes pulse{0%{box-shadow:0 0 0 0 rgba(46,204,113,.7)}70%{box-shadow:0 0 0 9px rgba(46,204,113,0)}100%{box-shadow:0 0 0 0 rgba(46,204,113,0)}}
 .tabs{display:flex;gap:6px;padding:0 24px 8px}
 .tab{font-size:12px;padding:4px 12px;border-radius:999px;border:1px solid #1e2a44;color:#9fb0d0;cursor:pointer;background:#0f1730}
 .tab.on{background:#1d4ed8;border-color:#1d4ed8;color:#fff}
 .grid{display:grid;grid-template-columns:110px 1fr 110px 90px 110px 70px 150px;align-items:center;gap:8px}
 .colhead{padding:2px 24px 6px;color:#7d8db1;font-size:11px;letter-spacing:.06em;text-transform:uppercase}
 .colhead div:nth-child(n+3){text-align:right}
 #board{position:relative;margin:0 0 2px}
 .row{position:absolute;left:0;right:0;height:var(--row);padding:0 24px;background:#0b1220;
      border-bottom:1px solid #16203a;transition:transform .65s cubic-bezier(.2,.8,.2,1),background .5s ease}
 .row.flash{background:#12233f}
 .sym{font-weight:650}
 .barwrap{height:9px;background:#16203a;border-radius:6px;overflow:hidden;margin-right:18px}
 .bar{height:100%;width:0;border-radius:6px;background:linear-gradient(90deg,#2563eb,#22d3ee);
      transition:width .7s cubic-bezier(.2,.8,.2,1),background .4s}
 .bar.down{background:linear-gradient(90deg,#b91c1c,#f87171)}
 .num{text-align:right;font-variant-numeric:tabular-nums}
 .price{text-align:right;font-variant-numeric:tabular-nums;font-weight:650;font-size:15px}
 .chg{text-align:right;font-variant-numeric:tabular-nums;font-weight:600}
 .up{color:#2ecc71}.down{color:#f87171}.flat{color:#7d8db1}
 .we{text-align:right;color:#7d8db1;font-size:12px}
 #logwrap{flex:1;min-height:0;display:flex;flex-direction:column;border-top:1px solid #1e2a44;padding-top:6px}
 .logtitle{padding:0 24px 4px;color:#7d8db1;font-size:11px;letter-spacing:.06em;text-transform:uppercase}
 #logs{flex:1;min-height:0;overflow-y:auto;padding:0 24px 10px;
       font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12.5px;line-height:1.5}
 .ln{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
 .lt{color:#5b6b8c;margin-right:8px}
 .lc{display:inline-block;width:96px}
 .c-bridge{color:#22d3ee}.c-publisher{color:#a78bfa}.c-spark{color:#f59e0b}
 .c-scheduler{color:#34d399}.c-bigquery{color:#60a5fa}
 .sev-WARNING{color:#fbbf24}.sev-ERROR{color:#f87171}
 .cursor{color:#2ecc71}
</style></head>
<body>
<header><span class="dot"></span><h1>Live crypto market board</h1>
<span class="sub">jaiscloud-gcp · per-symbol 10s tumbling window · event time · notional in USD</span></header>
<div class="tabs"><div class="tab on" id="tab-volume">Volume</div><div class="tab" id="tab-movers">Movers</div></div>
<div class="grid colhead"><div>Symbol</div><div>Notional (USD)</div><div>Price</div><div>Chg%</div><div>VWAP</div><div>Trades</div><div>Window end</div></div>
<div id="board"></div>
<div id="logwrap"><div class="logtitle">app logs · Cloud Logging</div><div id="logs"></div></div>
<script>
const ROW=68, board=document.getElementById('board'), logEl=document.getElementById('logs'), state={};
let tab='volume', rotate=Date.now(), seen=new Set(), follow=true;
logEl.addEventListener('scroll',()=>{ follow=(logEl.scrollHeight-logEl.scrollTop-logEl.clientHeight)<40; });
const el=(t,c,x)=>{const e=document.createElement(t); if(c)e.className=c; if(x!=null)e.textContent=x; return e;};
const fmtPrice=v=>v==null?'—':Number(v).toLocaleString(undefined,{maximumFractionDigits:Number(v)>=1000?2:4});
const fmtNot=v=>{v=+v||0; return v>=1e6?(v/1e6).toFixed(2)+'M':v>=1e3?(v/1e3).toFixed(1)+'k':v.toFixed(0);};
function tween(st,key,to,apply){const from=st[key]; st[key]=to;
  if(from===to){apply(to);return;}
  const t0=performance.now();
  (function step(t){const k=Math.min(1,(t-t0)/700),e=k*(2-k);apply(from+(to-from)*e);if(k<1)requestAnimationFrame(step);})(t0);}
function makeRow(sym){
  const r=el('div','row grid'); r.style.transform='translateY(-200px)';
  r.appendChild(el('div','sym',sym));
  const bw=el('div','barwrap'), b=el('div','bar'); bw.appendChild(b); r.appendChild(bw);
  const price=el('div','price','—'); r.appendChild(price);
  const chg=el('div','chg flat','—'); r.appendChild(chg);
  const vwap=el('div','num','—'); r.appendChild(vwap);
  const tr=el('div','num','—'); r.appendChild(tr);
  const we=el('div','we','—'); r.appendChild(we);
  board.appendChild(r);
  state[sym]={el:r,bar:b,price,priceVal:0,chg,vwap,vwapVal:0,tr,we};
}
function setTab(t){tab=t; rotate=Date.now();
  document.getElementById('tab-volume').classList.toggle('on',t==='volume');
  document.getElementById('tab-movers').classList.toggle('on',t==='movers');}
document.getElementById('tab-volume').onclick=()=>setTab('volume');
document.getElementById('tab-movers').onclick=()=>setTab('movers');
async function boardTick(){
  try{
    const rows=await (await fetch('/api/leaderboard',{cache:'no-store'})).json();
    const qsum=rows.reduce((s,x)=>s+(+x.quote_volume||0),0)||1;
    const maxAbs=Math.max(1e-9,...rows.map(x=>Math.abs(+x.change_pct||0)));
    if(tab==='volume') rows.sort((a,b)=>(+b.quote_volume||0)-(+a.quote_volume||0));
    else rows.sort((a,b)=>(b.change_pct??-1e9)-(a.change_pct??-1e9));
    rows.forEach((x,i)=>{
      if(!state[x.symbol]) makeRow(x.symbol);
      const st=state[x.symbol];
      st.el.style.transform=`translateY(${i*ROW}px)`;
      if(tab==='volume'){ st.bar.className='bar'; st.bar.style.width=Math.max(2,100*(+x.quote_volume||0)/qsum)+'%'; }
      else { const c=+x.change_pct||0; st.bar.className='bar'+(c<0?' down':''); st.bar.style.width=Math.max(2,100*Math.abs(c)/maxAbs)+'%'; }
      st.el.classList.add('flash'); setTimeout(()=>st.el.classList.remove('flash'),450);
      tween(st,'priceVal',+x.price||0, v=>st.price.textContent=fmtPrice(v));
      const c=x.change_pct;
      st.chg.textContent=c==null?'—':(c>=0?'+':'')+Number(c).toFixed(2)+'%';
      st.chg.className='chg '+(c==null?'flat':c>0?'up':c<0?'down':'flat');
      tween(st,'vwapVal',+x.vwap||0, v=>st.vwap.textContent=fmtPrice(v));
      st.tr.textContent=x.trades??'—';
      st.we.textContent=(x.window_end||'').replace('T',' ').slice(0,19);
    });
    board.style.height=(rows.length*ROW)+'px';
  }catch(e){}
}
async function logTick(){
  try{
    const rows=await (await fetch('/api/logs',{cache:'no-store'})).json();
    let added=false;
    for(const r of rows){ if(seen.has(r.id)) continue; seen.add(r.id);
      const ln=el('div','ln'); ln.title=r.msg;
      ln.appendChild(el('span','lt',r.ts));
      ln.appendChild(el('span','lc c-'+r.comp,'['+r.comp+']'));
      ln.appendChild(el('span','sev-'+r.severity, r.msg));
      logEl.appendChild(ln); added=true; }
    if(added){ while(logEl.childElementCount>300) logEl.removeChild(logEl.firstChild);
      if(follow) logEl.scrollTop=logEl.scrollHeight; }
  }catch(e){}
}
setInterval(()=>{ if(Date.now()-rotate>10000) setTab(tab==='volume'?'movers':'volume'); },1000);
setInterval(boardTick, 500); boardTick();
setInterval(logTick, 1000); logTick();
</script></body></html>"""


def http_get(path: str):
    req = urllib.request.Request(EMULATOR + path)
    req.add_header("Authorization", "Bearer demo-token")
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read() or b"{}")


def http_post(path: str, body: dict):
    req = urllib.request.Request(EMULATOR + path, data=json.dumps(body).encode(),
                                 method="POST")
    req.add_header("Authorization", "Bearer demo-token")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read() or b"{}")


def value(field: dict):
    for key in ("stringValue", "doubleValue", "integerValue", "timestampValue",
                "booleanValue"):
        if key in field:
            v = field[key]
            return int(v) if key == "integerValue" else v
    return None


def leaderboard_rows() -> list[dict]:
    data = http_get(f"/v1/projects/{PROJECT}/databases/(default)/documents/{COLLECTION}")
    rows = [{k: value(v) for k, v in doc.get("fields", {}).items()}
            for doc in data.get("documents", [])]
    rows.sort(key=lambda r: float(r.get("quote_volume") or 0), reverse=True)
    return rows


COMPONENT = {"bridge.ingest": "bridge", "publisher.delivery": "publisher",
             "medallion.window": "spark", "rollup.submit": "scheduler",
             "bigquery.load": "bigquery"}


def format_log(event: str, p: dict) -> str:
    if event == "bridge.ingest":
        return f"produced {p.get('produced')} trades · total {p.get('total')} · {p.get('mode')}"
    if event == "publisher.delivery":
        return f"{p.get('symbols')} symbols · {p.get('trades')} trades · top {p.get('top')}"
    if event == "medallion.window":
        return f"window committed · {p.get('symbols')} symbols · top {p.get('top')}"
    if event == "rollup.submit":
        return f"submitted {p.get('job')}"
    if event == "bigquery.load":
        return f"loaded {p.get('rows')} rows → {p.get('table')}"
    return " ".join(f"{k}={v}" for k, v in p.items() if k != "event")


def log_rows() -> list[dict]:
    data = http_post("/v2/entries:list", {
        "resourceNames": [f"projects/{PROJECT}"],
        "orderBy": "timestamp desc",
        "pageSize": 60,
    })
    import datetime
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=5)
    out = []
    for e in data.get("entries", []):
        p = e.get("jsonPayload") or {}
        event = p.get("event")
        if not event:
            continue
        ts = e.get("timestamp", "")
        try:
            if datetime.datetime.fromisoformat(ts.replace("Z", "+00:00")) < cutoff:
                continue  # keep the tail fresh across restarts
        except ValueError:
            pass
        out.append({
            "id": e.get("insertId") or (ts + event),
            "ts": ts[11:19],
            "severity": e.get("severity", "INFO"),
            "comp": COMPONENT.get(event, event.split(".")[0]),
            "msg": format_log(event, p),
        })
    out.reverse()  # oldest first, so the page appends to the tail
    return out


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
        elif self.path.startswith("/api/logs"):
            try:
                body = json.dumps(log_rows()).encode()
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
