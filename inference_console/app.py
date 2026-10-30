from __future__ import annotations

import asyncio
import json
import subprocess
import time
import uuid
from collections import deque
from datetime import datetime
from typing import Any
from urllib.parse import quote

import httpx
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
import uvicorn

OLLAMA_UPSTREAM = "http://127.0.0.1:11434"
LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 11435
MAX_EVENTS = 40
MAX_CAPTURE_CHARS = 250_000

app = FastAPI(title="Ollama Inference Monitor", docs_url=None, redoc_url=None)
http_client: httpx.AsyncClient | None = None
history: deque[dict[str, Any]] = deque(maxlen=MAX_EVENTS)
clients: set[WebSocket] = set()


def now_text() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")


def safe_display(value: Any, key: str = "") -> Any:
    """Make a display-only copy; original request bytes are forwarded untouched."""
    k = key.lower()
    if k in {"image_url", "images", "image"}:
        if isinstance(value, str):
            return f"[图像数据已隐藏，原始字符数 {len(value):,}]"
        if isinstance(value, list):
            return [safe_display(v, "image") for v in value]
        if isinstance(value, dict):
            if "url" in value and isinstance(value["url"], str):
                url = value["url"]
                if url.startswith("data:"):
                    return {"url": f"[Base64 图像已隐藏，原始字符数 {len(url):,}]"}
            return {kk: safe_display(vv, kk) for kk, vv in value.items()}
    if isinstance(value, dict):
        return {str(kk): safe_display(vv, str(kk)) for kk, vv in value.items()}
    if isinstance(value, list):
        return [safe_display(v, key) for v in value]
    if isinstance(value, str) and len(value) > MAX_CAPTURE_CHARS:
        return value[:MAX_CAPTURE_CHARS] + f"\n…[监控显示截断，原文共 {len(value):,} 字符；请求本身未截断]"
    return value


async def broadcast(message: dict[str, Any]) -> None:
    dead: list[WebSocket] = []
    for ws in list(clients):
        try:
            await ws.send_json(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)


def append_captured(event: dict[str, Any], field: str, text: str) -> None:
    current = event.get(field, "")
    if len(current) < MAX_CAPTURE_CHARS:
        event[field] = current + text[: max(0, MAX_CAPTURE_CHARS - len(current))]


async def add_text(event: dict[str, Any], field: str, text: str) -> None:
    if not text:
        return
    append_captured(event, field, text)
    await broadcast({"type": "delta", "id": event["id"], "field": field, "text": text})


def new_event(body: Any, raw_body: bytes, path: str) -> dict[str, Any]:
    model = body.get("model", "(未声明)") if isinstance(body, dict) else "(非 JSON 请求)"
    display_body = safe_display(body) if body is not None else {
        "raw_body": raw_body[:MAX_CAPTURE_CHARS].decode("utf-8", errors="replace")
    }
    ev = {
        "id": uuid.uuid4().hex[:12],
        "time": now_text(),
        "model": model,
        "path": path,
        "status": "运行中",
        "request": display_body,
        "thinking": "",
        "output": "",
        "raw_content": "",
        "_content_buffer": "",
        "_inside_think": False,
        "tool_calls": "",
        "usage": None,
        "error": "",
        "duration": None,
    }
    history.appendleft(ev)
    return ev


def extract_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        pieces = []
        for part in value:
            if isinstance(part, dict):
                if isinstance(part.get("text"), str):
                    pieces.append(part["text"])
                elif part.get("type") in {"image_url", "image"}:
                    pieces.append("[图像内容]")
            else:
                pieces.append(str(part))
        return "".join(pieces)
    return json.dumps(value, ensure_ascii=False)


async def add_model_content(event: dict[str, Any], text: str) -> None:
    """Capture the exact content stream, splitting visible <think> tags when present."""
    if not text:
        return
    append_captured(event, "raw_content", text)
    await broadcast({"type": "delta", "id": event["id"], "field": "raw_content", "text": text})
    buf = event.get("_content_buffer", "") + text
    open_tag, close_tag = "<think>", "</think>"
    while buf:
        inside = bool(event.get("_inside_think"))
        if inside:
            idx = buf.find(close_tag)
            if idx >= 0:
                if idx:
                    await add_text(event, "thinking", buf[:idx])
                buf = buf[idx + len(close_tag):]
                event["_inside_think"] = False
            else:
                safe_len = max(0, len(buf) - len(close_tag) + 1)
                if safe_len:
                    await add_text(event, "thinking", buf[:safe_len])
                    buf = buf[safe_len:]
                break
        else:
            idx = buf.find(open_tag)
            if idx >= 0:
                if idx:
                    await add_text(event, "output", buf[:idx])
                buf = buf[idx + len(open_tag):]
                event["_inside_think"] = True
            else:
                safe_len = max(0, len(buf) - len(open_tag) + 1)
                if safe_len:
                    await add_text(event, "output", buf[:safe_len])
                    buf = buf[safe_len:]
                break
    event["_content_buffer"] = buf


async def flush_model_content(event: dict[str, Any]) -> None:
    buf = event.get("_content_buffer", "")
    if buf:
        field = "thinking" if event.get("_inside_think") else "output"
        await add_text(event, field, buf)
    event["_content_buffer"] = ""


async def parse_openai_chunk(event: dict[str, Any], payload: dict[str, Any]) -> None:
    usage = payload.get("usage")
    if usage is not None:
        event["usage"] = usage
        await broadcast({"type": "usage", "id": event["id"], "usage": usage})
    choices = payload.get("choices") or []
    for choice in choices:
        delta = choice.get("delta") or choice.get("message") or {}
        # Some model/API combinations expose reasoning as a dedicated field.
        for key in ("reasoning", "reasoning_content", "thinking"):
            value = delta.get(key)
            if value:
                await add_text(event, "thinking", extract_text(value))
        content = delta.get("content")
        if content:
            await add_model_content(event, extract_text(content))
        tool_calls = delta.get("tool_calls")
        if tool_calls:
            text = json.dumps(tool_calls, ensure_ascii=False)
            await add_text(event, "tool_calls", text + "\n")
        finish_reason = choice.get("finish_reason")
        if finish_reason:
            event["finish_reason"] = finish_reason


async def parse_nonstream_response(event: dict[str, Any], payload: Any) -> None:
    if not isinstance(payload, dict):
        return
    choices = payload.get("choices") or []
    if choices:
        message = choices[0].get("message") or {}
        for key in ("reasoning", "reasoning_content", "thinking"):
            if message.get(key):
                await add_text(event, "thinking", extract_text(message[key]))
        if message.get("content"):
            await add_model_content(event, extract_text(message["content"]))
            await flush_model_content(event)
        if message.get("tool_calls"):
            await add_text(event, "tool_calls", json.dumps(message["tool_calls"], ensure_ascii=False))
        event["finish_reason"] = choices[0].get("finish_reason")
    if payload.get("usage") is not None:
        event["usage"] = payload["usage"]
        await broadcast({"type": "usage", "id": event["id"], "usage": payload["usage"]})


@app.on_event("startup")
async def startup() -> None:
    global http_client
    http_client = httpx.AsyncClient(timeout=None, follow_redirects=False, trust_env=False)


@app.on_event("shutdown")
async def shutdown() -> None:
    if http_client:
        await http_client.aclose()


@app.get("/", response_class=HTMLResponse)
async def dashboard() -> str:
    return HTML


@app.get("/api/health")
async def health() -> dict[str, Any]:
    return {"ok": True, "mode": "passive", "upstream": OLLAMA_UPSTREAM, "listen_port": LISTEN_PORT, "ollama_probe": False}


@app.get("/api/events")
async def get_events() -> list[dict[str, Any]]:
    return list(history)


@app.get("/api/gpu")
async def gpu_status() -> dict[str, Any]:
    command = [
        "nvidia-smi",
        "--query-gpu=utilization.gpu,memory.used,memory.total,power.draw,temperature.gpu,clocks.gr,clocks.mem",
        "--format=csv,noheader,nounits",
    ]
    try:
        result = await asyncio.to_thread(
            subprocess.run, command, capture_output=True, text=True, timeout=3, check=False
        )
        if result.returncode != 0:
            return {"ok": False, "error": (result.stderr or result.stdout).strip() or "nvidia-smi 执行失败"}
        line = next((x.strip() for x in result.stdout.splitlines() if x.strip()), "")
        cols = [x.strip() for x in line.split(",")]
        if len(cols) < 7:
            return {"ok": False, "error": f"无法解析 nvidia-smi 输出：{line}"}

        def number(s: str) -> float | None:
            try:
                return float(s.replace("%", "").replace("W", "").strip())
            except (ValueError, AttributeError):
                return None

        data = {
            "ok": True,
            "gpu_util": number(cols[0]),
            "memory_used": number(cols[1]),
            "memory_total": number(cols[2]),
            "power": number(cols[3]),
            "temperature": number(cols[4]),
            "clock_core": number(cols[5]),
            "clock_memory": number(cols[6]),
            "time": now_text(),
        }
        used = data["memory_used"]
        total = data["memory_total"]
        data["memory_percent"] = round(used * 100 / total, 1) if used is not None and total else None

        # Process-level GPU memory is best-effort; Windows WDDM may report N/A.
        p = await asyncio.to_thread(
            subprocess.run,
            ["nvidia-smi", "--query-compute-apps=pid,process_name,used_memory", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=3, check=False,
        )
        processes = []
        if p.returncode == 0:
            for row in p.stdout.splitlines():
                parts = [x.strip() for x in row.split(",", 2)]
                if len(parts) == 3:
                    processes.append({"pid": parts[0], "name": parts[1], "memory": parts[2]})
        data["processes"] = processes
        return data
    except FileNotFoundError:
        return {"ok": False, "error": "未找到 nvidia-smi。请确认已安装 NVIDIA 驱动，并且 nvidia-smi 可在 PowerShell 中运行。"}
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "nvidia-smi 查询超时"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    clients.add(ws)
    try:
        await ws.send_json({"type": "snapshot", "events": list(history)})
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        clients.discard(ws)
    except Exception:
        clients.discard(ws)


@app.api_route("/v1/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
async def ollama_proxy(request: Request, path: str) -> Response:
    assert http_client is not None
    raw_body = await request.body()
    try:
        body_obj = json.loads(raw_body.decode("utf-8")) if raw_body else None
    except Exception:
        body_obj = None

    track = request.method.upper() == "POST" and path in {"chat/completions", "completions", "responses"}
    event = new_event(body_obj, raw_body, "/v1/" + path) if track else None
    if event:
        await broadcast({"type": "new", "event": event})

    target_url = f"{OLLAMA_UPSTREAM}/v1/{path}"
    if request.url.query:
        target_url += "?" + request.url.query
    drop_headers = {
        "host", "content-length", "connection", "keep-alive", "proxy-authenticate",
        "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade",
        "accept-encoding",
    }
    headers = {k: v for k, v in request.headers.items() if k.lower() not in drop_headers}
    headers["accept-encoding"] = "identity"
    started = time.perf_counter()
    outbound = http_client.build_request(
        request.method, target_url, content=raw_body if raw_body else None, headers=headers
    )
    try:
        upstream = await http_client.send(outbound, stream=True)
    except Exception as exc:
        if event:
            event["status"] = "连接上游失败"
            event["error"] = str(exc)
            event["duration"] = round(time.perf_counter() - started, 3)
            await broadcast({"type": "done", "id": event["id"], "event": event})
        return JSONResponse({"error": f"无法连接本机 Ollama：{exc}"}, status_code=502)

    response_headers = {}
    for key in ("content-type", "cache-control", "retry-after"):
        if key in upstream.headers:
            response_headers[key] = upstream.headers[key]

    content_type = upstream.headers.get("content-type", "")
    if "text/event-stream" in content_type.lower():
        async def stream_body():
            try:
                async for line in upstream.aiter_lines():
                    if event and line.startswith("data:"):
                        data = line[5:].strip()
                        if data and data != "[DONE]":
                            try:
                                payload = json.loads(data)
                                await parse_openai_chunk(event, payload)
                            except Exception:
                                pass
                        elif data == "[DONE]":
                            await flush_model_content(event)
                            event["status"] = "完成"
                            event["duration"] = round(time.perf_counter() - started, 3)
                            await broadcast({"type": "done", "id": event["id"], "event": event})
                    # Reconstruct a valid SSE line. Empty lines preserve event boundaries.
                    yield (line + "\n").encode("utf-8")
            except Exception as exc:
                if event:
                    event["status"] = "流式读取异常"
                    event["error"] = str(exc)
                    await broadcast({"type": "error", "id": event["id"], "error": str(exc)})
            finally:
                await upstream.aclose()
                if event and event["status"] == "运行中":
                    await flush_model_content(event)
                    event["status"] = "连接结束"
                    event["duration"] = round(time.perf_counter() - started, 3)
                    await broadcast({"type": "done", "id": event["id"], "event": event})

        return StreamingResponse(
            stream_body(), status_code=upstream.status_code,
            headers=response_headers, media_type=None,
        )

    try:
        content = await upstream.aread()
        status_code = upstream.status_code
    finally:
        await upstream.aclose()
    if event:
        try:
            payload = json.loads(content.decode("utf-8"))
            await parse_nonstream_response(event, payload)
            event["status"] = "完成" if status_code < 400 else f"HTTP {status_code}"
        except Exception as exc:
            event["status"] = f"HTTP {status_code}"
            event["error"] = f"响应解析失败：{exc}"
        event["duration"] = round(time.perf_counter() - started, 3)
        await broadcast({"type": "done", "id": event["id"], "event": event})

    out_headers = {k: v for k, v in response_headers.items()}
    return Response(content=content, status_code=status_code, headers=out_headers)


HTML = r'''<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ollama 推理监控台 v0.1.1</title>
<style>
:root{color-scheme:dark;--bg:#0b1020;--panel:#131b2e;--panel2:#0f1728;--line:#27334a;--muted:#91a0b8;--text:#e8eef8;--green:#38d996;--blue:#5ba8ff;--orange:#ffb454;--red:#ff6b79}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 "Segoe UI",Microsoft YaHei,sans-serif}
header{display:flex;justify-content:space-between;align-items:center;padding:16px 22px;border-bottom:1px solid var(--line);background:#0e1627;position:sticky;top:0;z-index:5}
h1{font-size:19px;margin:0;font-weight:650}.sub{color:var(--muted);font-size:12px}.pill{border:1px solid var(--line);border-radius:999px;padding:4px 10px;color:var(--muted);font-size:12px}.pill.ok{color:var(--green);border-color:#27664e}
main{padding:18px;max-width:1800px;margin:auto}.section-title{display:flex;justify-content:space-between;align-items:center;margin:5px 0 12px}.section-title h2{font-size:15px;margin:0}.gpu-grid{display:grid;grid-template-columns:repeat(4,minmax(150px,1fr));gap:12px}.card{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:13px;min-width:0}.metric-label{font-size:12px;color:var(--muted)}.metric-value{font-size:25px;font-weight:700;margin:4px 0}.metric-sub{font-size:11px;color:var(--muted)}.charts{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px}.chartbox{height:160px}.chartbox canvas{width:100%;height:110px;display:block}.chart-head{display:flex;justify-content:space-between;color:var(--muted);font-size:12px}.lower-gpu{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px}.mono{font:12px/1.5 Consolas,"Cascadia Code",monospace;white-space:pre-wrap;overflow-wrap:anywhere}.small{font-size:12px;color:var(--muted)}
.work-grid{display:grid;grid-template-columns:300px minmax(0,1fr);gap:12px;margin-top:22px}.requests{max-height:720px;overflow:auto}.request-item{padding:11px;border-bottom:1px solid var(--line);cursor:pointer}.request-item:hover,.request-item.active{background:#1b2941}.request-item strong{display:block;font-size:12px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.request-item span{color:var(--muted);font-size:11px}.details{display:grid;grid-template-columns:1fr 1fr;gap:12px;min-width:0}.pane{background:var(--panel);border:1px solid var(--line);border-radius:12px;overflow:hidden;min-width:0}.pane-head{padding:10px 12px;border-bottom:1px solid var(--line);font-weight:650;display:flex;justify-content:space-between;align-items:center}.pane-body{padding:12px;max-height:340px;overflow:auto}.pane-body pre{margin:0;white-space:pre-wrap;overflow-wrap:anywhere;font:12px/1.55 Consolas,"Cascadia Code",monospace;color:#d9e5f5}.wide{grid-column:1/-1}.empty{color:var(--muted);padding:28px;text-align:center}.warning{color:var(--orange)}.good{color:var(--green)}.error{color:var(--red)}.tag{font-size:11px;border:1px solid var(--line);border-radius:5px;padding:2px 6px;color:var(--muted)}
@media(max-width:1000px){.gpu-grid{grid-template-columns:repeat(2,1fr)}.work-grid{grid-template-columns:1fr}.requests{max-height:250px}.details{grid-template-columns:1fr}}
@media(max-width:600px){main{padding:10px}.charts,.lower-gpu{grid-template-columns:1fr}.gpu-grid{gap:8px}.metric-value{font-size:21px}}
</style></head>
<body>
<header><div><h1>Ollama 推理监控台 <span class="tag">v0.1.1 被动监控</span></h1><div class="sub">GPU 实时遥测 · Ollama API 请求 · 流式生成内容</div></div><div style="display:flex;gap:9px;align-items:center"><span id="proxyStatus" class="pill">代理连接中…</span><span class="pill">本地监控 · 不保存到磁盘</span></div></header>
<main>
<div class="section-title"><h2>01 / GPU 硬件监控</h2><span class="small" id="gpuTime">等待 NVIDIA-SMI…</span></div>
<div class="gpu-grid">
 <div class="card"><div class="metric-label">GPU 利用率</div><div class="metric-value" id="gpuUtil">--%</div><div class="metric-sub">实时计算负载</div></div>
 <div class="card"><div class="metric-label">显存使用</div><div class="metric-value" id="gpuMem">-- / -- GB</div><div class="metric-sub" id="gpuMemPct">显存占用 --</div></div>
 <div class="card"><div class="metric-label">GPU 温度</div><div class="metric-value" id="gpuTemp">-- °C</div><div class="metric-sub">核心温度</div></div>
 <div class="card"><div class="metric-label">GPU 功耗</div><div class="metric-value" id="gpuPower">-- W</div><div class="metric-sub" id="gpuClock">核心频率 -- MHz · 显存频率 -- MHz</div></div>
</div>
<div class="charts">
 <div class="card chartbox"><div class="chart-head"><span>GPU 利用率历史</span><span>最近 90 秒</span></div><canvas id="gpuChart"></canvas></div>
 <div class="card chartbox"><div class="chart-head"><span>显存占用历史</span><span>最近 90 秒</span></div><canvas id="memChart"></canvas></div>
</div>
<div class="lower-gpu">
 <div class="card"><div class="section-title"><h2>GPU 进程</h2><span class="tag">NVIDIA-SMI</span></div><div id="gpuProcesses" class="mono small">正在读取进程…</div></div>
 <div class="card"><div class="section-title"><h2>Ollama 状态</h2><span class="tag">被动模式</span></div><div id="ollamaPs" class="mono small">监控台启动时不会探测或连接 Ollama。只有 AstrBot 请求经过本代理时，才会与 Ollama 通信；模型名称可在下方请求记录中查看。</div></div>
</div>
<div class="section-title" style="margin-top:25px"><h2>02 / 模型运行与 API 监控</h2><span class="small">显示实际发送到 Ollama 的请求与实际返回的数据</span></div>
<div class="work-grid">
 <div class="card requests"><div class="section-title"><h2>请求记录</h2><span id="requestCount" class="tag">0</span></div><div id="requestList" class="small">等待 AstrBot 请求…</div></div>
 <div id="detailArea" class="details"><div class="pane wide"><div class="empty">尚无模型请求。将 AstrBot 的 Ollama API 地址改为本监控代理后，在 QQ 中触发一次 code_developer 任务，这里就会显示实际请求。</div></div></div>
</div>
<p class="small" style="margin:14px 2px 4px">说明：这里显示模型/API 实际传输的内容。只有模型通过 API 返回的 reasoning/thinking 字段或文本标记才会出现在“思考内容”中；不会读取模型未输出的内部状态。</p>
</main>
<script>
const $=id=>document.getElementById(id);let events=[];let selected=null;let ws;const gpuHist=[],memHist=[];
function fmt(n,d=0){return n==null?'--':Number(n).toFixed(d)}
function drawChart(canvasId,values,maxValue,label){const c=$(canvasId),ctx=c.getContext('2d'),r=c.getBoundingClientRect(),dpr=devicePixelRatio||1;c.width=Math.max(300,r.width*dpr);c.height=110*dpr;ctx.scale(dpr,dpr);const w=r.width,h=110;ctx.clearRect(0,0,w,h);ctx.strokeStyle='#26334a';ctx.lineWidth=1;for(let i=0;i<4;i++){let y=8+i*(h-22)/3;ctx.beginPath();ctx.moveTo(0,y);ctx.lineTo(w,y);ctx.stroke()}if(values.length<2)return;ctx.strokeStyle=label==='GPU'?'#5ba8ff':'#38d996';ctx.lineWidth=2;ctx.beginPath();values.forEach((v,i)=>{let x=i*(w-4)/(values.length-1),y=h-10-(Math.max(0,Math.min(maxValue,v))/maxValue)*(h-24);if(i===0)ctx.moveTo(x,y);else ctx.lineTo(x,y)});ctx.stroke()}
async function refreshGpu(){try{let res=await fetch('/api/gpu');let d=await res.json();if(!d.ok){$('gpuTime').textContent=d.error||'读取失败';return} $('gpuUtil').textContent=fmt(d.gpu_util)+'%';$('gpuMem').textContent=`${fmt(d.memory_used/1024,1)} / ${fmt(d.memory_total/1024,1)} GB`;$('gpuMemPct').textContent=`显存占用 ${fmt(d.memory_percent,1)}%`;$('gpuTemp').textContent=fmt(d.temperature)+' °C';$('gpuPower').textContent=fmt(d.power,0)+' W';$('gpuClock').textContent=`核心 ${fmt(d.clock_core)} MHz · 显存 ${fmt(d.clock_memory)} MHz`;$('gpuTime').textContent='更新于 '+d.time;gpuHist.push(d.gpu_util||0);memHist.push(d.memory_percent||0);if(gpuHist.length>90)gpuHist.shift();if(memHist.length>90)memHist.shift();drawChart('gpuChart',gpuHist,100,'GPU');drawChart('memChart',memHist,100,'MEM');$('gpuProcesses').textContent=d.processes&&d.processes.length?d.processes.map(p=>`${p.name}  PID ${p.pid}  ${p.memory} MB`).join('\n'):'当前驱动未提供进程级显存列表（WDDM 下可能如此）；上方 GPU/显存总量仍有效。';}catch(e){$('gpuTime').textContent='GPU 监控连接失败：'+e}}
function statusColor(s){return s==='完成'?'good':s==='运行中'?'warning':'error'}
function renderList(){ $('requestCount').textContent=events.length;const root=$('requestList');root.innerHTML='';if(!events.length){root.textContent='等待 AstrBot 请求…';return}events.forEach(ev=>{let el=document.createElement('div');el.className='request-item'+(ev.id===selected?' active':'');let title=document.createElement('strong');title.textContent=ev.model||'模型请求';let meta=document.createElement('span');meta.textContent=`${ev.time} · ${ev.status} · ${ev.id}`;el.append(title,meta);el.onclick=()=>{selected=ev.id;renderList();renderDetails()};root.append(el)})}
function renderDetails(){const ev=events.find(e=>e.id===selected);const root=$('detailArea');root.innerHTML='';if(!ev){root.innerHTML='<div class="pane wide"><div class="empty">选择一条请求查看详情</div></div>';return}
 function pane(title,content,wide=false){let box=document.createElement('section');box.className='pane'+(wide?' wide':'');let head=document.createElement('div');head.className='pane-head';head.textContent=title;let body=document.createElement('div');body.className='pane-body';let pre=document.createElement('pre');pre.textContent=content||'（暂无内容）';body.append(pre);box.append(head,body);root.append(box)}
 pane(`收到的请求 · ${ev.model} · ${ev.status}`,JSON.stringify(ev.request,null,2),true);pane('模型思考 / reasoning 字段',ev.thinking);pane('模型传出内容 / content（已分离 think 标签）',ev.output);pane('原始 content 流（含 think 标签）',ev.raw_content);pane('工具调用增量',ev.tool_calls);if(ev.error)pane('错误',ev.error,true);if(ev.usage)pane('Token 用量',JSON.stringify(ev.usage,null,2));
}
function applyEvent(msg){if(msg.type==='snapshot'){events=msg.events||[];if(!selected&&events.length)selected=events[0].id;renderList();renderDetails();return}if(msg.type==='new'){events.unshift(msg.event);events=events.slice(0,40);selected=msg.event.id;renderList();renderDetails();return}let ev=events.find(e=>e.id===msg.id);if(!ev)return;if(msg.type==='delta'){ev[msg.field]=(ev[msg.field]||'')+msg.text;if(msg.id===selected)renderDetails()}else if(msg.type==='done'){Object.assign(ev,msg.event||{});renderList();if(msg.id===selected)renderDetails()}else if(msg.type==='error'){ev.error=msg.error;ev.status='异常';renderList();if(msg.id===selected)renderDetails()}else if(msg.type==='usage'){ev.usage=msg.usage;if(msg.id===selected)renderDetails()}}
function connect(){let proto=location.protocol==='https:'?'wss':'ws';ws=new WebSocket(`${proto}://${location.host}/ws`);ws.onopen=()=>{$('proxyStatus').textContent='API 监控已连接';$('proxyStatus').className='pill ok'};ws.onclose=()=>{$('proxyStatus').textContent='监控连接断开，重连中';$('proxyStatus').className='pill';setTimeout(connect,2000)};ws.onmessage=e=>{try{applyEvent(JSON.parse(e.data))}catch(_){}}}
connect();refreshGpu();setInterval(refreshGpu,1000);window.addEventListener('resize',()=>{drawChart('gpuChart',gpuHist,100,'GPU');drawChart('memChart',memHist,100,'MEM')});
</script></body></html>'''



if __name__ == "__main__":
    print("Ollama 推理监控台 v0.1.1 启动中…")
    print("被动监控模式：启动时不探测 Ollama；仅在收到 /v1 请求时转发。")
    print(f"本机面板：http://127.0.0.1:{LISTEN_PORT}/")
    print(f"局域网访问：http://<你的本地 IP>:{LISTEN_PORT}/v1  （按需修改）")
    print("上游 Ollama：http://127.0.0.1:11434")
    print("如需通过公网或其他设备访问，请在 app.py 中修改 LISTEN_HOST 为 0.0.0.0，")
    print("并在 OpenAPI 配置中填写正确的 Web 端口。")
    uvicorn.run(app, host=LISTEN_HOST, port=LISTEN_PORT, log_level="info")
