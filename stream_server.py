"""
stream_server.py — WebSocket 기반 다중 폰 스트리밍 서버

설치: pip install aiohttp python-dotenv qrcode[pil] requests
실행: python stream_server.py
"""

import asyncio
import atexit
import os
import re
import socket
import subprocess
import time
import uuid

import qrcode
import requests
from aiohttp import web, WSMsgType
from dotenv import load_dotenv

load_dotenv()

PORT           = int(os.getenv("PORT", "5001"))
STREAM_FPS     = int(os.getenv("STREAM_FPS", "15"))
SPRING_URL     = os.getenv("SPRING_URL", "")
USE_CLOUDFLARE = os.getenv("USE_CLOUDFLARE", "false") == "true"

broadcasters: dict[str, "FrameBroadcaster"] = {}


# ── FrameBroadcaster ───────────────────────────────────
class FrameBroadcaster:
    def __init__(self, drone_id: str):
        self.drone_id      = drone_id
        self._frame: bytes | None = None
        self._client_count = 0
        self._recv_fps     = 0.0
        self._frame_count  = 0
        self._last_fps_time = time.monotonic()

    async def push(self, frame: bytes) -> None:
        self._frame = frame
        self._frame_count += 1
        now = time.monotonic()
        elapsed = now - self._last_fps_time
        if elapsed >= 1.0:
            self._recv_fps = round(self._frame_count / elapsed, 1)
            self._frame_count = 0
            self._last_fps_time = now

    async def stream(self, fps: int):
        interval   = 1.0 / fps
        last_frame = None
        self._client_count += 1
        try:
            while True:
                frame = self._frame
                if frame is not None and frame is not last_frame:
                    last_frame = frame
                    yield frame
                await asyncio.sleep(interval)
        finally:
            self._client_count -= 1

    def stats(self) -> dict:
        return {
            "drone_id": self.drone_id,
            "recv_fps": self._recv_fps,
            "clients":  self._client_count,
            "has_frame": self._frame is not None,
        }


# ── 유틸 ───────────────────────────────────────────────
def get_local_ip() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"


def get_base_url(request: web.Request) -> str:
    """터널 경유 시 public URL, 아닐 시 로컬 URL 반환"""
    proto = request.headers.get("X-Forwarded-Proto", "http")
    host  = request.headers.get("X-Forwarded-Host", request.host)
    return f"{proto}://{host}"


def open_qr(url: str) -> None:
    """QR 이미지를 /tmp에 저장 후 macOS Preview로 자동 오픈"""
    try:
        img  = qrcode.make(url)
        path = "/tmp/stream_qr.png"
        img.save(path)
        subprocess.Popen(["open", path])
        print("  QR이 Preview에서 열렸습니다 → 폰 카메라로 스캔하세요")
    except Exception as e:
        print(f"  QR 생성 실패: {e}")


def start_cloudflare(port: int) -> str | None:
    """cloudflared quick tunnel — 계정 불필요, brew install cloudflared"""
    try:
        proc = subprocess.Popen(
            ["cloudflared", "tunnel", "--url", f"http://localhost:{port}", "--no-autoupdate"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        atexit.register(proc.terminate)

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            line  = proc.stdout.readline().decode("utf-8", errors="ignore")
            match = re.search(r"https://[\w\-]+\.trycloudflare\.com", line)
            if match:
                return match.group(0)
        print("[cloudflare] URL 파싱 실패")
        return None
    except FileNotFoundError:
        print("[cloudflare] 설치 필요: brew install cloudflared")
        return None
    except Exception as e:
        print(f"[cloudflare] 실패: {e}")
        return None


def notify_spring(drone_id: str, stream_url: str, connected: bool) -> None:
    if not SPRING_URL:
        return
    try:
        requests.post(
            f"{SPRING_URL}/api/v1/drone-callback/stream",
            json={"droneId": drone_id, "streamUrl": stream_url, "connected": connected},
            timeout=3,
        )
    except Exception as e:
        print(f"[Spring] 알림 실패: {e}")


# ── 라우트 핸들러 ──────────────────────────────────────
async def handle_ws(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse(max_msg_size=5 * 1024 * 1024)
    await ws.prepare(request)

    drone_id    = "drone-" + uuid.uuid4().hex[:8]
    broadcaster = FrameBroadcaster(drone_id)
    broadcasters[drone_id] = broadcaster

    stream_url = f"{get_base_url(request)}/video/{drone_id}"
    print(f"[{drone_id}] 연결 → {stream_url}")

    asyncio.create_task(asyncio.to_thread(notify_spring, drone_id, stream_url, True))
    await ws.send_json({"droneId": drone_id, "streamUrl": stream_url})

    try:
        async for msg in ws:
            if msg.type == WSMsgType.BINARY:
                await broadcaster.push(msg.data)
            elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE):
                break
    finally:
        broadcasters.pop(drone_id, None)
        print(f"[{drone_id}] 종료")
        asyncio.create_task(asyncio.to_thread(notify_spring, drone_id, stream_url, False))

    return ws


async def handle_video(request: web.Request) -> web.StreamResponse:
    drone_id    = request.match_info["drone_id"]
    broadcaster = broadcasters.get(drone_id)
    if broadcaster is None:
        raise web.HTTPNotFound(text=f"드론 '{drone_id}' 없음")

    client_fps = min(int(request.rel_url.query.get("fps", STREAM_FPS)), STREAM_FPS)
    response   = web.StreamResponse(headers={
        "Content-Type": "multipart/x-mixed-replace; boundary=frame",
        "Cache-Control": "no-cache",
    })
    await response.prepare(request)

    try:
        async for frame in broadcaster.stream(client_fps):
            length = str(len(frame)).encode()
            await response.write(
                b"--frame\r\n"
                b"Content-Type: image/jpeg\r\n"
                b"Content-Length: " + length + b"\r\n"
                b"\r\n" + frame + b"\r\n"
            )
    except Exception:
        pass

    return response


async def handle_drones(request: web.Request) -> web.Response:
    base = get_base_url(request)
    return web.json_response([
        {**b.stats(), "stream_url": f"{base}/video/{did}"}
        for did, b in broadcasters.items()
    ])


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({
        "status": "ok",
        "active_drones": len(broadcasters),
        "stream_fps_max": STREAM_FPS,
    })


async def handle_sender(request: web.Request) -> web.Response:
    html = """<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>드론 스트리밍</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { background:#111; color:#fff; font-family:sans-serif;
           display:flex; flex-direction:column; align-items:center; gap:10px; padding:14px; }
    video  { width:100%; max-width:480px; background:#000; border-radius:10px; aspect-ratio:4/3; }
    button { width:100%; max-width:480px; padding:14px; font-size:16px;
             border-radius:10px; border:none; background:#4A90E2; color:#fff; cursor:pointer; }
    button:disabled { background:#444; cursor:default; }
    #status { font-size:13px; color:#aaa; text-align:center; }
    #info   { font-size:12px; color:#FFD600; text-align:center; background:#1a1a1a;
              padding:8px 14px; border-radius:8px; display:none;
              width:100%; max-width:480px; word-break:break-all; }
    #fps    { font-size:12px; color:#888; }
    .ok  { color:#4CAF50 !important; }
    .err { color:#F44336 !important; }
  </style>
</head>
<body>
  <video id="video" autoplay playsinline muted></video>
  <button id="btn" onclick="start()">카메라 시작 및 연결</button>
  <p id="status">버튼을 눌러 시작하세요</p>
  <div id="info"></div>
  <p id="fps"></p>
  <script>
    const TARGET_FPS   = 12;
    const JPEG_QUALITY = 0.65;
    const WS_URL = (location.protocol === 'https:' ? 'wss' : 'ws') + '://' + location.host + '/ws';

    let ws = null, streaming = false, sending = false;
    let lastSent = 0, fpsCount = 0, fpsTimer = Date.now();
    const canvas = document.createElement('canvas');
    const ctx    = canvas.getContext('2d');
    const $      = id => document.getElementById(id);
    const setStatus = (msg, cls = '') => { $('status').className = cls; $('status').innerText = msg; };

    function connect() {
      ws = new WebSocket(WS_URL);
      ws.onopen    = () => setStatus('연결됨, 스트리밍 준비 중...');
      ws.onmessage = e => {
        try {
          const d = JSON.parse(e.data);
          $('info').style.display = 'block';
          $('info').innerText = '드론 ID: ' + d.droneId;
          setStatus('✅ 스트리밍 중', 'ok');
        } catch {}
      };
      ws.onclose = () => {
        streaming = false;
        setStatus('연결 끊김 — 3초 후 재연결...', 'err');
        setTimeout(connect, 3000);
      };
      ws.onerror = () => ws.close();
    }

    async function start() {
      $('btn').disabled = true;
      setStatus('카메라 초기화 중...');
      try {
        const stream = await navigator.mediaDevices.getUserMedia({
          video: { facingMode: 'environment', width: { ideal: 640 }, height: { ideal: 480 } },
          audio: false,
        });
        $('video').srcObject = stream;
        canvas.width = 640; canvas.height = 480;
        $('video').addEventListener('playing', () => {
          streaming = true;
          requestAnimationFrame(sendLoop);
        }, { once: true });
        connect();
      } catch (err) {
        setStatus('오류: ' + err.message, 'err');
        $('btn').disabled = false;
      }
    }

    function sendLoop(ts) {
      if (!streaming) return;
      requestAnimationFrame(sendLoop);
      if (ts - lastSent < 1000 / TARGET_FPS) return;
      lastSent = ts;
      if (!ws || ws.readyState !== WebSocket.OPEN || sending) return;
      sending = true;
      ctx.drawImage($('video'), 0, 0, 640, 480);
      canvas.toBlob(blob => {
        if (blob && ws.readyState === WebSocket.OPEN) {
          ws.send(blob);
          fpsCount++;
          if (Date.now() - fpsTimer >= 1000) {
            $('fps').innerText = '전송 fps: ' + fpsCount;
            fpsCount = 0; fpsTimer = Date.now();
          }
        }
        sending = false;
      }, 'image/jpeg', JPEG_QUALITY);
    }
  </script>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


async def handle_test(request: web.Request) -> web.Response:
    html = b"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Stream Monitor</title>
  <style>
    body { background:#111; color:#fff; font-family:sans-serif; padding:20px; }
    .grid { display:flex; flex-wrap:wrap; gap:12px; margin-top:16px; }
    .card { background:#1a1a1a; border-radius:10px; padding:10px; }
    .card img { width:320px; border-radius:6px; display:block; }
    .card p { font-size:12px; color:#aaa; margin-top:6px; }
    #empty { color:#666; }
  </style>
</head>
<body>
  <h2>Drone Stream Monitor</h2>
  <p id="empty">No active drones.</p>
  <div class="grid" id="grid"></div>
  <script>
    async function refresh() {
      const drones = await fetch('/drones').then(r => r.json());
      document.getElementById('empty').style.display = drones.length ? 'none' : 'block';
      const grid = document.getElementById('grid');
      drones.forEach(d => {
        if (document.getElementById('card-' + d.drone_id)) return;
        const card = document.createElement('div');
        card.className = 'card';
        card.id = 'card-' + d.drone_id;
        card.innerHTML = `<img src="/video/${d.drone_id}"><p>${d.drone_id} | fps: ${d.recv_fps}</p>`;
        grid.appendChild(card);
      });
    }
    refresh();
    setInterval(refresh, 3000);
  </script>
</body>
</html>"""
    return web.Response(body=html, content_type="text/html")


# ── 미들웨어 & 앱 ──────────────────────────────────────
@web.middleware
async def common_headers(request: web.Request, handler):
    response = await handler(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response


def build_app() -> web.Application:
    app = web.Application(middlewares=[common_headers])
    app.router.add_get("/",               handle_sender)
    app.router.add_get("/ws",             handle_ws)
    app.router.add_get("/video/{drone_id}", handle_video)
    app.router.add_get("/drones",         handle_drones)
    app.router.add_get("/health",         handle_health)
    app.router.add_get("/test",           handle_test)
    return app


if __name__ == "__main__":
    local_url = f"http://{get_local_ip()}:{PORT}"

    public_url = None
    if USE_CLOUDFLARE:
        print("[cloudflare] 터널 시작 중 (최대 30초)...")
        public_url = start_cloudflare(PORT)
        if public_url:
            print(f"[cloudflare] {public_url}")
        else:
            print("[cloudflare] 실패 — 로컬 IP로 진행")

    best_url = public_url or local_url
    ws_url   = best_url.replace("https://", "wss://").replace("http://", "ws://")

    print()
    print("=" * 54)
    print("  스트림 서버 시작")
    print("=" * 54)
    print(f"  로컬 : {local_url}/")
    if public_url:
        print(f"  공인 : {public_url}/")
    print()
    open_qr(best_url)
    print(f"  URL  : {best_url}")
    print()
    print("  [엔드포인트]")
    print(f"  WebSocket : {ws_url}/ws")
    print(f"  스트림    : {best_url}/video/{{drone_id}}")
    print(f"  드론 목록 : {best_url}/drones")
    print(f"  모니터    : {best_url}/test")
    print(f"  헬스체크  : {best_url}/health")
    print("=" * 54)

    web.run_app(build_app(), host="0.0.0.0", port=PORT)