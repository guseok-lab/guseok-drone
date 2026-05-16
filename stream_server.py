"""
stream_server.py — WebSocket 기반 다중 폰 스트리밍 서버
다른 네트워크(LTE 등)에서도 동작, TURN 서버 불필요

설치: pip install aiohttp python-dotenv qrcode pyngrok
실행: python stream_server.py
"""

import asyncio
import os
import socket
import time
import uuid

import qrcode
import requests
from aiohttp import web, WSMsgType
from dotenv import load_dotenv

load_dotenv()

PORT       = int(os.getenv("PORT", "5001"))
STREAM_FPS = int(os.getenv("STREAM_FPS", "15"))
SPRING_URL = os.getenv("SPRING_URL", "")
USE_NGROK  = os.getenv("USE_NGROK", "false") == "true"

# 드론ID → FrameBroadcaster
broadcasters: dict[str, "FrameBroadcaster"] = {}


# ── FrameBroadcaster ───────────────────────────────────
class FrameBroadcaster:
    """
    asyncio.Future 기반 멀티클라이언트 브로드캐스터.
    WebSocket으로 수신한 JPEG 프레임을
    MJPEG 구독자 전원에게 동시 전달.
    """

    def __init__(self, drone_id: str):
        self.drone_id = drone_id
        self._frame: bytes | None = None
        self._client_count = 0
        self._recv_fps = 0.0
        self._frame_count = 0
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
        """
        MJPEG 스트림 제너레이터 — 클라이언트마다 독립 실행.
        폴링 방식: Future 레이스 컨디션 없이 안정적으로 동작.
        """
        interval = 1.0 / fps
        last_frame = None
        self._client_count += 1
        try:
            while True:
                frame = self._frame
                if frame is not None:
                    last_frame = frame
                    yield frame
                await asyncio.sleep(interval)
        finally:
            self._client_count -= 1

    def stats(self) -> dict:
        return {
            "drone_id": self.drone_id,
            "recv_fps": self._recv_fps,
            "clients": self._client_count,
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


def print_qr(url: str) -> None:
    qr = qrcode.QRCode(border=1)
    qr.add_data(url)
    qr.make(fit=True)
    qr.print_ascii(invert=True)


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


# ── WebSocket 수신 (폰 → 서버) ─────────────────────────
async def handle_ws(request: web.Request) -> web.WebSocketResponse:
    """
    GET /ws
    폰이 WebSocket으로 연결 → JPEG 바이너리 프레임 수신.
    연결 시 drone_id 자동 생성, 연결 종료 시 정리.
    """
    ws = web.WebSocketResponse(max_msg_size=5 * 1024 * 1024)  # 5MB
    await ws.prepare(request)

    drone_id = "drone-" + uuid.uuid4().hex[:8]
    broadcaster = FrameBroadcaster(drone_id)
    broadcasters[drone_id] = broadcaster

    host = request.headers.get("X-Forwarded-Host", request.host)
    stream_url = f"http://{host}/video/{drone_id}"
    print(f"[{drone_id}] 연결됨 → {stream_url}")

    asyncio.create_task(
        asyncio.to_thread(notify_spring, drone_id, stream_url, True)
    )

    # 연결 직후 폰에게 drone_id 전달
    await ws.send_json({"droneId": drone_id, "streamUrl": stream_url})

    try:
        async for msg in ws:
            if msg.type == WSMsgType.BINARY:
                # JPEG 프레임 수신 → 브로드캐스터에 전달
                await broadcaster.push(msg.data)
            elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE):
                break
    finally:
        broadcasters.pop(drone_id, None)
        print(f"[{drone_id}] 연결 종료")
        asyncio.create_task(
            asyncio.to_thread(notify_spring, drone_id, stream_url, False)
        )

    return ws


# ── MJPEG 송출 (서버 → 프론트/AI) ─────────────────────
async def handle_video(request: web.Request) -> web.StreamResponse:
    """
    GET /video/{drone_id}?fps=N
    프론트:  <img src="http://서버/video/drone-a1b2c3d4">
    AI:      cv2.VideoCapture("http://서버/video/drone-a1b2c3d4")
    """
    drone_id = request.match_info["drone_id"]
    broadcaster = broadcasters.get(drone_id)
    if broadcaster is None:
        raise web.HTTPNotFound(text=f"드론 '{drone_id}' 없음 또는 미연결")

    client_fps = min(int(request.rel_url.query.get("fps", STREAM_FPS)), STREAM_FPS)

    response = web.StreamResponse(headers={
        "Content-Type": "multipart/x-mixed-replace; boundary=frame",
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
        "Connection": "keep-alive",
        "Access-Control-Allow-Origin": "*",
    })

    response.enable_chunked_encoding()

    await response.prepare(request)

    try:
        async for frame in broadcaster.stream(client_fps):
            length = str(len(frame)).encode()
            await response.write(
                b"--frame\r\n"
                b"Content-Type: image/jpeg\r\n"
                b"Content-Length: " + length + b"\r\n"
                b"\r\n"
                + frame
                + b"\r\n\r\n"
            )
    except Exception:
        pass

    return response


# ── 기타 엔드포인트 ────────────────────────────────────
async def handle_drones(request: web.Request) -> web.Response:
    """GET /drones — 연결된 드론 목록 + 스트림 URL"""
    host = request.headers.get("X-Forwarded-Host", request.host)
    data = [
        {**b.stats(), "stream_url": f"http://{host}/video/{did}"}
        for did, b in broadcasters.items()
    ]
    return web.json_response(data)


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({
        "status": "ok",
        "active_drones": len(broadcasters),
        "stream_fps_max": STREAM_FPS,
    })


# ── 폰 송신 페이지 ─────────────────────────────────────
async def handle_sender(request: web.Request) -> web.Response:
    """
    GET /
    QR코드 스캔 후 폰 브라우저에서 열리는 페이지.
    WebSocket으로 서버에 연결 → canvas로 JPEG 캡처 → 전송.
    WebRTC·PeerJS 불필요, 표준 API만 사용.
    """
    # WebSocket URL: ws:// 또는 wss:// (ngrok이면 자동으로 wss)
    html = """<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>드론 스트리밍</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: #111; color: #fff; font-family: sans-serif;
           display: flex; flex-direction: column; align-items: center;
           gap: 10px; padding: 14px; }
    video { width: 100%; max-width: 480px; background: #000;
            border-radius: 10px; aspect-ratio: 4/3; }
    button { width: 100%; max-width: 480px; padding: 14px;
             font-size: 16px; border-radius: 10px; border: none;
             background: #4A90E2; color: #fff; cursor: pointer; }
    button:disabled { background: #444; cursor: default; }
    #status { font-size: 13px; color: #aaa; text-align: center; }
    #info { font-size: 12px; color: #FFD600; text-align: center;
            background: #1a1a1a; padding: 8px 14px; border-radius: 8px;
            display: none; width: 100%; max-width: 480px; word-break: break-all; }
    #fps  { font-size: 12px; color: #888; }
    .ok  { color: #4CAF50 !important; }
    .err { color: #F44336 !important; }
  </style>
</head>
<body>
  <video id="video" autoplay playsinline muted></video>
  <button id="btn" onclick="start()">카메라 시작 및 연결</button>
  <p id="status">버튼을 눌러 시작하세요</p>
  <div id="info"></div>
  <p id="fps"></p>

  <script>
    const TARGET_FPS  = 12;
    const JPEG_QUALITY = 0.65;
    const WS_URL = (location.protocol === 'https:' ? 'wss' : 'ws')
                   + '://' + location.host + '/ws';

    let ws = null, streaming = false, sending = false;
    let lastSent = 0, fpsCount = 0, fpsTimer = Date.now();
    const canvas = document.createElement('canvas');
    const ctx    = canvas.getContext('2d');

    function setStatus(msg, cls = '') {
      const el = document.getElementById('status');
      el.className = cls; el.innerText = msg;
    }

    function connect() {
      ws = new WebSocket(WS_URL);
      ws.binaryType = 'arraybuffer';

      ws.onopen = () => setStatus('WebSocket 연결됨, 스트리밍 준비 중...');

      ws.onmessage = (e) => {
        // 서버가 보낸 droneId 수신
        try {
          const data = JSON.parse(e.data);
          const info = document.getElementById('info');
          info.style.display = 'block';
          info.innerText = '드론 ID: ' + data.droneId;
          setStatus('✅ 스트리밍 중', 'ok');
        } catch {}
      };

      ws.onclose = () => {
        streaming = false;
        setStatus('연결 끊김 — 3초 후 재연결...', 'err');
        setTimeout(connect, 3000);   // 자동 재연결
      };

      ws.onerror = () => ws.close();
    }

    async function start() {
      document.getElementById('btn').disabled = true;
      setStatus('카메라 초기화 중...');
      try {
        const stream = await navigator.mediaDevices.getUserMedia({
          video: { facingMode: 'environment',
                   width: { ideal: 640 }, height: { ideal: 480 } },
          audio: false
        });
        const video = document.getElementById('video');
        video.srcObject = stream;
        canvas.width  = 640;
        canvas.height = 480;

        video.addEventListener('playing', () => {
          streaming = true;
          requestAnimationFrame(sendLoop);
        }, { once: true });

        connect();
      } catch (err) {
        setStatus('오류: ' + err.message, 'err');
        document.getElementById('btn').disabled = false;
      }
    }

    function sendLoop(ts) {
      if (!streaming) return;
      requestAnimationFrame(sendLoop);

      // FPS 제한
      if (ts - lastSent < 1000 / TARGET_FPS) return;
      lastSent = ts;

      // WebSocket 미연결 or 이전 전송 중이면 스킵
      if (!ws || ws.readyState !== WebSocket.OPEN || sending) return;
      sending = true;

      const video = document.getElementById('video');
      ctx.drawImage(video, 0, 0, 640, 480);
      canvas.toBlob(blob => {
        if (blob && ws.readyState === WebSocket.OPEN) {
          ws.send(blob);
          // fps 카운트
          fpsCount++;
          const now = Date.now();
          if (now - fpsTimer >= 1000) {
            document.getElementById('fps').innerText = '전송 fps: ' + fpsCount;
            fpsCount = 0; fpsTimer = now;
          }
        }
        sending = false;
      }, 'image/jpeg', JPEG_QUALITY);
    }
  </script>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


# ── 앱 구성 ────────────────────────────────────────────
async def handle_test(request: web.Request) -> web.Response:
    """GET /test — 스트림 확인용 테스트 페이지"""
    html = b"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Stream Test</title>
  <style>
    body { background:#111; color:#fff; font-family:sans-serif; padding:20px; }
    .grid { display:flex; flex-wrap:wrap; gap:12px; margin-top:16px; }
    .card { background:#1a1a1a; border-radius:10px; padding:10px; }
    .card img { width:320px; border-radius:6px; display:block; }
    .card p { font-size:12px; color:#aaa; margin-top:6px; }
    h2 { margin:0 0 4px; }
    #empty { color:#666; }
  </style>
</head>
<body>
  <h2>Drone Stream Monitor</h2>
  <p id="empty">No active drones. Start a phone stream first.</p>
  <div class="grid" id="grid"></div>
  <script>
    async function refresh() {
      const res = await fetch('/drones');
      const drones = await res.json();
      const grid = document.getElementById('grid');
      const empty = document.getElementById('empty');
      empty.style.display = drones.length ? 'none' : 'block';
      drones.forEach(d => {
        if (document.getElementById('card-' + d.drone_id)) return;
        const card = document.createElement('div');
        card.className = 'card';
        card.id = 'card-' + d.drone_id;
        card.innerHTML =
          '<img src="/video/' + d.drone_id + '" alt="stream">' +
          '<p>' + d.drone_id + ' &nbsp;|&nbsp; fps: ' + d.recv_fps + '</p>';
        grid.appendChild(card);
      });
    }
    refresh();
    setInterval(refresh, 3000);
  </script>
</body>
</html>"""
    return web.Response(body=html, content_type="text/html")


def build_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", handle_sender)
    app.router.add_get("/test", handle_test)
    app.router.add_get("/ws", handle_ws)
    app.router.add_get("/video/{drone_id}", handle_video)
    app.router.add_get("/drones", handle_drones)
    app.router.add_get("/health", handle_health)
    return app


if __name__ == "__main__":
    local_ip  = get_local_ip()
    local_url = f"http://{local_ip}:{PORT}"

    public_url = None
    if USE_NGROK:
        try:
            from pyngrok import ngrok
            public_url = ngrok.connect(PORT).public_url
            print(f"[ngrok] {public_url}")
        except Exception as e:
            print(f"[ngrok] 실패: {e}")

    best_url = public_url or local_url

    print("=" * 52)
    print("  스트림 서버 시작")
    print("=" * 52)
    print(f"  로컬  : {local_url}/")
    if public_url:
        print(f"  ngrok : {public_url}/")
    print()
    print("  📱 폰으로 QR코드 스캔 후 접속하세요")
    print()
    print_qr(best_url)
    print()
    print("  [엔드포인트]")
    print(f"  WebSocket   : {best_url.replace('http','ws')}/ws")
    print(f"  MJPEG 스트림: {best_url}/video/{{drone_id}}")
    print(f"  드론 목록   : {best_url}/drones")
    print(f"  헬스체크    : {best_url}/health")
    print("=" * 52)

    web.run_app(build_app(), host="0.0.0.0", port=PORT)