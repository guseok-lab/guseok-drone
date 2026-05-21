import asyncio
import uuid

from aiohttp import web, WSMsgType

from frame_broadcaster import FrameBroadcaster
from utils import get_base_url, notify_spring, STREAM_FPS

broadcasters: dict[str, FrameBroadcaster] = {}


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
    video  { width:100%; max-width:480px; background:#000; border-radius:10px; aspect-ratio:16/9; }
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
    const TARGET_FPS   = 15;
    const JPEG_QUALITY = 0.8;
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
          video: { facingMode: 'environment', width: { ideal: 1280 }, height: { ideal: 720 } },
          audio: false,
        });
        $('video').srcObject = stream;
        canvas.width = 1280; canvas.height = 720;
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
      ctx.drawImage($('video'), 0, 0, 1280, 720);
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