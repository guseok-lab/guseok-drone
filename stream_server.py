import os
import threading
import requests
from flask import Flask, Response, jsonify, request
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# ── 설정 ──────────────────────────────────────────────
SPRING_URL  = os.getenv("SPRING_URL", "")
USE_NGROK   = os.getenv("USE_NGROK", "false") == "true"
PORT        = int(os.getenv("PORT", "5001"))
STREAM_FPS  = int(os.getenv("STREAM_FPS", "10"))
# ──────────────────────────────────────────────────────

latest_frame = None
frame_lock   = threading.Lock()


# ── Spring 연동 ────────────────────────────────────────

def register_stream_url(stream_url):
    """Spring 서버에 스트림 URL 등록"""
    if not SPRING_URL:
        print("[Spring] SPRING_URL 미설정 - 스킵")
        return
    try:
        res = requests.post(
            f"{SPRING_URL}/api/v1/drone-callback/stream",
            json={"streamUrl": stream_url},
            timeout=3
        )
        if res.status_code == 200:
            print(f"[Spring] 스트림 URL 등록 성공: {stream_url}")
        else:
            print(f"[Spring] 등록 실패: {res.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"[Spring] 등록 오류: {e}")


# ── MJPEG 스트리밍 ─────────────────────────────────────

def generate_mjpeg():
    """MJPEG 스트림 생성"""
    interval = 1.0 / STREAM_FPS
    while True:
        with frame_lock:
            frame = latest_frame
        if frame is None:
            import time; import time as t; t.sleep(0.01)
            continue
        yield (
            b'--frame\r\n'
            b'Content-Type: image/jpeg\r\n\r\n'
            + frame +
            b'\r\n'
        )
        import time; time.sleep(interval)


# ── Flask 라우트 ───────────────────────────────────────

@app.route('/push_frame', methods=['POST'])
def push_frame():
    """WebRTC receiver에서 JPEG 프레임 수신"""
    global latest_frame
    frame_data = request.data
    if frame_data:
        with frame_lock:
            latest_frame = frame_data
    return jsonify({"status": "ok"})


@app.route('/video')
def video_feed():
    """MJPEG 스트림 (프론트/AI가 수신)"""
    return Response(
        generate_mjpeg(),
        mimetype='multipart/x-mixed-replace; boundary=frame'
    )


@app.route('/health')
def health():
    """서버 상태 확인"""
    with frame_lock:
        has_frame = latest_frame is not None
    return jsonify({
        "status": "ok",
        "frame_ready": has_frame,
        "stream_fps": STREAM_FPS
    })


@app.route('/sender')
def sender_page():
    """아이폰에서 열 송신 페이지"""
    return '''
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>카메라 송신</title>
    <script src="https://unpkg.com/peerjs@1.5.1/dist/peerjs.min.js"></script>
    <style>
        body { margin:0; background:black; display:flex; flex-direction:column; align-items:center; }
        video { width:100%; max-width:640px; }
        button { margin:10px; padding:15px 30px; font-size:18px; border-radius:10px; }
        p { color:white; font-size:16px; margin:10px; text-align:center; }
        span { color:yellow; font-weight:bold; }
        input { padding:10px; font-size:16px; width:300px; margin:5px; }
    </style>
</head>
<body>
    <video id="video" autoplay playsinline muted></video>
    <p id="status">초기화 중...</p>
    <p>내 ID: <span id="myId">-</span></p>
    <input id="receiverId" placeholder="수신자 ID 입력">
    <button onclick="startCamera()">카메라 시작</button>
    <button onclick="connect()">연결</button>
    <script>
        let localStream = null;
        const peer = new Peer();

        peer.on('open', id => {
            document.getElementById('myId').innerText = id;
            document.getElementById('status').innerText = '카메라를 시작하세요.';
        });

        peer.on('error', err => {
            document.getElementById('status').innerText = '오류: ' + err.type;
        });

        async function startCamera() {
            try {
                localStream = await navigator.mediaDevices.getUserMedia({
                    video: { facingMode: 'environment', width: { ideal: 640 }, height: { ideal: 480 } },
                    audio: false
                });
                document.getElementById('video').srcObject = localStream;
                document.getElementById('status').innerText = '수신자 ID 입력 후 연결하세요.';
            } catch (err) {
                document.getElementById('status').innerText = '카메라 오류: ' + err.message;
            }
        }

        function connect() {
            const receiverId = document.getElementById('receiverId').value;
            if (!receiverId) { alert('수신자 ID를 입력하세요'); return; }
            if (!localStream) { alert('카메라를 먼저 시작하세요'); return; }

            const call = peer.call(receiverId, localStream);
            call.on('stream', () => {
                document.getElementById('status').innerText = '스트리밍 중 ✅';
            });
            call.on('error', err => {
                document.getElementById('status').innerText = '연결 오류: ' + err;
            });
        }
    </script>
</body>
</html>
    '''


@app.route('/receiver')
def receiver_page():
    """맥북에서 열 수신 페이지"""
    return '''
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>영상 수신</title>
    <script src="https://unpkg.com/peerjs@1.5.1/dist/peerjs.min.js"></script>
    <style>
        body { margin:0; background:black; display:flex; flex-direction:column; align-items:center; }
        video { width:100%; max-width:640px; }
        p { color:white; font-size:16px; margin:10px; }
        span { color:yellow; font-weight:bold; }
        canvas { display:none; }
    </style>
</head>
<body>
    <video id="video" autoplay playsinline muted></video>
    <canvas id="canvas"></canvas>
    <p>내 ID: <span id="myId">-</span></p>
    <p id="status">송신자 연결 대기 중...</p>
    <script>
        const peer = new Peer();
        const video = document.getElementById('video');
        const canvas = document.getElementById('canvas');
        const ctx = canvas.getContext('2d');
        let streaming = false;

        peer.on('open', id => {
            document.getElementById('myId').innerText = id;
        });

        peer.on('error', err => {
            document.getElementById('status').innerText = '오류: ' + err.type;
        });

        peer.on('call', call => {
            call.answer();
            call.on('stream', remoteStream => {
                video.srcObject = remoteStream;
                document.getElementById('status').innerText = '수신 중 ✅';
                streaming = true;
                sendFrames();
            });
        });

        function sendFrames() {
            if (!streaming) return;
            canvas.width = 640;
            canvas.height = 480;
            ctx.drawImage(video, 0, 0, 640, 480);
            canvas.toBlob(blob => {
                if (!blob) return;
                fetch('/push_frame', {
                    method: 'POST',
                    headers: { 'Content-Type': 'image/jpeg' },
                    body: blob
                }).catch(() => {});
            }, 'image/jpeg', 0.5);
            setTimeout(sendFrames, 100); // 10fps
        }
    </script>
</body>
</html>
    '''


# ──────────────────────────────────────────────────────

if __name__ == '__main__':
    print(f"[설정] 포트: {PORT}, FPS: {STREAM_FPS}")

    # ngrok 터널 생성
    ngrok_url = None
    if USE_NGROK:
        try:
            from pyngrok import ngrok
            ngrok_url = ngrok.connect(PORT).public_url
            print(f"[ngrok] 공인 URL: {ngrok_url}")
            print(f"[ngrok] 송신 페이지: {ngrok_url}/sender")
        except Exception as e:
            print(f"[ngrok] 실행 실패: {e}")

    # Spring에 스트림 URL 등록
    if SPRING_URL:
        stream_url = f"{ngrok_url}/video" if ngrok_url else f"http://localhost:{PORT}/video"
        register_stream_url(stream_url)

    app.run(host='0.0.0.0', port=PORT, threaded=True)