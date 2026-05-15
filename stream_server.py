import cv2
import requests
import threading
import time
import os
from flask import Flask, Response, jsonify
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# ── 설정 ──────────────────────────────────────────────
MOCK_MODE      = os.getenv("MOCK_MODE", "true") == "true"
STREAM_FPS     = int(os.getenv("STREAM_FPS", "10"))
AI_FPS         = int(os.getenv("AI_FPS", "5"))
STREAM_QUALITY = int(os.getenv("STREAM_QUALITY", "50"))
AI_QUALITY     = int(os.getenv("AI_QUALITY", "70"))
STREAM_WIDTH   = int(os.getenv("STREAM_WIDTH", "640"))
STREAM_HEIGHT  = int(os.getenv("STREAM_HEIGHT", "480"))
AI_WIDTH       = int(os.getenv("AI_WIDTH", "320"))
AI_HEIGHT      = int(os.getenv("AI_HEIGHT", "240"))
AI_SERVER_URL  = os.getenv("AI_SERVER_URL", "")
SPRING_URL     = os.getenv("SPRING_URL", "")
USE_NGROK      = os.getenv("USE_NGROK", "true") == "true"
PORT           = int(os.getenv("PORT", "5001"))
# ──────────────────────────────────────────────────────

latest_raw_frame = None
frame_lock       = threading.Lock()


def init_source():
    """영상 소스 초기화 (웹캠 or Tello)"""
    if MOCK_MODE:
        cap = cv2.VideoCapture(0)
        cap.set(cv2.CAP_PROP_FRAME_WIDTH, STREAM_WIDTH)
        cap.set(cv2.CAP_PROP_FRAME_HEIGHT, STREAM_HEIGHT)
        print(f"[설정] 웹캠(Mock) 모드")
        return cap
    else:
        from djitellopy import Tello
        tello = Tello()
        tello.connect()
        print(f"[드론] 배터리: {tello.get_battery()}%")
        tello.streamon()
        print(f"[설정] Tello 드론 모드")
        return tello.get_frame_read()


def encode_frame(frame, width, height, quality):
    """프레임 리사이즈 + JPEG 압축"""
    resized = cv2.resize(frame, (width, height))
    _, buffer = cv2.imencode(
        '.jpg', resized,
        [cv2.IMWRITE_JPEG_QUALITY, quality]
    )
    return buffer.tobytes()


def capture_frames(source):
    """프레임 캡처 스레드"""
    global latest_raw_frame

    while True:
        try:
            if MOCK_MODE:
                ret, frame = source.read()
                if not ret:
                    time.sleep(0.01)
                    continue
            else:
                frame = source.frame
                if frame is None:
                    time.sleep(0.01)
                    continue

            raw_frame = encode_frame(frame, STREAM_WIDTH, STREAM_HEIGHT, STREAM_QUALITY)

            with frame_lock:
                latest_raw_frame = raw_frame

        except Exception as e:
            print(f"[캡처 오류] {e}")
            time.sleep(0.01)


def send_to_ai():
    """AI 서버로 프레임 전송 스레드"""
    interval = 1.0 / AI_FPS

    while True:
        if not AI_SERVER_URL:
            time.sleep(interval)
            continue

        with frame_lock:
            frame = latest_raw_frame

        if frame is None:
            time.sleep(interval)
            continue

        try:
            res = requests.post(
                f"{AI_SERVER_URL}/analyze",
                data=frame,
                headers={"Content-Type": "image/jpeg"},
                timeout=0.5
            )
            if res.status_code == 200:
                print(f"[AI] 탐지 결과: {res.json()}")
        except requests.exceptions.RequestException:
            pass

        time.sleep(interval)


def register_stream_url(ngrok_url=None):
    """Spring 서버에 스트림 URL 등록"""
    if not SPRING_URL:
        print("[Spring] SPRING_URL 미설정 - 스트림 URL 등록 스킵")
        return

    if ngrok_url:
        stream_url = f"{ngrok_url}/video"
    else:
        import socket
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        stream_url = f"http://{local_ip}:{PORT}/video"

    try:
        res = requests.post(
            f"{SPRING_URL}/api/v1/drone-callback/stream",
            json={"streamUrl": stream_url},
            timeout=3
        )
        if res.status_code == 200:
            print(f"[Spring] 스트림 URL 등록 성공: {stream_url}")
        else:
            print(f"[Spring] 스트림 URL 등록 실패: {res.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"[Spring] 스트림 URL 등록 오류: {e}")


def generate_mjpeg():
    """MJPEG 스트림 생성"""
    interval = 1.0 / STREAM_FPS

    while True:
        with frame_lock:
            frame = latest_raw_frame

        if frame is None:
            time.sleep(0.01)
            continue

        yield (
            b'--frame\r\n'
            b'Content-Type: image/jpeg\r\n\r\n'
            + frame +
            b'\r\n'
        )
        time.sleep(interval)


# ── Flask 라우트 ───────────────────────────────────────

@app.route('/video')
def video_feed():
    return Response(
        generate_mjpeg(),
        mimetype='multipart/x-mixed-replace; boundary=frame'
    )

@app.route('/health')
def health():
    with frame_lock:
        has_frame = latest_raw_frame is not None
    return jsonify({
        "status": "ok",
        "mode": "mock" if MOCK_MODE else "drone",
        "stream_fps": STREAM_FPS,
        "ai_fps": AI_FPS,
        "resolution": f"{STREAM_WIDTH}x{STREAM_HEIGHT}",
        "frame_ready": has_frame
    })

# ──────────────────────────────────────────────────────

if __name__ == '__main__':
    print(f"[설정] {'웹캠(Mock)' if MOCK_MODE else 'Tello 드론'} 모드")
    print(f"[설정] 스트림 {STREAM_WIDTH}x{STREAM_HEIGHT} @ {STREAM_FPS}fps")

    source = init_source()

    # 프레임 캡처 스레드
    threading.Thread(target=capture_frames, args=(source,), daemon=True).start()

    # AI 전송 스레드 (AI 서버 준비되면 주석 해제)
    # threading.Thread(target=send_to_ai, daemon=True).start()

    # ngrok 터널 생성
    ngrok_url = None
    if USE_NGROK:
        try:
            from pyngrok import ngrok
            ngrok_url = ngrok.connect(PORT).public_url
            print(f"[ngrok] 공인 URL: {ngrok_url}")
        except Exception as e:
            print(f"[ngrok] 실행 실패: {e}")

    # Spring에 스트림 URL 등록 (Spring 준비되면 주석 해제)
    # register_stream_url(ngrok_url)

    app.run(host='0.0.0.0', port=PORT, threaded=True)