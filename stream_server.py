from pyngrok import ngrok
from dotenv import load_dotenv
load_dotenv()  # .env 파일 자동 로드

import cv2
import requests
import threading
import time
import os
import socket
from flask import Flask, Response, jsonify

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
AI_SERVER_URL  = os.getenv("AI_SERVER_URL", "")        # AI 팀원 서버 주소
SPRING_URL     = os.getenv("SPRING_URL", "")           # Spring 서버 주소
PORT           = int(os.getenv("PORT", "5001"))
# ──────────────────────────────────────────────────────

latest_stream_frame = None  # AI 서버가 처리한 탐지 영상
latest_raw_frame    = None  # AI 서버로 보낼 원본 프레임
frame_lock          = threading.Lock()


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
    """프레임 캡처 스레드 - 항상 최신 프레임만 유지"""
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

            raw_frame = encode_frame(frame, AI_WIDTH, AI_HEIGHT, AI_QUALITY)

            with frame_lock:
                latest_raw_frame = raw_frame

        except Exception as e:
            print(f"[캡처 오류] {e}")
            time.sleep(0.01)


def send_to_ai():
    """
    AI 서버로 원본 프레임 전송 스레드
    AI 서버가 MJPEG으로 직접 맥북 스트림 수신하므로
    현재는 미사용 - AI 팀원과 협의 후 결정
    """
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
            pass  # AI 서버 없으면 조용히 스킵

        time.sleep(interval)


def register_stream_url():
    """Spring 서버에 스트림 URL 등록"""
    if not SPRING_URL:
        print("[Spring] SPRING_URL 미설정 - 스트림 URL 등록 스킵")
        return

    # 맥북 로컬 IP 자동 감지
    import socket
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    stream_url = f"http://{local_ip}:{PORT}/video"

    try:
        res = requests.post(
            f"{SPRING_URL}/api/drone/stream-register",
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
    """
    MJPEG 스트림 엔드포인트
    - 프론트: <img src="http://맥북IP:5001/video" />
    - AI 서버: cv2.VideoCapture("http://맥북IP:5001/video")
    """
    return Response(
        generate_mjpeg(),
        mimetype='multipart/x-mixed-replace; boundary=frame'
    )

@app.route('/health')
def health():
    """상태 확인"""
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

@app.route('/gps')
def gps_page():
    """휴대폰 브라우저에서 열면 GPS 자동 전송"""
    return '''
    <html>
    <body>
    <h2>GPS 전송 중...</h2>
    <p id="status">위치 확인 중...</p>
    <script>
    const SPRING_URL = "http://AWS서버주소/api/v1/drone-callback/location";
    
    navigator.geolocation.watchPosition(
        (pos) => {
            const data = {
                lat: pos.coords.latitude,
                lng: pos.coords.longitude,
                accuracy: pos.coords.accuracy
            };
            
            fetch(SPRING_URL, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(data)
            });
            
            document.getElementById('status').innerText = 
                `위도: ${data.lat}, 경도: ${data.lng}`;
        },
        (err) => {
            document.getElementById('status').innerText = 'GPS 오류: ' + err.message;
        },
        { enableHighAccuracy: true, maximumAge: 0 }
    );
    </script>
    </body>
    </html>
    ''';

# ──────────────────────────────────────────────────────

if __name__ == '__main__':
    print(f"[설정] 스트림 {STREAM_WIDTH}x{STREAM_HEIGHT} @ {STREAM_FPS}fps")
    print(f"[설정] AI {AI_WIDTH}x{AI_HEIGHT} @ {AI_FPS}fps")

    source = init_source()

    # 프레임 캡처 스레드
    threading.Thread(target=capture_frames, args=(source,), daemon=True).start()

    # AI 전송 스레드 (AI 팀원 서버 준비되면 주석 해제)
    # threading.Thread(target=send_to_ai, daemon=True).start()

    # Spring에 스트림 URL 등록 (Spring 준비되면 주석 해제)
    # threading.Thread(target=register_stream_url, daemon=True).start()

    # ngrok 터널 생성
    if os.getenv("USE_NGROK", "true") == "true":
        public_url = ngrok.connect(PORT)
        stream_url = f"{public_url}/video"
        print(f"[ngrok] 공인 URL: {stream_url}")

        # Spring에 자동 등록
        if SPRING_URL:
            try:
                requests.post(
                    f"{SPRING_URL}/api/v1/drone-callback/stream",
                    json={"streamUrl": stream_url},
                    timeout=3
                )
                print(f"[Spring] 스트림 URL 등록 완료")
            except Exception as e:
                print(f"[Spring] 등록 실패: {e}")

    app.run(host='0.0.0.0', port=PORT, threaded=True)