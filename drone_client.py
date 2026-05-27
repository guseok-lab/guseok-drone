"""
drone_client.py — 웹캠 영상을 WebSocket으로 스트림 서버에 전송 (시연용)

설치: pip install opencv-python websockets python-dotenv
실행: python drone_client.py
"""

import asyncio
import os
import time

import cv2
import websockets
from dotenv import load_dotenv

load_dotenv()

SERVER_URL   = os.getenv("DRONE_SERVER_URL", "http://localhost:5001") \
               .replace("https://", "wss://").replace("http://", "ws://")
SEARCH_ID    = os.getenv("SEARCH_ID", "")
STREAM_FPS   = int(os.getenv("STREAM_FPS", "15"))
JPEG_QUALITY = int(os.getenv("JPEG_QUALITY", "80"))
CAM_INDEX    = int(os.getenv("CAM_INDEX", "0"))


async def stream():
    ws_url = f"{SERVER_URL}/ws"
    if SEARCH_ID:
        ws_url += f"?searchId={SEARCH_ID}"

    cap = cv2.VideoCapture(CAM_INDEX)
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)

    if not cap.isOpened():
        print(f"[오류] 웹캠({CAM_INDEX})을 열 수 없습니다")
        return

    print(f"[드론 클라이언트] 서버 연결 중: {ws_url}")

    while True:
        try:
            async with websockets.connect(ws_url, max_size=5 * 1024 * 1024) as ws:
                # 서버로부터 droneId 수신
                msg = await ws.recv()
                print(f"[드론 클라이언트] {msg}")

                interval = 1.0 / STREAM_FPS
                print(f"[드론 클라이언트] 스트리밍 시작 (fps={STREAM_FPS})")

                fpsCount = 0
                fpsTimer = time.monotonic()

                while True:
                    start = time.monotonic()

                    ret, frame = cap.read()
                    if not ret:
                        print("[오류] 프레임 캡처 실패")
                        break

                    _, jpeg = cv2.imencode(
                        ".jpg", frame,
                        [cv2.IMWRITE_JPEG_QUALITY, JPEG_QUALITY]
                    )
                    await ws.send(jpeg.tobytes())

                    # fps 출력
                    fpsCount += 1
                    now = time.monotonic()
                    if now - fpsTimer >= 1.0:
                        print(f"[드론 클라이언트] 전송 fps: {fpsCount}")
                        fpsCount = 0
                        fpsTimer = now

                    elapsed = time.monotonic() - start
                    sleep_time = interval - elapsed
                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)

        except (websockets.exceptions.ConnectionClosed, OSError) as e:
            print(f"[드론 클라이언트] 연결 끊김: {e} → 3초 후 재연결")
            await asyncio.sleep(3)
        except Exception as e:
            print(f"[드론 클라이언트] 오류: {e} → 3초 후 재연결")
            await asyncio.sleep(3)


if __name__ == "__main__":
    print("=" * 50)
    print("  드론 클라이언트 시작")
    print("=" * 50)
    print(f"  서버    : {SERVER_URL}")
    print(f"  searchId: {SEARCH_ID or '없음 (테스트)'}")
    print(f"  FPS     : {STREAM_FPS}")
    print(f"  화질    : {JPEG_QUALITY}")
    print(f"  웹캠    : {CAM_INDEX}번")
    print("=" * 50)
    asyncio.run(stream())