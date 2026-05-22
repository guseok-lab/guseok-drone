import asyncio
import time


class FrameBroadcaster:
    def __init__(self, drone_id: str):
        self.drone_id       = drone_id
        self._frame: bytes | None = None
        self._client_count  = 0
        self._recv_fps      = 0.0
        self._frame_count   = 0
        self._last_fps_time = time.monotonic()

    '''폰에서 받은 JPEG 프레임 저장 + fps 계산'''
    async def push(self, frame: bytes) -> None:
        self._frame = frame
        self._frame_count += 1
        now = time.monotonic()
        elapsed = now - self._last_fps_time
        if elapsed >= 1.0:
            self._recv_fps = round(self._frame_count / elapsed, 1)
            self._frame_count = 0
            self._last_fps_time = now

    '''MJPEG 구독자(프론트/AI)에게 프레임 전달'''
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

    '''드론 상태 정보 반환'''
    def stats(self) -> dict:
        return {
            "drone_id": self.drone_id,
            "recv_fps": self._recv_fps,
            "clients":  self._client_count,
            "has_frame": self._frame is not None,
        }