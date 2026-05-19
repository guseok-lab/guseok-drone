import time

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
        import asyncio
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
