import atexit
import os
import re
import socket
import subprocess
import time

import qrcode
import requests
from dotenv import load_dotenv

load_dotenv()

SPRING_URL = os.getenv("SPRING_URL", "")
STREAM_FPS = int(os.getenv("STREAM_FPS", "15"))

'''로컬 IP 조회'''
def get_local_ip() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"

def get_base_url(request) -> str:
    """터널 경유 시 public URL, 아니면 로컬 URL 반환"""
    proto = request.headers.get("X-Forwarded-Proto", "http")
    host  = request.headers.get("X-Forwarded-Host", request.host)
    return f"{proto}://{host}"


def open_qr(url: str) -> None:
    """QR 이미지를 /tmp에 저장 후 Preview로 자동 오픈"""
    try:
        img  = qrcode.make(url)
        path = "/tmp/stream_qr.png"
        img.save(path)
        subprocess.Popen(["open", path])
        print("  QR이 Preview에서 열렸습니다 → 폰 카메라로 스캔하세요")
    except Exception as e:
        print(f"  QR 생성 실패: {e}")


def start_cloudflare(port: int) -> str | None:
    """cloudflared 프로세스 실행 -> URL 파싱 반환"""
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


def notify_spring_stream(drone_id: str, search_id: int | None, stream_url: str, connected: bool) -> None:
    """POST /api/v1/drone-callback/stream — 스트림 URL 등록/해제"""
    if not SPRING_URL:
        return
    try:
        requests.post(
            f"{SPRING_URL}/api/v1/drone-callback/stream",
            json={
                "droneId":   drone_id,
                "searchId":  search_id,
                "streamUrl": stream_url,
                "connected": connected,
            },
            timeout=3,
        )
    except Exception as e:
        print(f"[Spring] 스트림 알림 실패: {e}")


def notify_spring_status(drone_id: str, search_id: int | None, status: str) -> None:
    """POST /api/v1/drone-callback/status — 드론 상태 전달 (STREAMING | DISCONNECTED)"""
    if not SPRING_URL:
        return
    try:
        requests.post(
            f"{SPRING_URL}/api/v1/drone-callback/status",
            json={
                "droneId":  drone_id,
                "searchId": search_id,
                "status":   status,
            },
            timeout=3,
        )
    except Exception as e:
        print(f"[Spring] 상태 알림 실패: {e}")