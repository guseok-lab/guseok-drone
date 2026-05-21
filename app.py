import os

from aiohttp import web
from dotenv import load_dotenv

from handlers import (
    handle_ws, handle_video, handle_drones,
    handle_health, handle_sender, handle_test,
)
from utils import get_local_ip, open_qr, start_cloudflare

load_dotenv()

PORT             = int(os.getenv("PORT", "5001"))
USE_CLOUDFLARE   = os.getenv("USE_CLOUDFLARE", "false") == "true"
DRONE_SERVER_URL = os.getenv("DRONE_SERVER_URL", "")  # 배포 시 고정 URL


@web.middleware
async def common_headers(request: web.Request, handler):
    response = await handler(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response


def build_app() -> web.Application:
    app = web.Application(middlewares=[common_headers])
    app.router.add_get("/",                  handle_sender)
    app.router.add_get("/ws",                handle_ws)
    app.router.add_get("/video/{drone_id}",  handle_video)
    app.router.add_get("/drones",            handle_drones)
    app.router.add_get("/health",            handle_health)
    app.router.add_get("/test",              handle_test)
    return app


if __name__ == "__main__":
    local_url = f"http://{get_local_ip()}:{PORT}"

    if DRONE_SERVER_URL:
        # 배포: 환경변수에 고정 URL 설정된 경우
        best_url = DRONE_SERVER_URL
        print(f"[배포] 고정 URL 사용: {best_url}")

    elif USE_CLOUDFLARE:
        # 개발: cloudflare tunnel 자동 실행
        print("[cloudflare] 터널 시작 중 (최대 30초)...")
        public_url = start_cloudflare(PORT)
        if public_url:
            print(f"[cloudflare] {public_url}")
            best_url = public_url
        else:
            print("[cloudflare] 실패 — 로컬 IP로 진행")
            best_url = local_url

    else:
        # 로컬: 로컬 IP 사용
        best_url = local_url

    ws_url = best_url.replace("https://", "wss://").replace("http://", "ws://")

    print()
    print("=" * 54)
    print("  스트림 서버 시작")
    print("=" * 54)
    print(f"  로컬 : {local_url}/")
    if best_url != local_url:
        print(f"  공인 : {best_url}/")
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