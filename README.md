# guseok-drone
## 주요 파일별 역할

### frame_broadcaster.py
- **FrameBroadcaster 클래스** 정의
- 각 드론의 프레임(이미지) 데이터 관리, 클라이언트에게 실시간 스트림 제공
- 프레임 수신/전송 FPS, 클라이언트 수, 프레임 상태 등 통계 제공

### handlers.py
- **aiohttp 라우트 핸들러** 모음
- WebSocket 연결(드론 송신), 비디오 스트림, 드론 목록, 헬스체크, 송신/모니터링용 HTML 페이지 제공
- 각 요청별로 FrameBroadcaster와 상호작용하여 실시간 영상 송수신 처리

### utils.py
- **유틸리티 함수 및 상수**
- 로컬 IP 조회, QR코드 생성 및 오픈, cloudflared 터널 실행, Spring 서버 알림 등 공통 기능 제공
- 환경변수 로딩 및 스트림 FPS 등 설정값 관리
