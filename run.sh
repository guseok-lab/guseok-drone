if [ -d "venv" ]; then
    source venv/bin/activate
    echo "Python 가상환경 활성화됨."
else
    echo "venv 디렉토리가 없습니다. requirements.txt를 기반으로 가상환경을 먼저 생성하세요."
    exit 1
fi

python app.py
