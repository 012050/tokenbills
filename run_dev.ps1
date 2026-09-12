# tokenbills 개발 환경 로컬 실행 (Windows 개발 PC)
# LAN 클라이언트 → 이 PC:8000 → vLLM(192.168.1.221:8001)
# DB: ./data (개발용 스냅샷 — 운영 데이터와 무관)
$env:VLLM_BASE_URL = 'http://192.168.1.221:8001'
$env:TOKENBILLS_DB = Join-Path $PSScriptRoot 'data\tokenbills.db'
$env:TRUSTED_PROXIES = '192.168.1.3'
python -m uvicorn main:app --no-proxy-headers --host 0.0.0.0 --port 8000
