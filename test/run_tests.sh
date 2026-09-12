#!/usr/bin/env bash
# tokenbills 통합 테스트 — SPEC.md 8절 수락 기준 검증.
# mock vLLM(8100) + tokenbills(기본 8000, PROXY_PORT 로 대체 가능)만 사용.
# 실제 vLLM 컨테이너/compose 에는 접근하지 않는다.
set -u
cd "$(dirname "$0")/.."

PY="${PYTHON:-python3}"
[ -x ".venv/bin/python" ] && PY=".venv/bin/python"

MOCK_PORT="${MOCK_PORT:-8100}"
PROXY_PORT="${PROXY_PORT:-8000}"
PROXY2_PORT="${PROXY2_PORT:-$((PROXY_PORT + 200))}"
MOCK_URL="http://127.0.0.1:$MOCK_PORT"
PROXY_URL="http://127.0.0.1:$PROXY_PORT"
PROXY2_URL="http://127.0.0.1:$PROXY2_PORT"

T="$(mktemp -d)"
DB="$T/tokenbills.db"

# Windows(Git Bash)에서는 /tmp 가 Windows Python 에 안 보이므로 경로를 변환 (Linux 는 그대로)
pypath() { if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else printf "%s" "$1"; fi; }
PY_DB="$(pypath "$DB")"
PY_LAST="$(pypath "$T/last.json")"
PASS=0
FAIL=0
MOCK_PID=""
PROXY_PID=""
PROXY2_PID=""
PROXY3_PID=""

cleanup() {
  [ -n "$PROXY3_PID" ] && kill "$PROXY3_PID" >/dev/null 2>&1
  [ -n "$PROXY2_PID" ] && kill "$PROXY2_PID" >/dev/null 2>&1
  [ -n "$PROXY_PID" ] && kill "$PROXY_PID" >/dev/null 2>&1
  [ -n "$MOCK_PID" ] && kill "$MOCK_PID" >/dev/null 2>&1
  sleep 0.3
  rm -rf "$T"
}
trap cleanup EXIT

ok()  { echo "PASS: $1"; PASS=$((PASS + 1)); }
bad() { echo "FAIL: $1"; FAIL=$((FAIL + 1)); }

wait_http() {
  local url="$1" tries="${2:-60}" i
  for i in $(seq 1 "$tries"); do
    curl -sf -o /dev/null "$url" && return 0
    sleep 0.5
  done
  return 1
}

echo "== mock vLLM 기동 ($MOCK_URL)"
"$PY" test/mock_vllm.py >"$T/mock.log" 2>&1 &
MOCK_PID=$!
if ! wait_http "$MOCK_URL/v1/models"; then
  echo "mock vLLM 기동 실패:"; cat "$T/mock.log"; exit 1
fi

echo "== tokenbills 기동 ($PROXY_URL)"
TOKENBILLS_DB="$PY_DB" VLLM_BASE_URL="$MOCK_URL" TZ=Asia/Seoul \
  "$PY" -m uvicorn main:app --no-proxy-headers --host 127.0.0.1 --port "$PROXY_PORT" >"$T/proxy.log" 2>&1 &
PROXY_PID=$!
if ! wait_http "$PROXY_URL/health"; then
  echo "tokenbills 기동 실패:"; cat "$T/proxy.log"; exit 1
fi

B='{"model":"mock-model","messages":[{"role":"user","content":"hello"}]}'
SB='{"model":"mock-model","messages":[{"role":"user","content":"hello"}],"stream":true}'

# ---------------------------------------------------------------- 8.1 passthrough

# /v1/models
curl -s "$PROXY_URL/v1/models" -o "$T/models_via.json"
curl -s "$MOCK_URL/v1/models" -o "$T/models_direct.json"
cmp -s "$T/models_via.json" "$T/models_direct.json" \
  && ok "8.1 GET /v1/models 본문 passthrough" \
  || bad "8.1 GET /v1/models 본문 passthrough"

# non-streaming: 프록시 호출 직후 mock 이 받은 헤더를 확보 (XFF 검증)
curl -s -X POST "$PROXY_URL/v1/chat/completions" \
  -H 'content-type: application/json' -H 'x-test-header: abc123' \
  -d "$B" -o "$T/via_proxy.json"
curl -s "$MOCK_URL/mock/last_request" -o "$T/last.json"
curl -s -X POST "$MOCK_URL/v1/chat/completions" \
  -H 'content-type: application/json' -d "$B" -o "$T/direct.json"
cmp -s "$T/via_proxy.json" "$T/direct.json" \
  && ok "8.1 non-streaming 본문 passthrough" \
  || bad "8.1 non-streaming 본문 passthrough"

cat > "$T/t_xff.py" <<EOF
import json
last = json.load(open("$PY_LAST"))
h = last["headers"]
assert h.get("x-forwarded-for") == "127.0.0.1", h.get("x-forwarded-for")
assert h.get("x-test-header") == "abc123", h.get("x-test-header")
EOF
"$PY" "$T/t_xff.py" >/dev/null 2>&1 \
  && ok "8.1 X-Forwarded-For(클라이언트 IP) + 클라이언트 헤더 전달" \
  || bad "8.1 X-Forwarded-For(클라이언트 IP) + 클라이언트 헤더 전달"

# streaming SSE: 프록시는 include_usage 를 주입해 upstream 을 호출하므로,
# 동일 본문(include_usage)으로 mock 직출과 바이트를 비교
curl -s -N -X POST "$PROXY_URL/v1/chat/completions" \
  -H 'content-type: application/json' -d "$SB" -o "$T/proxy.sse"
curl -s -N -X POST "$MOCK_URL/v1/chat/completions" \
  -H 'content-type: application/json' \
  -d '{"model":"mock-model","messages":[{"role":"user","content":"hello"}],"stream":true,"stream_options":{"include_usage":true}}' \
  -o "$T/direct.sse"
cmp -s "$T/proxy.sse" "$T/direct.sse" \
  && ok "8.1/8.3 SSE 본문 무손실 (chunk + usage + [DONE])" \
  || bad "8.1/8.3 SSE 본문 무손실 (chunk + usage + [DONE])"

# 500 에러 전파
CODE_VIA=$(curl -s -o "$T/err_via.json" -w '%{http_code}' -X POST "$PROXY_URL/v1/chat/completions" \
  -H 'content-type: application/json' -d '{"model":"fail-500","messages":[]}')
curl -s -o "$T/err_direct.json" -X POST "$MOCK_URL/v1/chat/completions" \
  -H 'content-type: application/json' -d '{"model":"fail-500","messages":[]}'
[ "$CODE_VIA" = "500" ] && cmp -s "$T/err_via.json" "$T/err_direct.json" \
  && ok "8.1 upstream 500 전파 (코드+본문)" \
  || bad "8.1 upstream 500 전파 (코드+본문) — got $CODE_VIA"

# ---------------------------------------------------------------- 8.2 기록
cat > "$T/t_db2.py" <<EOF
import sqlite3
conn = sqlite3.connect("$PY_DB")
rows = conn.execute("SELECT prompt_tokens, completion_tokens FROM usage ORDER BY ts").fetchall()
assert rows == [(123, 45), (123, 45)], rows
EOF
"$PY" "$T/t_db2.py" >/dev/null 2>&1 \
  && ok "8.2 non-streaming + streaming usage 각 1행 기록 (123/45)" \
  || bad "8.2 non-streaming + streaming usage 각 1행 기록 (123/45)"

# usage 없는 요청은 기록되지 않는다
curl -s -X POST "$PROXY_URL/v1/chat/completions" \
  -H 'content-type: application/json' -d '{"model":"no-usage","messages":[]}' -o /dev/null
# embeddings: prompt_tokens 만 있는 usage → completion 0 으로 기록
curl -s -X POST "$PROXY_URL/v1/embeddings" \
  -H 'content-type: application/json' -d '{"model":"mock-model","input":"hello"}' -o /dev/null
# responses API: passthrough + input_tokens/output_tokens → prompt/completion 으로 기록
curl -s -X POST "$PROXY_URL/v1/responses" \
  -H 'content-type: application/json' -d '{"model":"mock-model","input":"hello"}' -o "$T/resp_via.json"
curl -s -X POST "$MOCK_URL/v1/responses" \
  -H 'content-type: application/json' -d '{"model":"mock-model","input":"hello"}' -o "$T/resp_direct.json"
cmp -s "$T/resp_via.json" "$T/resp_direct.json" \
  && ok "8.1 Responses API non-streaming 본문 passthrough" \
  || bad "8.1 Responses API non-streaming 본문 passthrough"
curl -s -N -X POST "$PROXY_URL/v1/responses" \
  -H 'content-type: application/json' -d '{"model":"mock-model","input":"hello","stream":true}' -o "$T/resp_via.sse"
curl -s -N -X POST "$MOCK_URL/v1/responses" \
  -H 'content-type: application/json' -d '{"model":"mock-model","input":"hello","stream":true}' -o "$T/resp_direct.sse"
cmp -s "$T/resp_via.sse" "$T/resp_direct.sse" \
  && ok "8.1/8.3 Responses API SSE 본문 무손실" \
  || bad "8.1/8.3 Responses API SSE 본문 무손실"
# responses: usage 없는 요청은 미기록
curl -s -X POST "$PROXY_URL/v1/responses" \
  -H 'content-type: application/json' -d '{"model":"no-usage","input":"x"}' -o /dev/null
cat > "$T/t_db3.py" <<EOF
import sqlite3
conn = sqlite3.connect("$PY_DB")
rows = conn.execute("SELECT prompt_tokens, completion_tokens FROM usage ORDER BY ts").fetchall()
assert rows == [(123, 45), (123, 45), (7, 0), (31, 7), (31, 7)], rows
EOF
"$PY" "$T/t_db3.py" >/dev/null 2>&1 \
  && ok "8.2 usage 없으면 미기록 / embeddings completion 0 기록" \
  || bad "8.2 usage 없으면 미기록 / embeddings completion 0 기록"

# ---------------------------------------------------------------- 8.4 가격
# 누적 5행: prompt 315, completion 104 — 기본 요율 6872/34360
cat > "$T/t_price.py" <<EOF
import json, urllib.request
port = $PROXY_PORT
d = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/api/usage?unit=hour"))
t = d["totals"]
assert t["requests"] == 5, t
assert t["prompt_tokens"] == 315, t
assert t["completion_tokens"] == 104, t
assert t["total_tokens"] == 419, t
expected = 315 * 6872.0 / 1e6 + 104 * 34360.0 / 1e6
assert abs(t["cost_krw"] - expected) < 1e-9, (t["cost_krw"], expected)
EOF
"$PY" "$T/t_price.py" >/dev/null 2>&1 \
  && ok "8.4 기본 요율 cost_krw = p*P/1e6 + c*C/1e6" \
  || { bad "8.4 기본 요율 cost_krw"; "$PY" "$T/t_price.py" || true; }

# 환경변수 요율 우선 (동일 DB 를 읽는 두 번째 인스턴스, 요율 2000/4000)
TOKENBILLS_DB="$PY_DB" VLLM_BASE_URL="$MOCK_URL" \
  PROMPT_PRICE_KRW=2000 COMPLETION_PRICE_KRW=4000 TZ=Asia/Seoul \
  "$PY" -m uvicorn main:app --no-proxy-headers --host 127.0.0.1 --port "$PROXY2_PORT" >"$T/proxy2.log" 2>&1 &
PROXY2_PID=$!
if wait_http "$PROXY2_URL/health"; then
  cat > "$T/t_price2.py" <<EOF
import json, urllib.request
d = json.load(urllib.request.urlopen("http://127.0.0.1:$PROXY2_PORT/api/usage?unit=day"))
t = d["totals"]
expected = 315 * 2000.0 / 1e6 + 104 * 4000.0 / 1e6
assert abs(t["cost_krw"] - expected) < 1e-9, (t["cost_krw"], expected)
EOF
  "$PY" "$T/t_price2.py" >/dev/null 2>&1 \
    && ok "8.4 PROMPT/COMPLETION_PRICE_KRW 환경변수 우선" \
    || bad "8.4 PROMPT/COMPLETION_PRICE_KRW 환경변수 우선"
else
  bad "8.4 가격 우선 검증 인스턴스 기동 실패 (proxy2.log)"
fi
kill "$PROXY2_PID" >/dev/null 2>&1; PROXY2_PID=""

# ---------------------------------------------------------------- 9. IP별 기록/조회 (신규)
# 9.1 직결: 위조 XFF 무시 (신뢰 프록시가 아닌 소스) → TCP 소스 IP 기록
# 9.2 신뢰 프록시(TRUSTED_PROXIES): XFF 첫 항목 = 실 클라이언트 IP / XFF 없으면 TCP 소스 IP
# 9.3 /api/clients IP별 집계 + /api/usage?ip= 필터
IP_PORT="${PROXY3_PORT:-$((PROXY2_PORT + 100))}"
IP_URL="http://127.0.0.1:$IP_PORT"
IP_DB="$T/ip.db"
PY_IP_DB="$(pypath "$IP_DB")"

TOKENBILLS_DB="$PY_IP_DB" VLLM_BASE_URL="$MOCK_URL" TZ=Asia/Seoul \
  "$PY" -m uvicorn main:app --no-proxy-headers --host 127.0.0.1 --port "$IP_PORT" >"$T/proxy_ip.log" 2>&1 &
PROXY3_PID=$!
if wait_http "$IP_URL/health"; then
  curl -s -X POST "$IP_URL/v1/chat/completions" \
    -H 'content-type: application/json' -H 'X-Forwarded-For: 203.0.113.99' \
    -d "$B" -o /dev/null
  cat > "$T/t_ip1.py" <<EOF
import sqlite3
conn = sqlite3.connect("$PY_IP_DB")
rows = conn.execute("SELECT client_ip, prompt_tokens FROM usage ORDER BY ts").fetchall()
assert rows == [("127.0.0.1", 123)], rows
EOF
  "$PY" "$T/t_ip1.py" >/dev/null 2>&1 \
    && ok "9.1 직결 요청: 위조 XFF 무시, TCP 소스 IP 기록" \
    || { bad "9.1 직결 요청: 위조 XFF 무시"; "$PY" "$T/t_ip1.py" || true; }
else
  bad "9.1 IP 테스트 인스턴스 기동 실패 (proxy_ip.log)"
fi
kill "$PROXY3_PID" >/dev/null 2>&1; PROXY3_PID=""

TOKENBILLS_DB="$PY_IP_DB" VLLM_BASE_URL="$MOCK_URL" TRUSTED_PROXIES=127.0.0.1 TZ=Asia/Seoul \
  "$PY" -m uvicorn main:app --no-proxy-headers --host 127.0.0.1 --port "$IP_PORT" >"$T/proxy_ip2.log" 2>&1 &
PROXY3_PID=$!
if wait_http "$IP_URL/health"; then
  curl -s -X POST "$IP_URL/v1/chat/completions" \
    -H 'content-type: application/json' -H 'X-Forwarded-For: 203.0.113.7' \
    -d "$B" -o /dev/null
  sleep 0.2
  curl -s -X POST "$IP_URL/v1/chat/completions" \
    -H 'content-type: application/json' -d "$B" -o /dev/null
  cat > "$T/t_ip2.py" <<EOF
import sqlite3
conn = sqlite3.connect("$PY_IP_DB")
rows = conn.execute("SELECT client_ip, prompt_tokens, completion_tokens FROM usage ORDER BY ts").fetchall()
assert rows[1:] == [("203.0.113.7", 123, 45), ("127.0.0.1", 123, 45)], rows
EOF
  "$PY" "$T/t_ip2.py" >/dev/null 2>&1 \
    && ok "9.2 신뢰 프록시: XFF 첫 항목 기록 (XFF 없으면 TCP 소스 IP)" \
    || { bad "9.2 신뢰 프록시 XFF 기록"; "$PY" "$T/t_ip2.py" || true; }
  cat > "$T/t_ip3.py" <<EOF
import json, urllib.request
port = $IP_PORT
d = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/api/clients"))
assert d["totals"]["requests"] == 3, d
ips = {c["client_ip"]: c for c in d["clients"]}
assert set(ips) == {"127.0.0.1", "203.0.113.7"}, ips
assert ips["203.0.113.7"]["requests"] == 1
assert ips["203.0.113.7"]["prompt_tokens"] == 123
assert ips["203.0.113.7"]["completion_tokens"] == 45
assert ips["127.0.0.1"]["requests"] == 2
assert ips["127.0.0.1"]["total_tokens"] == 336
expected = 369 * 6872.0 / 1e6 + 135 * 34360.0 / 1e6
assert abs(d["totals"]["cost_krw"] - expected) < 1e-9, d["totals"]
assert d["clients"][0]["client_ip"] == "127.0.0.1"  # 토큰 합계 내림차순
d0 = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/api/clients?start=2000-01-01&end=2000-01-31"))
assert d0["totals"]["requests"] == 0 and d0["clients"] == []
f = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/api/usage?unit=day&ip=203.0.113.7"))
assert f["ip"] == "203.0.113.7"
assert f["totals"]["requests"] == 1 and f["totals"]["prompt_tokens"] == 123
f0 = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/api/usage?unit=day&ip=192.0.2.55"))
assert f0["totals"]["requests"] == 0 and f0["series"] == []
EOF
  "$PY" "$T/t_ip3.py" >/dev/null 2>&1 \
    && ok "9.3 /api/clients 집계 + /api/usage?ip= 필터" \
    || { bad "9.3 /api/clients + ip 필터"; "$PY" "$T/t_ip3.py" || true; }
else
  bad "9.2 신뢰 프록시 인스턴스 기동 실패 (proxy_ip2.log)"
fi
kill "$PROXY3_PID" >/dev/null 2>&1; PROXY3_PID=""

# ---------------------------------------------------------------- 8.5 웹/API 일관성
cat > "$T/t_api.py" <<EOF
import json, sqlite3, urllib.request
from datetime import datetime, timedelta, timezone
try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
except Exception:
    KST = timezone(timedelta(hours=9))  # tzdata 없는 환경(Windows) 폴백

port = $PROXY_PORT
conn = sqlite3.connect("$PY_DB")
rows = conn.execute("SELECT ts, prompt_tokens, completion_tokens FROM usage").fetchall()

for unit in ("month", "day", "hour"):
    d = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/api/usage?unit={unit}"))
    t = d["totals"]
    assert d["unit"] == unit
    assert t["requests"] == len(rows), (unit, t)
    assert t["prompt_tokens"] == sum(r[1] for r in rows), (unit, t)
    assert t["completion_tokens"] == sum(r[2] for r in rows), (unit, t)
    assert t["total_tokens"] == t["prompt_tokens"] + t["completion_tokens"]
    assert sum(s["requests"] for s in d["series"]) == t["requests"]
    assert sum(s["prompt_tokens"] for s in d["series"]) == t["prompt_tokens"]
    assert sum(s["completion_tokens"] for s in d["series"]) == t["completion_tokens"]
    for s in d["series"]:
        assert sum(s.get("clients", {}).values()) == s["total_tokens"], s
        p = s["period"]
        if unit == "month":
            assert len(p) == 7 and p[4] == "-"
        elif unit == "day":
            assert len(p) == 10
        else:
            assert len(p) == 16 and p.endswith(":00")
    periods = [s["period"] for s in d["series"]]
    assert periods == sorted(periods)

# 빈 범위 → 0
d = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/api/usage?unit=day&start=2000-01-01&end=2000-01-31"))
assert d["totals"]["requests"] == 0 and d["series"] == []

# 명시 범위 (오늘 포함, Asia/Seoul)
today = datetime.now(tz=KST).strftime("%Y-%m-%d")
d = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/api/usage?unit=day&start={today}&end={today}"))
assert d["totals"]["requests"] == len(rows), d
assert d["range"] == {"start": today, "end": today}
EOF
"$PY" "$T/t_api.py" >/dev/null 2>&1 \
  && ok "8.5 unit=month/day/hour + 범위 조회, DB 집계와 일치 (KST)" \
  || { bad "8.5 unit=month/day/hour + 범위 조회, DB 집계와 일치 (KST)"; "$PY" "$T/t_api.py" || true; }

CODE=$(curl -s -o "$T/web.html" -w '%{http_code}' "$PROXY_URL/")
[ "$CODE" = "200" ] && grep -q "tokenbills" "$T/web.html" \
  && ok "8.5/8.7 웹 페이지 서빙 (GET /)" \
  || bad "8.5/8.7 웹 페이지 서빙 (GET /)"

# ---------------------------------------------------------------- 8.3 중간 끊김
cat > "$T/t_drop.py" <<EOF
import httpx
port = $PROXY_PORT
with httpx.Client(timeout=10) as client:
    with client.stream("POST", f"http://127.0.0.1:{port}/v1/chat/completions",
                       json={"model": "mock-model", "messages": [], "stream": True}) as r:
        assert r.status_code == 200
        for line in r.iter_lines():
            if line.startswith("data:"):
                break
# 여기에서 연결을 갑자기 닫음 (컨텍스트 종료 → TCP close)
EOF
"$PY" "$T/t_drop.py" >/dev/null 2>&1
sleep 1.5
kill -0 "$PROXY_PID" >/dev/null 2>&1 && curl -sf "$PROXY_URL/health" | grep -q '"ok"' \
  && ok "8.3 클라이언트 중간 끊김 → 프로세스 생존, /health 응답" \
  || bad "8.3 클라이언트 중간 끊김 → 프로세스 생존, /health 응답"

# ---------------------------------------------------------------- 8.6 견고성
kill "$MOCK_PID" >/dev/null 2>&1
MOCK_PID=""
sleep 0.5
CODE_VIA=$(curl -s -o "$T/err502.json" -w '%{http_code}' -X POST "$PROXY_URL/v1/chat/completions" \
  -H 'content-type: application/json' -d "$B")
[ "$CODE_VIA" = "502" ] && ok "8.6 upstream 다운 시 502 반환" || bad "8.6 upstream 다운 시 502 반환 — got $CODE_VIA"

H=$(curl -s "$PROXY_URL/health")
A=$(curl -s -o /dev/null -w '%{http_code}' "$PROXY_URL/api/usage?unit=day")
W=$(curl -s -o /dev/null -w '%{http_code}' "$PROXY_URL/")
[ "$H" = '{"status":"ok"}' ] && [ "$A" = "200" ] && [ "$W" = "200" ] \
  && ok "8.6 vLLM 다운에도 /health, /api/usage, 웹 페이지 동작" \
  || bad "8.6 vLLM 다운에도 /health, /api/usage, 웹 페이지 동작 (health=$H api=$A web=$W)"

echo
echo "결과: PASS $PASS / FAIL $FAIL"
[ "$FAIL" -eq 0 ] || exit 1
