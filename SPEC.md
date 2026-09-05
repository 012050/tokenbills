# tokenbills — 로컬 LLM 토큰 가치 추적 서비스 개발 스펙

> 이 문서는 다른 LLM 에이전트가 이 폴더(`tokenbills/`)에 서비스를 **처음부터 구현**하도록 작성한 개발 요구사항 문서이다.
> 문서를 처음부터 끝까지 읽고, 5절의 파일 구조, 6절의 요구사항, 8절의 수락 기준을 모두 만족하는 구현을 완성하세요.
> 중간에 판단이 안 설 때: 이 문서의 선택(예: FastAPI, SQLite, 바닐라 JS)을 따르세요.

---

## 1. 목적

- vLLM(로컬 LLM 서버) 앞에 **프록시 서비스**를 두어, 내부망의 여러 클라이언트가 생성/소비하는 토큰을 자동 집계한다.
- 핵심 기능 4가지:
  1. **8000번 포트**로 들어오는 요청을 모두 vLLM으로 전달(클라이언트 코드 변경: 0)
  2. 입력(prompt) / 생성(completion) 토큰의 값을 **원화(KRW)** 로 계산
  3. 토큰량을 **시간**과 함께 **기록**(시간대 단위 집계 가능)
  4. **월 / 일 / 시간별** 토큰 사용량을 보는 **웹 페이지** — 사용자가 **범위(기간)를 지정**해 원하는 구간만 조회
- 실제로 돈을 받는 결제/정산 기능은 **아니다**. 절감액/가치 추적 + 대시보드 서비스다.

## 2. 실행 환경 (실측된 사실 — 이 전제에서 구현할 것)

| 항목 | 값 |
|---|---|
| vLLM 서버 | 호스트 공개 포트 `192.168.1.221:8001` (bridge + 포트 매핑, 컨테이너 포트는 8000 유지, 내부 LAN, HTTP, 인증 없음) |
| vLLM 이미지 | `vllm/vllm-openai:v0.26.0-cu129` (docker compose 서비스명 `vllm`) |
| 클라이언트 | 내부 LAN의 PC 다수 (OpenAI 호환 SDK를 vLLM으로 돌리는 앱들) |
| 프록시 배포 위치 | vLLM 서버(192.168.1.221)에서 **별도(독립) docker-compose 스택**, `network_mode: host` — vLLM compose에는 아무것도 추가하지 않음. 대외 포트 `8000` (vLLM 기존 포트 승계 → 클라이언트 변경 불필요) |
| 웹 페이지 접근 | 내부 LAN의 어떤 PC에서도 `http://192.168.1.221:8000/` (읽기 전용) |
| 타임존 | `Asia/Seoul` (컨테이너에 `TZ=Asia/Seoul` 설정) |

### 보안 전제 (중요)
- vLLM에는 인증이 없다. 프록시도 **내부 LAN 전용**으로만 열 것. 공개 인터넷 노출용 기능(공인 IP, 도메인, 인증서)은 구현 대상이 아니다.
- vLLM 호스트 포트는 8000 → **8001**로 이동(컨테이너 포트는 8000 유지). **8001은 관리자 점검 전용 직결**이다 — 프록시를 우회하므로 집계에서 빠진다. 클라이언트 트래픽은 모두 프록시 경유가 원칙.

## 3. 아키텍처

```
PC A ─┐
PC B ─┼─→ [tokenbills 프록시 :8000] ─→ [vLLM :8001]
PC C ─┘         (동작: 포워딩 + 토큰 기록)
                 │
                 └─→ SQLite (tokenbills.db)
                         │
      모든 PC에서 읽기: http://192.168.1.221:8000/ (웹 페이지), /api/usage
```

- 클라이언트는 **변경 불필요**다. 프록시가 vLLM의 원래 포트(8000)를 승계하므로, 기존 `base_url`(`http://192.168.1.221:8000`) 트래픽이 그대로 프록시로 흘러들어온다.
- 프록시는 OpenAI 호환 API를 **통과시키는(passthrough)** 게 제1원칙이다. 응답 본문은 upstream 그대로 반환한다(메타데이터 추가 금지 — 기존 클라이언트 호환성).

## 4. 기술 스택 (고정)

- Python 3.12, FastAPI, httpx(비동기), SQLite(표준 라이브러리 `sqlite3`, ORM 금지)
- 웹 페이지: `static/index.html` 단일 파일, 바닐라 HTML/CSS/JS, **외부 CDN/빌드 단계 금지** (서버가 오프라인이어도 열릴 것)
- 컨테이너: `python:3.12-slim`

## 5. 파일 구조

```
tokenbills/
├── SPEC.md              # 이 문서 (읽기 전용)
├── main.py              # FastAPI 앱 전체 (프록시 + usage API + 웹 페이지 서빙)
├── static/
│   └── index.html       # 웹 페이지 (단일 파일)
├── test/
│   ├── mock_vllm.py     # 통합 테스트용 모의 vLLM 서버
│   └── run_tests.sh     # 통합 테스트 실행 스크립트
├── Dockerfile
├── docker-compose.yml   # tokenbills 전용 독립 compose (vLLM compose에 추가하지 않음)
└── .gitignore           # tokenbills.db, __pycache__/
```

## 6. 기능 요구사항

### 6.1 프록시 (main.py)

1. 라우트: `GET/POST /v1/{path:path}` 전체를 `{VLLM_BASE_URL}/v1/{path}` 로 포워딩.
   - `VLLM_BASE_URL`은 환경변수, 기본값 `http://localhost:8001`. 프록시는 `network_mode: host`로 돌기 때문에 vLLM(호스트 포트 8001)은 같은 호스트의 `localhost:8001`으로 직접 도달한다 — 운영 compose와 개발 머신 모두 동일.
2. **리버스 프록시(nginx 방식) passthrough**: 클라이언트 요청 헤더를 vLLM으로 그대로 전달한다(hop-by-hop 헤더 제외)하고, **`X-Forwarded-For`에 실제 클라이언트 IP를 첨부**한다 (클라이언트가 이미 XFF를 보냈다면 체인 끝에 append, 첫 항목이 최초 소스). 프록시는 `network_mode: host`이므로 실제 클라이언트 IP를 직접 얻는다.
   - 참고: vLLM의 기본 접근 로그는 TCP 피어(프록시 경유이므로 `127.0.0.1`)를 표시하지 않고, XFF도 표시하지 않는다 — vLLM 로그에 실제 클라이언트 IP를 보이게 하려면 **vLLM 쪽 로그 설정**이 필요하며, 이는 운영자 영역이다(tokenbills 범위 밖).
3. upstream의 응답(상태 코드, 본문)을 **동일하게** 반환한다(응답 본문 무손실).
4. upstream에 도달할 수 없으면 502를 반환하고 에러 로그만 남긴다. 웹 페이지/API는 계속 동작한다.
5. 로그: uvicorn 기본 로그 + 프록시 에러만 stderr에 기록. **정규 요청은 로그 스팸 금지**.

### 6.2 토큰 기록

1. 기록 대상 엔드포인트: `chat/completions`, `completions`, `embeddings` (POST). 그 외(`/v1/models` 등)는 포워딩만 하고 기록하지 않는다.
2. **non-streaming**: upstream JSON 응답의 `usage` 필드를 읽고 기록한다. `usage`가 없으면 **기록하지 않는다**(추정 금지).
3. **streaming (SSE)**:
   - 요청 본문에 `stream_options.include_usage`가 없으면 `{"include_usage": true}`를 주입해 보낸다. (vLLM은 이 옵션이 있을 때 마지막 chunk에 `usage`를 포함)
   - 응답 SSE를 클라이언트로 그대로 스트리밍하며, `data:` 라인을 파싱해 마지막에 등장하는 `usage` 객체를 수집한다.
   - 클라이언트가 중간에 연결을 끊으면, 그 시점까지 수집한 usage(있으면 기록, 없으면 기록 없음)로 조용히 마무리한다. 에러 로그만 남긴다. 프로세스는 죽지 않아야 한다.
4. 기록된 요청 1건당 1행 INSERT.

### 6.3 가격 설정 (환경변수) — KRW 단일 요율

- **모델 구별 없음** — 모든 모델이 동일한 요율을 적용받는다.
- 설정 변수 (환경변수 — 운영자가 임의로 수정):
  - `PROMPT_PRICE_KRW` — 입력 1M 토큰당 원화 (기본 `6872`)
  - `COMPLETION_PRICE_KRW` — 출력(생성) 1M 토큰당 원화 (기본 `34360`)
- 비용 공식 — **DB에 저장하지 않고 조회/표시 시 계산**한다 (가격을 수정하면 과거 데이터에도 즉시 반영):
  - `cost_krw = prompt_tokens*p/1e6 + completion_tokens*c/1e6` (p, c는 위 환경변수 값, KRW/1M)

### 6.4 데이터베이스 (SQLite)

- 파일: `tokenbills.db` (경로: 환경변수 `TOKENBILLS_DB`, 기본 `./tokenbills.db`). 컨테이너에선 볼륨 마운트.
- 스키마:
```sql
CREATE TABLE IF NOT EXISTS usage (
    id                TEXT PRIMARY KEY,      -- uuid4 hex
    ts                REAL NOT NULL,         -- unix epoch (초, 소수 허용)
    prompt_tokens     INTEGER NOT NULL,
    completion_tokens INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_usage_ts ON usage (ts);
```
- 쓰기: 기록된 요청 1건당 1행 INSERT. 별도 purging 정책 없음(후속 요구사항).

### 6.5 usage API

`GET /api/usage?unit=month|day|hour&start=YYYY-MM-DD&end=YYYY-MM-DD`

- `unit`: 집계 단위 — `month` / `day` / `hour` (기본 `day`)
- `start` / `end`: 조회 범위 (선택, 기본: 현재 월 1일 ~ 현재). **Asia/Seoul** 기준.

응답 JSON:
```json
{
  "unit": "day",
  "range": {"start": "2026-09-01", "end": "2026-09-30"},
  "totals": {
    "requests": 1234,
    "prompt_tokens": 5000000,
    "completion_tokens": 2000000,
    "total_tokens": 7000000,
    "cost_krw": 12345678.9
  },
  "series": [
    {"period": "2026-09-01", "requests": 100, "prompt_tokens": 500000,
     "completion_tokens": 200000, "total_tokens": 700000, "cost_krw": 456789.0},
    {"period": "2026-09-02", "requests": 95, "prompt_tokens": 480000,
     "completion_tokens": 190000, "total_tokens": 670000, "cost_krw": 430000.0}
  ]
}
```

- `period` 형식: month `YYYY-MM`, day `YYYY-MM-DD`, hour `YYYY-MM-DD HH:00`
- `series`는 period 오름차순. 빈 기간은 메우지 않아도 된다(웹 페이지가 처리).

### 6.6 웹 페이지 (static/index.html)

- 한국어 UI, 다크 테마. 외부 의존성 0 (CDN 폰트/라이브러리 금지).
- 구성:
  1. 상단: 제목 `tokenbills`, 집계 단위 셀렉트(월/일/시간), 범위 시작·끝 날짜 입력, 마지막 갱신 시각, 수동 새로고침 버튼
  2. 요약: 선택 범위 총 토큰(입력/생성 분해), 원화 가치, 요청 수
  3. 사용량 차트: CSS 바 차트(라이브러리 금지), period별, hover 시 토큰/원화 tooltip
  4. period 상세 표: period / 입력 / 생성 / 합계 / 원화
- 자동 갱신 30초. 숫자 포맷: 천 단위 콤마, 원화는 정수.

### 6.7 기타

- **docker compose로 실행 가능해야 한다**: 폴더에 딸린 `Dockerfile` + `docker-compose.yml`만으로 **단일 명령**(`docker compose up -d --build`)에 서비스가 빌드/시동되어야 한다. (7절의 compose가 그대로 산출물)
- API/프록시 라우트(`/api/*`, `/v1/*`, `/health`)를 제외한 모든 GET은 웹 페이지를 서빙한다.
- `/health` → `{"status": "ok"}`.

## 7. 배포 (vLLM 서버에서)

> **vLLM compose 파일은 이 프로젝트의 산출물이 아니고, tokenbills가 읽거나 수정하거나 재시작하는 일은 절대 없다.** 기존 `docker-compose.yml`은 **운영자가 직접** 수정한다 — 필요한 변경은 `vllm` 서비스의 `ports` `"8000:8000"` → `"8001:8000"` 1줄(컨테이너 포트 8000 유지)뿐이다.

1. 이 폴더를 vLLM 서버로 복사(예: `/opt/tokenbills`).
2. (전제 확인) vLLM 호스트 포트가 운영자에 의해 이미 8001로 옮겨져 있는지 확인 — `http://192.168.1.221:8001/v1/models` 이 응답하면 준비 완료.
3. tokenbills는 **독립 스택**으로 운영 — 이 폴더에 딸린 `docker-compose.yml`을 그대로 실행한다 (vLLM compose에 합치지 않는다):
```yaml
services:
  tokenbills:
    build: .
    container_name: tokenbills
    network_mode: host     # 포트 매핑 없이 호스트 8000에 직접 바인드
    environment:
      - VLLM_BASE_URL=http://localhost:8001
      - PROMPT_PRICE_KRW=6872
      - COMPLETION_PRICE_KRW=34360
      - TZ=Asia/Seoul
    volumes:
      - ./data:/app/data
    restart: always
```
   - `network_mode: host`: 컨테이너가 호스트 네트워크를 공유 — 앱이 호스트 포트 8000에 직접 바인드하고, upstream은 호스트의 `localhost:8001`으로 도달한다.
4. `cd /opt/tokenbills && docker compose up -d` → `http://192.168.1.221:8000/` 에서 웹 페이지 확인.
5. 클라이언트 변경은 **없다**(프록시가 원 포트 8000을 승계). vLLM 호스트 포트 8001은 관리자 직결 점검 전용이며, 직결 트래픽은 집계 대상이 아님을 운영 규칙으로 명시.
6. 롤백: tokenbills 쪽 `docker compose down` + (운영자 작업) vLLM `ports`를 `"8000:8000"` 으로 되돌리고 `docker compose up -d vllm`.
7. `Dockerfile`은 `python:3.12-slim` 기반, `WORKDIR /app`, `ENV TOKENBILLS_DB=/app/data/tokenbills.db`, `EXPOSE 8000`, `CMD ["uvicorn","main:app","--host","0.0.0.0","--port","8000"]` (host network이므로 앱이 직접 바인드하는 포트가 대외 포트). `VOLUME /app/data`.

## 8. 수락 기준 (모두 통과해야 완성)

1. **passthrough**: 프록시로 `/v1/chat/completions` 등 호출 시 upstream 응답과 **동일한 본문**을 반환한다 (non-streaming과 streaming SSE 모두). vLLM으로 전달되는 요청에는 클라이언트 헤더 전체 + `X-Forwarded-For`(실제 클라이언트 IP)가 포함된다 (nginx 방식).
2. **기록**: `usage`가 있는 요청은 DB에 `prompt_tokens`/`completion_tokens`와 시각이 일치하는 행이 1건 기록된다. `usage`가 없는 요청은 기록되지 않는다.
3. **streaming**: `stream: true` 요청 시 SSE가 끊김 없이 전달되고, 마지막 chunk의 `usage`가 DB에 기록된다. 클라이언트가 중간에 끊으면 프로세스가 죽지 않는다.
4. **가격**: `cost_krw`가 `prompt*p/1e6 + completion*c/1e6`로 정확히 계산된다 (p/c는 KRW/1M 토큰 — 기본 입력 6,872/출력 34,360, 모델 무관). 환경변수 `PROMPT_PRICE_KRW`/`COMPLETION_PRICE_KRW`가 있으면 그 값이 우선.
5. **웹 페이지**: `unit=month/day/hour` 및 임의 범위 조회의 합계가 해당 범위 DB 집계와 일치한다. 시간은 Asia/Seoul 기준.
6. **견고성**: vLLM이 죽으면 프록시는 502를 반환하고, 웹 페이지/API는 계속 동작한다.
7. **docker compose**: `docker compose up -d --build` 후 서비스가 시동되어 8000 포트의 `/health`가 응답하고 웹 페이지가 서빙된다.

## 9. 테스트 방법

- `test/mock_vllm.py`: FastAPI로 만든 모의 vLLM.
  - `GET /v1/models` → 모델 목록
  - `POST /v1/chat/completions` → non-streaming: 고정 `usage {prompt_tokens:123, completion_tokens:45}`. streaming: 3개 delta chunk + usage 포함 최종 chunk + `data: [DONE]`
  - `model: "fail-500"` 요청에 500 에러 본문 반환 (에러 전파 테스트용)
  - **실제 vLLM 서버로 테스트하지 않는다** — 테스트는 프록시의 계약(본문 무손실, usage 기록, 에러 전파)을 검증하는 것이며, 실물은 응답이 매번 달라 정확한 단언이 불가능하고 500/다운 시나리오도 재현할 수 없다. mock은 vLLM의 wire format(엔드포인트/JSON/SSE, `include_usage` 시에만 usage 포함)을 그대로 재현한다. 실물 검증은 7절의 배포 단계에서 한다.
- `test/run_tests.sh`: mock(포트 8100)과 tokenbills(포트 8000, `VLLM_BASE_URL=http://127.0.0.1:8100`, 임시 DB; 개발 머신 포트 충돌 시 `PROXY_PORT` 환경변수로 대체 포트 지정)를 띄우고 8절 항목을 `curl`/`python -c`로 검증한 후 정리.
- 개발 머신에는 Python 3.12 + fastapi + httpx + uvicorn이 이미 설치되어 있다.
- **통합 테스트는 실제 vLLM에 의존하지 않는다** (mock만 사용) — vLLM 포트 전환(8000→8001) 전에 개발/테스트를 전부 끝낼 수 있다. 개발 중 실제 vLLM 컨테이너/compose에 접근하거나 재시작하는 일은 하지 않는다.

## 10. 비목표 (실현 금지 — 범위 밖)

- 실제 결제/정산, 사용자 계정, API 키 인증, 멀티테넌트 쿼터
- GPU/전력 등 하드웨어 지표, 모델별 성능 벤치마크
- 공개 인터넷 노출, TLS, rate limiting
- 클라이언트(IP)/모델 식별 및 클라이언트별·모델별 집계
- USD 환산, 요청 상세(최근 요청 목록) 표시
- 토큰 수의 추정/추정치 기록(usage가 없으면 기록하지 않는다)
- vLLM compose/설정 변경 (ports 1줄은 운영자가 직접 수정 — tokenbills는 vLLM 배포물에 어떤 접근도 하지 않는다)
