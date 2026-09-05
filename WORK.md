# WORK.md — tokenbills 작업 기록

> 이 문서는 `SPEC.md`에 따라 tokenbills 서비스를 개발한 과정과 현재 상태를 정리한 기록이다.
> (git 미사용 — 사용자 지시. 문서화만 목적.)

---

## 1. 개요

- **목적**: 로컬 vLLM 서버 앞에 리버스 프록시(`:8000`)를 두어 내부 LAN 클라이언트의 LLM 토큰 사용량을 원화(KRW) 가치로 추적·집계하는 서비스. 실제 결제 기능 없음.
- **아키텍처**: 클라이언트 → tokenbills 프록시 `:8000`(vLLM 원 포트 승계, 클라이언트 변경 0) → vLLM `:8001` → SQLite(`data/tokenbills.db`). 모든 PC에서 `http://<서버>:8000/` 웹 대시보드(읽기 전용) + `/api/usage`.
- **기술 스택**: Python 3.12 / FastAPI / httpx(비동기) / sqlite3(ORM 무) / 바닐라 HTML·CSS·JS(외부 CDN 0) / `python:3.12-slim` 컨테이너, `network_mode: host`.

## 2. 작업 환경

| 항목 | 값 |
|---|---|
| 개발/배포 머신 | vLLM 서버 (192.168.1.221, vLLM host 포트 8001 운영 확인) |
| Python | 3.12.3 — 의존성은 `.venv` 가상환경에 설치 (fastapi 0.141.1, httpx 0.28.1, uvicorn 0.52.4, starlette 1.6.0) |
| Docker | 29.6.2 |
| 타임존 | Asia/Seoul (컨테이너 `TZ=Asia/Seoul`) |

## 3. 산출 파일

| 파일 | 내용 |
|---|---|
| `main.py` | FastAPI 앱 전체 — 프록시 + 토큰 기록 + `/api/usage` + `/health` + 웹 페이지 폴백 서빙 |
| `static/index.html` | 대시보드 단일 파일 (한국어/다크, CSS 바 차트 + hover tooltip, period 상세 표, 30초 자동 갱신, 외부 의존성 0) |
| `test/mock_vllm.py` | 모의 vLLM — vLLM/OpenAI wire format 재현, 결정적 응답 (chat completions, responses, embeddings, models, `fail-500`, `no-usage`) |
| `test/run_tests.sh` | 통합 테스트 — mock(8100) + tokenbills(기본 8000, `PROXY_PORT`로 대체 가능) 기동 후 SPEC 8절 수락 기준 검증, 임시 DB, 자동 정리 |
| `Dockerfile` | `python:3.12-slim`, `WORKDIR /app`, `ENV TOKENBILLS_DB=/app/data/tokenbills.db`, `EXPOSE 8000`, `VOLUME /app/data`, `CMD uvicorn main:app --host 0.0.0.0 --port 8000` |
| `docker-compose.yml` | SPEC 7절과 동일 — 독립 스택, `network_mode: host`, `VLLM_BASE_URL=http://localhost:8001`, 요율 env, `./data:/app/data`, `restart: always` |
| `.gitignore` | `tokenbills.db`, `__pycache__/`, `.venv/`, `data/` |
| `.venv/` | 개발 가상환경 (라이브러리 설치·테스트 전부 이 환경에서 수행) |
| `data/` | 볼륨 마운트 포인트 — 운영 DB 위치 (컨테이너가 root로 생성/쓰기) |

## 4. 구현 포인트 (main.py)

### 4.1 프록시
- `GET/POST /v1/{path}` 전체를 `VLLM_BASE_URL`(기본 `http://localhost:8001`)로 포워딩.
- nginx 방식 passthrough: 클라이언트 헤더 통전(hop-by-hop 제거, `content-length` 재계산), `X-Forwarded-For`에 실제 클라이언트 IP append(기존 체인 보존), 응답 본문 바이트 무손실(응답 헤더에서 `content-encoding` 제거 후 재전송 — httpx가 투명 디코딩하므로).
- upstream 미도달 시 **502** + stderr 에러 로그. 정규 요청 로그 스팸 없음(uvic 기본 로그만).
- `/health` → `{"status":"ok"}`, API/프록시/health 외의 GET 전부 웹 페이지 서빙.

### 4.2 토큰 기록
- **기록 대상**: `chat/completions`, `completions`, `embeddings`, **`responses`** (4.3 참고).
- non-streaming: 응답 JSON의 `usage`를 읽어 1행 INSERT. **usage 없으면 기록 안 함 (추정 금지 — 스펙 원칙)**.
- streaming(SSE):
  - `chat/completions`·`completions`만 요청 본문에 `stream_options.include_usage`가 없으면 주입 (Responses API는 옵션 없이 usage 포함 — 주입 불가/불필요).
  - SSE를 클라이언트에 그대로 흘려보내며 `data:` 라인에서 usage 수집. 위치: chunk 최상위 `usage`(completions 계열) + `response.usage` 중첩(Responses API의 `response.completed` 이벤트). 마지막(최종) usage가 유효.
  - 클라이언트 중간 끊김/상游 장애 시 수집된 usage(있으면 기록, 없으면 미기록)로 조용히 마무리 — 프로세스 생존 확인됨.
- **필드 매핑**: completions `prompt_tokens`/`completion_tokens`, Responses `input_tokens`→prompt / `output_tokens`→completion 자동 대응. `completion_tokens`/`output_tokens` 누락 시 0.

### 4.3 Responses API 확장 (2026-09-02 현장 대응)
- **현상**: 운영 트래픽이 `POST /v1/responses`(OpenAI Responses API)로 몰리는데 스펙 기록 대상에 없어 대시보드 수치가 0.
- **실측 확인** (vLLM `vllm/vllm-openai`, 모델 qwen):
  - non-streaming: 응답 최상위 `usage: {input_tokens, output_tokens, total_tokens, ...}` (옵틴 필요 없음)
  - streaming: `event: response.completed` 이벤트의 `response.usage`에 중첩, `data: [DONE]` 없음
- **조치**: 기록 대상에 `responses` 추가 + 위 필드 매핑 + SSE 중첩 usage 파싱 + include_usage 주입 범위 제한(completions 계열만).
- **검증**: 컨테이너 재빌드 후 실 vLLM 요청 2건(스트리밍 포함) 경유 → DB (54, 5)×2 기록, `/api/usage`가 `requests:2, prompt 108, completion 10, cost 1.09원` 반환 확인.

### 4.4 DB / 가격 / API
- SQLite: `TOKENBILLS_DB` env (기본 `./tokenbills.db`, 컨테이너 `/app/data/tokenbills.db`), 스펙 스키마 그대로(`id` uuid4 hex, `ts` REAL epoch, `prompt_tokens`, `completion_tokens` + `ts` 인덱스), WAL 모드.
- **비용은 저장 안 함** — 조회 시 `prompt*p/1e6 + completion*c/1e6` 계산 (가격 수정 시 과거 데이터에도 즉시 반영).
- 요율: `PROMPT_PRICE_KRW`(기본 6872) / `COMPLETION_PRICE_KRW`(기본 34360) KRW/1M 토큰, 모델 무관.
- `GET /api/usage?unit=month|day|hour&start=YYYY-MM-DD&end=YYYY-MM-DD` — Asia/Seoul 기준, 기본 범위 "현재 월 1일 ~ 오늘(종료일整天 포함)", period 형식 `YYYY-MM` / `YYYY-MM-DD` / `YYYY-MM-DD HH:00`, series 오름차순.

## 5. 테스트 및 검증

- `bash test/run_tests.sh` → **PASS 16 / FAIL 0** (8000 점유 시 `PROXY_PORT=8002 PROXY2_PORT=8004`로 실행)
  - 8.1 passthrough: `/v1/models`·non-streaming·**Responses API(non-stream + SSE)** 본문 바이트 동일, XFF(클라이언트 IP) + 커스텀 헤더 전달, upstream 500 전파(코드+본문)
  - 8.2 기록: chat non/stream 각 1행(123/45), usage 없으면 미기록(chat·responses), embeddings `completion 0` 기록, responses `input/output_tokens` 매핑(31/7)
  - 8.3 streaming: SSE 무손실, 중간 끊김 후 프로세스 생존 + `/health` 응답
  - 8.4 가격: 기본 요율 공식(1e-9 오차) + env 요율 우선(독립 인스턴스로 2000/4000 검증)
  - 8.5 API/웹: 3개 unit + 임의 범위 합계가 DB 집계와 일치(KST), 빈 범위 0, period 형식/순서, 웹 페이지 서빙
  - 8.6 견고성: upstream kill 시 502, 웹/API/health 생존
- **docker compose 라이브 검증** (8.7): `docker compose up -d --build` → 8000 `/health` OK, 웹 페이지 서빙, **실 vLLM(8001) 경유 `/v1/models` 성공**, 컨테이너 내 `TZ=Asia/Seoul` 확인.
- 개발 중 실제 vLLM 컨테이너/compose는 건드리지 않음 (스펙 9절). 실 검증은 GET/소량 token 요청 수준으로만 수행.

## 6. 배포 및 현재 상태

- **현재**: 이 폴더(`/home/epyc/tokenbills`)에서 `docker compose up -d` 운영 중 (컨테이너 `tokenbills`, host network, 8000 바인드). 클라이언트 `base_url`(`http://192.168.1.221:8000`) 트래픽이 프록시로 유입·집계되는 상태 확인됨.
- **권장 정규 배포** (SPEC 7절): 폴더를 `/opt/tokenbills`로 복사 후 `docker compose up -d`. (현재 위치 운영도 기능상 동일 — `data/`가 DB 볼륨.)
- **운영 규칙**: 8001은 관리자 직결 점검 전용(집계 밖). 클라이언트 트래픽은 8000(프록시) 경유가 원칙.
- **롤백**: tokenbills `docker compose down` + 운영자가 vLLM `ports`를 `"8000:8000"`으로 복원.
- **주의**: `data/tokenbills.db`는 컨테이너가 root로 소유 → 호스트 epyc 계정은 읽기 전용(쓰기 불가). DB 확인은 `docker exec tokenbills python3 -c "import sqlite3; ..."` 또는 root로.
- **누락 구간**: Responses API 대응(4.3) 이전 트래픽은 usage가 없던(=기록 대상이 아니던) 요청이라 영구히 미집계 — 스펙상 추정이 금지되어 있음.

## 7. 스펙 대비 이점/편차

- **편차 1** (기능 추가): 기록 대상에 `responses` 추가 — 스펙 6.2의 3개 엔드포인트를 초과. 실 클라이언트가 Responses API를 쓰도록 확인되어 목적(토큰 자동 집계) 달성을 위해 확장. `include_usage` 주입은 completions 계열로 제한해 스펙 6.2.2 동작은 그대로 유지.
- **편차 2** (파일 추가): `WORK.md`(본 문서), `.venv/`(개발 가상환경), `.gitignore`에 `.venv/`·`data/` 추가.
- **편차 3** (세부): DB에 WAL 모드 설정(동시 읽기/쓰기 개선, 스펙 스키마 불변). Dockerfile에 `PYTHONUNBUFFERED=1`.
- 그 외 스펙 1~10절은 그대로 준수 (비목표 — 결제/인증/TLS/클라이언트별 집계/추정 기록 등 — 구현 없음).

## 8. 재현 명령

```bash
# 개발/테스트 (가상환경)
python3 -m venv .venv && .venv/bin/pip install fastapi httpx uvicorn
bash test/run_tests.sh                       # 8000 비어 있을 때
PROXY_PORT=8002 PROXY2_PORT=8004 bash test/run_tests.sh   # 8000 점유 시

# 운영
docker compose up -d --build                 # 8000에 서빙
docker logs -f tokenbills                    # 로그
docker compose down                          # 정지
```
