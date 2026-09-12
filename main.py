"""tokenbills — vLLM 앞 토큰 가치 추적 프록시 (SPEC.md 구현).

동작:
- GET/POST /v1/* 를 VLLM_BASE_URL 로 nginx 방식 리버스 프록시 (본문 무손실 passthrough)
- chat/completions, completions, embeddings 응답의 usage 를 SQLite 에 기록
- /api/usage : Asia/Seoul 기준 월/일/시간 집계 (원화 가치는 조회 시 계산)
- /health, 그리고 그 외 모든 GET 은 웹 페이지 (static/index.html) 서빙
"""

import ipaddress
import json
import logging
import os
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

VLLM_BASE_URL = os.environ.get("VLLM_BASE_URL", "http://localhost:8001").rstrip("/")
TOKENBILLS_DB = os.environ.get("TOKENBILLS_DB", "./tokenbills.db")
TRUSTED_PROXIES = os.environ.get("TRUSTED_PROXIES", "")
DEFAULT_PROMPT_PRICE_KRW = 6872.0
DEFAULT_COMPLETION_PRICE_KRW = 34360.0

TRACKED_ENDPOINTS = {"chat/completions", "completions", "embeddings", "responses"}
# completions 계열만 stream_options.include_usage 주입 대상.
# Responses API 는 별도 옵션 없이 최종 이벤트에 usage 를 포함한다.
INJECT_USAGE_ENDPOINTS = {"chat/completions", "completions"}

# RFC 7230 hop-by-hop 헤더 + 본문이 바뀔 수 있으니 재계산되는 content-length
HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}
REQUEST_DROP = HOP_BY_HOP | {"content-length"}
# httpx가 응답 바디를 투명하게 디코딩하므로 content-encoding 은 제거, 길이는 재계산
RESPONSE_DROP = HOP_BY_HOP | {"content-length", "content-encoding"}

log = logging.getLogger("tokenbills")

def _parse_trusted_proxies(raw: str) -> list:
    """TRUSTED_PROXIES (쉼표 구분, IP/CIDR) → network 객체 목록."""
    nets = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            nets.append(ipaddress.ip_network(item, strict=False))
        except ValueError:
            log.warning("invalid TRUSTED_PROXIES entry %r ignored", item)
    return nets


TRUSTED_PROXY_NETS = _parse_trusted_proxies(TRUSTED_PROXIES)


try:
    KST = ZoneInfo("Asia/Seoul")
except Exception:  # tzdata 없는 환경 폴백 — 서울은 1988년 이후 연중 UTC+9
    KST = timezone(timedelta(hours=9))

STATIC_DIR = Path(__file__).resolve().parent / "static"
INDEX_HTML = STATIC_DIR / "index.html"


# --------------------------------------------------------------------------- DB


def _connect() -> sqlite3.Connection:
    return sqlite3.connect(TOKENBILLS_DB, timeout=10)


def init_db() -> None:
    conn = _connect()
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS usage (
                id                TEXT PRIMARY KEY,
                ts                REAL NOT NULL,
                client_ip         TEXT NOT NULL DEFAULT '',
                prompt_tokens     INTEGER NOT NULL,
                completion_tokens INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_usage_ts ON usage (ts)")
        # 기존 운영 DB 마이그레이션 (client_ip 컬럼 추가 전 생성된 경우)
        cols = {row[1] for row in conn.execute("PRAGMA table_info(usage)")}
        if "client_ip" not in cols:
            conn.execute(
                "ALTER TABLE usage ADD COLUMN client_ip TEXT NOT NULL DEFAULT ''"
            )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_usage_client_ts ON usage (client_ip, ts)")
        conn.commit()
    finally:
        conn.close()


def record_usage(prompt_tokens: int, completion_tokens: int, client_ip: str = "") -> None:
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO usage (id, ts, client_ip, prompt_tokens, completion_tokens)"
            " VALUES (?, ?, ?, ?, ?)",
            (uuid.uuid4().hex, time.time(), client_ip, int(prompt_tokens), int(completion_tokens)),
        )
        conn.commit()
    except (sqlite3.Error, TypeError, ValueError):
        log.exception("failed to record usage")
    finally:
        conn.close()


def _normalize_usage(usage: object) -> tuple[int, int] | None:
    """completions(prompt/completion_tokens)과 Responses(input/output_tokens) 양식 모두 지원."""
    if not isinstance(usage, dict):
        return None
    prompt = usage.get("prompt_tokens")
    if prompt is None:
        prompt = usage.get("input_tokens")
    if prompt is None:
        return None
    completion = usage.get("completion_tokens")
    if completion is None:
        completion = usage.get("output_tokens")
    if completion is None:
        completion = 0
    return prompt, completion


def _record_if_any(usage: object, client_ip: str = "") -> None:
    """usage 객체가 있으면 1행 기록, 없으면 아무것도 안 한다 (추정 금지)."""
    normalized = _normalize_usage(usage)
    if normalized is None:
        return
    prompt_tokens, completion_tokens = normalized
    record_usage(prompt_tokens, completion_tokens, client_ip)


def _usage_from_json(raw: bytes) -> object:
    try:
        data = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return None
    if isinstance(data, dict):
        return data.get("usage")
    return None


# ------------------------------------------------------------------ SSE parsing


def _parse_sse_chunk(chunk: bytes, state: dict) -> None:
    """SSE 바이트를 라인 단위로 파싱해 마지막 usage 객체를 state 에 수집한다."""
    buf = state["buf"] + chunk
    while True:
        idx = buf.find(b"\n")
        if idx < 0:
            break
        line = buf[:idx]
        buf = buf[idx + 1:]
        if line.startswith(b"data:"):
            payload = line[5:].strip()
            if payload and payload != b"[DONE]":
                try:
                    obj = json.loads(payload)
                except ValueError:
                    obj = None
                if isinstance(obj, dict):
                    usage = obj.get("usage")
                    if not isinstance(usage, dict):
                        nested = obj.get("response")
                        if isinstance(nested, dict):
                            usage = nested.get("usage")
                    if isinstance(usage, dict):
                        state["usage"] = usage
    if len(buf) > 1_000_000:
        log.warning("SSE line buffer exceeded limit; dropping partial line")
        buf = b""
    state["buf"] = buf


# ------------------------------------------------------------------------ app


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    client = httpx.AsyncClient(
        base_url=VLLM_BASE_URL,
        timeout=httpx.Timeout(connect=10.0, read=None, write=60.0, pool=10.0),
    )
    _app.state.upstream = client
    log.info("tokenbills proxy started (upstream=%s, db=%s)", VLLM_BASE_URL, TOKENBILLS_DB)
    yield
    await client.aclose()


app = FastAPI(title="tokenbills", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)


def _is_trusted_proxy(ip: str) -> bool:
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return any(addr in net for net in TRUSTED_PROXY_NETS)


def _client_ip(request: Request) -> str:
    """클라이언트 IP: 신뢰 프록시(예: nginx)에서 온 요청은 XFF 첫 항목, 그 외는 TCP 소스 IP."""
    peer = request.client.host if request.client is not None else "unknown"
    if _is_trusted_proxy(peer):
        first = request.headers.get("x-forwarded-for", "").split(",")[0].strip()
        if first:
            return first
    return peer


def _forward_headers(request: Request) -> dict:
    headers = {
        key: value for key, value in request.headers.items() if key not in REQUEST_DROP
    }
    client_ip = request.client.host if request.client is not None else "unknown"
    existing = headers.pop("x-forwarded-for", None)
    headers["x-forwarded-for"] = f"{existing}, {client_ip}" if existing else client_ip
    return headers


def _maybe_inject_include_usage(body: bytes) -> tuple[bytes, bool]:
    """streaming 요청에 stream_options.include_usage 가 없으면 주입한다."""
    if not body:
        return body, False
    try:
        data = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return body, False
    if not isinstance(data, dict) or not data.get("stream"):
        return body, False
    options = data.get("stream_options")
    if not isinstance(options, dict):
        options = {}
    if "include_usage" in options:
        return body, False
    data["stream_options"] = {**options, "include_usage": True}
    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8"), True


def _response_headers(resp: httpx.Response) -> dict:
    return {key: value for key, value in resp.headers.items() if key not in RESPONSE_DROP}


@app.api_route("/v1/{path:path}", methods=["GET", "POST"])
async def proxy(path: str, request: Request) -> Response:
    upstream: httpx.AsyncClient = request.app.state.upstream
    body = await request.body()
    is_tracked = request.method == "POST" and path in TRACKED_ENDPOINTS
    client_ip = _client_ip(request)
    if request.method == "POST" and path in INJECT_USAGE_ENDPOINTS:
        body, _ = _maybe_inject_include_usage(body)

    url = f"/v1/{path}"
    if request.url.query:
        url = f"{url}?{request.url.query}"

    try:
        req = upstream.build_request(
            request.method,
            url,
            headers=_forward_headers(request),
            content=body if request.method == "POST" else None,
        )
        resp = await upstream.send(req, stream=True)
    except httpx.HTTPError as exc:
        log.error("upstream unreachable for %s %s: %s", request.method, url, exc)
        return JSONResponse(status_code=502, content={"detail": "upstream unavailable"})

    if "text/event-stream" not in resp.headers.get("content-type", ""):
        try:
            raw = await resp.aread()
        except httpx.HTTPError as exc:
            log.error("upstream read error for %s %s: %s", request.method, url, exc)
            await resp.aclose()
            return JSONResponse(status_code=502, content={"detail": "upstream unavailable"})
        await resp.aclose()
        if is_tracked:
            _record_if_any(_usage_from_json(raw), client_ip)
        return Response(content=raw, status_code=resp.status_code, headers=_response_headers(resp))

    state = {"usage": None, "buf": b""}

    async def event_stream():
        try:
            async for chunk in resp.aiter_bytes():
                _parse_sse_chunk(chunk, state)
                yield chunk
        except httpx.HTTPError as exc:
            log.error("upstream stream error: %s", exc)
        finally:
            try:
                await resp.aclose()
            except Exception:
                pass
            if is_tracked:
                _record_if_any(state["usage"], client_ip)

    return StreamingResponse(
        event_stream(), status_code=resp.status_code, headers=_response_headers(resp)
    )


# -------------------------------------------------------------------- /health


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


# ------------------------------------------------------------------- /api/usage


def _parse_date(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=KST)
    except ValueError:
        raise HTTPException(status_code=422, detail="start/end must be YYYY-MM-DD")


def _env_price(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        log.warning("invalid %s=%r, using default %s", name, raw, default)
        return default


def _resolve_range(start: str | None, end: str | None) -> tuple[datetime, float, str]:
    """KST 기준 (start_dt, end_epoch, end_date_str) 범위 해석. /api/usage·/api/clients 공유."""
    now = datetime.now(tz=KST)
    if start is None:
        start_dt = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        start_dt = _parse_date(start)
    if end is None:
        end_date = now.date()
    else:
        end_date = _parse_date(end).date()
    if end_date < start_dt.date():
        raise HTTPException(status_code=422, detail="end must be on/after start")
    end_epoch = datetime.combine(
        end_date + timedelta(days=1), datetime.min.time(), tzinfo=KST
    ).timestamp()
    return start_dt, end_epoch, end_date.isoformat()


def _bucket_key(ts: float, unit: str) -> str:
    dt = datetime.fromtimestamp(ts, tz=KST)
    if unit == "month":
        return dt.strftime("%Y-%m")
    if unit == "hour":
        return dt.strftime("%Y-%m-%d %H:00")
    return dt.strftime("%Y-%m-%d")


@app.get("/api/usage")
async def api_usage(unit: str = "day", start: str | None = None, end: str | None = None, ip: str | None = None) -> dict:
    if unit not in ("month", "day", "hour"):
        raise HTTPException(status_code=422, detail="unit must be month, day or hour")

    start_dt, end_epoch, end_str = _resolve_range(start, end)

    sql = "SELECT ts, client_ip, prompt_tokens, completion_tokens FROM usage"
    sql += " WHERE ts >= ? AND ts < ?"
    params: list = [start_dt.timestamp(), end_epoch]
    if ip is not None:
        sql += " AND client_ip = ?"
        params.append(ip)
    sql += " ORDER BY ts"

    conn = _connect()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    buckets: dict[str, list] = {}
    for ts, client_ip, prompt_tokens, completion_tokens in rows:
        bucket = buckets.setdefault(_bucket_key(ts, unit), [0, 0, 0, {}])
        bucket[0] += 1
        bucket[1] += prompt_tokens
        bucket[2] += completion_tokens
        ip_key = client_ip or "unknown"
        bucket[3][ip_key] = bucket[3].get(ip_key, 0) + prompt_tokens + completion_tokens

    prompt_price = _env_price("PROMPT_PRICE_KRW", DEFAULT_PROMPT_PRICE_KRW)
    completion_price = _env_price("COMPLETION_PRICE_KRW", DEFAULT_COMPLETION_PRICE_KRW)

    total_prompt = sum(b[1] for b in buckets.values())
    total_completion = sum(b[2] for b in buckets.values())

    result = {
        "unit": unit,
        "range": {"start": start_dt.strftime("%Y-%m-%d"), "end": end_str},
        "totals": {
            "requests": sum(b[0] for b in buckets.values()),
            "prompt_tokens": total_prompt,
            "completion_tokens": total_completion,
            "total_tokens": total_prompt + total_completion,
            "cost_krw": total_prompt * prompt_price / 1e6
            + total_completion * completion_price / 1e6,
        },
        "series": [
            {
                "period": key,
                "requests": b[0],
                "prompt_tokens": b[1],
                "completion_tokens": b[2],
                "total_tokens": b[1] + b[2],
                "cost_krw": b[1] * prompt_price / 1e6 + b[2] * completion_price / 1e6,
                "clients": b[3],
            }
            for key, b in sorted(buckets.items())
        ],
    }
    if ip is not None:
        result["ip"] = ip
    return result


@app.get("/api/clients")
async def api_clients(start: str | None = None, end: str | None = None) -> dict:
    """클라이언트 IP별 사용량 집계. /api/usage와 동일 기간 파라미터, 토큰 합계 내림차순."""
    start_dt, end_epoch, end_str = _resolve_range(start, end)

    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT COALESCE(client_ip, '') AS client_ip, COUNT(*),"
            " SUM(prompt_tokens), SUM(completion_tokens)"
            " FROM usage WHERE ts >= ? AND ts < ? GROUP BY client_ip",
            (start_dt.timestamp(), end_epoch),
        ).fetchall()
    finally:
        conn.close()

    prompt_price = _env_price("PROMPT_PRICE_KRW", DEFAULT_PROMPT_PRICE_KRW)
    completion_price = _env_price("COMPLETION_PRICE_KRW", DEFAULT_COMPLETION_PRICE_KRW)

    clients = []
    for client_ip, requests_count, prompt_tokens, completion_tokens in rows:
        prompt_tokens = prompt_tokens or 0
        completion_tokens = completion_tokens or 0
        clients.append(
            {
                "client_ip": client_ip or "unknown",
                "requests": requests_count,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": prompt_tokens + completion_tokens,
                "cost_krw": prompt_tokens * prompt_price / 1e6
                + completion_tokens * completion_price / 1e6,
            }
        )
    clients.sort(key=lambda item: item["total_tokens"], reverse=True)

    return {
        "range": {"start": start_dt.strftime("%Y-%m-%d"), "end": end_str},
        "totals": {
            "requests": sum(c["requests"] for c in clients),
            "prompt_tokens": sum(c["prompt_tokens"] for c in clients),
            "completion_tokens": sum(c["completion_tokens"] for c in clients),
            "total_tokens": sum(c["total_tokens"] for c in clients),
            "cost_krw": sum(c["cost_krw"] for c in clients),
        },
        "clients": clients,
    }


# ------------------------------------------------------------- web page fallback


@app.get("/{path:path}")
async def web_fallback(path: str) -> FileResponse:
    return FileResponse(INDEX_HTML)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, proxy_headers=False)
