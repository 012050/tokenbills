"""mock vLLM — tokenbills 통합 테스트용 (실제 vLLM 없이 계약 검증).

vLLM/OpenAI wire format 을 재현하되 응답은 고정값이라 프록시 계약
(본문 무손실, usage 기록, 에러 전파)을 정확히 단언할 수 있다.

- GET  /v1/models        → 모델 목록
- POST /v1/chat/completions
    - non-streaming: 고정 usage {prompt_tokens:123, completion_tokens:45}
    - streaming: 3개 delta chunk + (include_usage 시 usage 포함) 최종 chunk + data: [DONE]
    - model "no-usage"  → usage 없는 응답 (기록되지 않아야 한다)
    - model "fail-500"  → 500 에러 본문 (에러 전파 테스트)
- POST /v1/embeddings    → usage {prompt_tokens:7} (completion_tokens 없음)
- POST /v1/responses     → Responses API (usage: input_tokens/output_tokens,
                            streaming 시 response.completed 이벤트의 response.usage)
- GET  /mock/last_request → 마지막 요청의 헤더/본문 (XFF 검증용)
"""

import asyncio
import json

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

app = FastAPI()

MODEL = "mock-model"
_last = {"headers": {}, "body": ""}


async def _capture(request: Request) -> dict:
    _last["headers"] = dict(request.headers.items())
    body = await request.body()
    _last["body"] = body.decode("utf-8", "replace")
    try:
        return json.loads(body)
    except ValueError:
        return {}


def _error_500() -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "message": "mock internal server error",
                "type": "server_error",
                "code": "mock_500",
            }
        },
    )


def _chunk(cid: str, model: str, delta: dict, finish=None, usage=None) -> dict:
    chunk = {
        "id": cid,
        "object": "chat.completion.chunk",
        "created": 1700000000,
        "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
    }
    if usage is not None:
        chunk["usage"] = usage
    return chunk


@app.get("/v1/models")
async def models():
    return {"object": "list", "data": [{"id": MODEL, "object": "model", "owned_by": "mock"}]}


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    data = await _capture(request)
    model = data.get("model", MODEL)

    if model == "fail-500":
        return _error_500()

    usage = {"prompt_tokens": 123, "completion_tokens": 45, "total_tokens": 168}
    if model == "no-usage":
        usage = None

    if not data.get("stream"):
        resp = {
            "id": "chatcmpl-mock",
            "object": "chat.completion",
            "created": 1700000000,
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "mock reply"},
                    "finish_reason": "stop",
                }
            ],
        }
        if usage is not None:
            resp["usage"] = usage
        return JSONResponse(content=resp)

    include_usage = bool((data.get("stream_options") or {}).get("include_usage"))

    async def gen():
        cid = "chatcmpl-mock-stream"
        chunks = [
            _chunk(cid, model, {"role": "assistant", "content": ""}),
            _chunk(cid, model, {"content": "mock "}),
            _chunk(cid, model, {"content": "reply"}),
        ]
        final = _chunk(cid, model, {}, "stop", usage if include_usage else None)
        chunks.append(final)
        for chunk in chunks:
            yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"
            await asyncio.sleep(0.3)
        yield "data: [DONE]\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.post("/v1/embeddings")
async def embeddings(request: Request):
    data = await _capture(request)
    if data.get("model") == "fail-500":
        return _error_500()
    return {
        "object": "list",
        "data": [{"object": "embedding", "index": 0, "embedding": [0.1, 0.2, 0.3]}],
        "model": data.get("model", MODEL),
        "usage": {"prompt_tokens": 7, "total_tokens": 7},
    }


@app.post("/v1/responses")
async def responses_api(request: Request):
    data = await _capture(request)
    model = data.get("model", MODEL)
    if model == "fail-500":
        return _error_500()
    usage = {"input_tokens": 31, "output_tokens": 7, "total_tokens": 38}
    if model == "no-usage":
        usage = None

    if not data.get("stream"):
        resp = {
            "id": "resp-mock",
            "object": "response",
            "status": "completed",
            "model": model,
            "output": [
                {
                    "id": "msg-mock",
                    "type": "message",
                    "status": "completed",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "mock reply"}],
                }
            ],
        }
        if usage is not None:
            resp["usage"] = usage
        return JSONResponse(content=resp)

    async def gen():
        events = [
            {
                "type": "response.created",
                "sequence_number": 0,
                "response": {"id": "resp-mock", "object": "response", "status": "in_progress", "model": model, "usage": None},
            },
            {"type": "response.output_text.delta", "sequence_number": 1, "delta": "mock "},
            {
                "type": "response.completed",
                "sequence_number": 2,
                "response": {"id": "resp-mock", "object": "response", "status": "completed", "model": model, "usage": usage},
            },
        ]
        for ev in events:
            yield f"event: {ev['type']}\n"
            yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"
            await asyncio.sleep(0.3)

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/mock/last_request")
async def last_request():
    return {"headers": _last["headers"], "body": _last["body"]}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8100)
