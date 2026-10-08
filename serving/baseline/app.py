"""Baseline LLM server: Hugging Face transformers + FastAPI, no inference engine.

This is the "just wrap model.generate() in an endpoint" service that the vLLM
deployment is benchmarked against. It is deliberately simple:

* one process, one copy of the model;
* requests are executed strictly one at a time (a single worker thread), because
  plain `model.generate()` has no continuous batching -- concurrent requests wait
  in a FIFO queue;
* it speaks the subset of the OpenAI `/v1/chat/completions` API the benchmark uses,
  including vLLM's `ignore_eos` extension, so the same load generator and the same
  request payloads hit both servers.

Sampling is greedy with no repetition penalty, matching vLLM started with
`--generation-config vllm` (which ignores the model's generation_config.json defaults).
"""

import asyncio
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import torch
from fastapi import FastAPI
from pydantic import BaseModel
from transformers import AutoModelForCausalLM, AutoTokenizer, GenerationConfig

MODEL_PATH = os.environ.get("MODEL_PATH", "/models/Qwen2.5-0.5B-Instruct")
SERVED_MODEL_NAME = os.environ.get("SERVED_MODEL_NAME", "qwen2.5-0.5b-instruct")
# Default to every core this container is allowed to run on (its cpuset).
NUM_THREADS = int(os.environ.get("NUM_THREADS", len(os.sched_getaffinity(0))))

torch.set_num_threads(NUM_THREADS)
tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
model = AutoModelForCausalLM.from_pretrained(MODEL_PATH, dtype=torch.bfloat16).eval()

# max_workers=1 is the point of the baseline: generate() calls never overlap.
# torch's intra-op thread count is per OS thread, so set it in the worker too.
executor = ThreadPoolExecutor(
    max_workers=1, initializer=torch.set_num_threads, initargs=(NUM_THREADS,)
)

app = FastAPI(title="kubeserve-baseline")


class Message(BaseModel):
    role: str
    content: str


class ChatCompletionRequest(BaseModel):
    model: str | None = None
    messages: list[Message]
    max_tokens: int = 128
    temperature: float = 0.0
    top_p: float = 1.0
    ignore_eos: bool = False  # vLLM extension: always generate exactly max_tokens


def _generate(req: ChatCompletionRequest) -> tuple[str, int, int]:
    inputs = tokenizer.apply_chat_template(
        [m.model_dump() for m in req.messages],
        add_generation_prompt=True,
        return_tensors="pt",
        return_dict=True,
    )
    do_sample = req.temperature > 0
    config = GenerationConfig(
        max_new_tokens=req.max_tokens,
        min_new_tokens=req.max_tokens if req.ignore_eos else 0,
        do_sample=do_sample,
        temperature=req.temperature if do_sample else None,
        top_p=req.top_p if do_sample else None,
        top_k=None,
        repetition_penalty=1.0,
        eos_token_id=model.generation_config.eos_token_id,
        pad_token_id=tokenizer.pad_token_id,
    )
    with torch.inference_mode():
        output = model.generate(**inputs, generation_config=config)
    prompt_len = inputs["input_ids"].shape[1]
    new_tokens = output[0, prompt_len:]
    text = tokenizer.decode(new_tokens, skip_special_tokens=True)
    return text, prompt_len, int(new_tokens.shape[0])


@app.post("/v1/chat/completions")
async def chat_completions(req: ChatCompletionRequest) -> dict:
    loop = asyncio.get_running_loop()
    text, prompt_tokens, completion_tokens = await loop.run_in_executor(
        executor, _generate, req
    )
    finish_reason = "length" if completion_tokens >= req.max_tokens else "stop"
    return {
        "id": f"chatcmpl-{uuid.uuid4().hex}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": SERVED_MODEL_NAME,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": finish_reason,
            }
        ],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        },
    }


@app.get("/v1/models")
async def models() -> dict:
    return {
        "object": "list",
        "data": [{"id": SERVED_MODEL_NAME, "object": "model", "owned_by": "kubeserve"}],
    }


@app.get("/health")
async def health() -> dict:
    # The model is loaded at import time, so if the server is up it can serve.
    return {"status": "ok"}
