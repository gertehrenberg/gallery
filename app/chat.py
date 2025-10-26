from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
import httpx, json, asyncio, uvicorn
from starlette.middleware.cors import CORSMiddleware

app = FastAPI()
from fastapi.staticfiles import StaticFiles
import os

# Verzeichnis für statische Dateien (z. B. chat.html)
static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")


# 🔹 CORS (Browser darf POST /chat ausführen)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "scene2flux_13b"

async def stream_ollama(prompt: str):
    async with httpx.AsyncClient(timeout=None) as client:
        async with client.stream("POST", OLLAMA_URL, json={
            "model": MODEL,
            "prompt": prompt,
            "stream": True
        }) as response:
            async for line in response.aiter_lines():
                if not line.strip():
                    continue
                try:
                    data = json.loads(line)
                    if "response" in data:
                        yield data["response"]
                    if data.get("done"):
                        break
                except json.JSONDecodeError:
                    continue


# 🔹 Test-Endpunkt
@app.get("/ping")
async def ping():
    return {"ok": True}

@app.post("/chat")
async def chat(request: Request):
    body = await request.json()
    prompt = body.get("prompt", "")
    return StreamingResponse(stream_ollama(prompt), media_type="text/plain")

@app.get("/")
async def root():
    return {"status": "ok", "message": "Ollama Chat API is running."}

if __name__ == "__main__":
    uvicorn.run("chat:app", host="0.0.0.0", port=8005, reload=True)
