from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()

# Route für "/" → wird erreicht durch: https://levellevel.me/gallery
@app.get("/")
async def gallery_root():
    return {"message": "Das ist die Galerie-Startseite!"}

# Optionale Test-Route
@app.get("/foo")
async def foo():
    return {"message": "Das ist /foo"}

# Catch-all für alles andere
@app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def catch_all(full_path: str, request: Request):
    return JSONResponse({
        "message": "Diese Route wurde nicht explizit definiert.",
        "path": full_path,
        "method": request.method
    })
