from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse

from api.routes import clients, runner, sqlgen

app = FastAPI(title="PIVOT Test File Review Dashboard", version="1.0.0")

# Allow the Vite dev server (port 5173) to talk to FastAPI during development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(clients.router, prefix="/api")
app.include_router(runner.router, prefix="/api")
app.include_router(sqlgen.router, prefix="/api")

# Serve the built React app — only mount if the dist directory exists
DIST_DIR = Path(__file__).resolve().parent.parent / "dashboard" / "dist"

if DIST_DIR.exists():
    # Serve static assets (JS, CSS, images)
    app.mount("/assets", StaticFiles(directory=str(DIST_DIR / "assets")), name="assets")

    @app.get("/", include_in_schema=False)
    @app.get("/{full_path:path}", include_in_schema=False)
    async def serve_spa(full_path: str = ""):
        """Catch-all: serve index.html for all non-API routes (React Router)."""
        # Don't intercept API routes
        if full_path.startswith("api/"):
            return {"detail": "Not Found"}
        index = DIST_DIR / "index.html"
        if index.exists():
            return HTMLResponse(
                content=index.read_text(encoding="utf-8"),
                headers={
                    "Cache-Control": "no-cache, no-store, must-revalidate",
                    "Pragma": "no-cache",
                    "Expires": "0",
                },
            )
        return {"detail": "Frontend not built. Run: cd dashboard && npm run build"}
else:
    @app.get("/", include_in_schema=False)
    async def no_frontend():
        return {
            "message": "API is running. Frontend not built yet.",
            "instructions": "cd dashboard && npm install && npm run build",
        }
