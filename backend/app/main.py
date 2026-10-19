from fastapi import FastAPI

from app.api import jobs

app = FastAPI(title="TaskQueue", version="0.1.0")
app.include_router(jobs.router)


@app.get("/health", tags=["meta"])
def health() -> dict[str, str]:
    return {"status": "ok"}
