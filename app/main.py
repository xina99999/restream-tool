from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.controllers.api import router
from app.services.restream_service import APP_ROOT, lifespan


app = FastAPI(title="RTSP Restream Tool", version="1.0.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=APP_ROOT / "static"), name="static")
app.include_router(router)
