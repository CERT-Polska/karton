from pathlib import Path

from fastapi import FastAPI
from starlette.staticfiles import StaticFiles

from .api_routes import api_routes
from .routes import routes

package_dir = Path(__file__).parent


def mount_dashboard(app: FastAPI):
    app.mount("/static", StaticFiles(directory=package_dir / "static"), name="static")
    app.include_router(routes)
    app.include_router(api_routes)
