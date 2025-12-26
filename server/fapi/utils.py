import os
import sys
import asyncio

from typing import Dict, Optional, Any
from dotenv import dotenv_values
from pathlib import Path
from bestconfig import Config

from fastapi import Request, HTTPException, status, FastAPI

from server.fapi.ds import load_datasource_from_db, save_datasource_from_db_to_app_state
from server.datasources import DataSource

MAPTILER_ERROR = "Out of bounds"


def initialize_event_loop():
    if sys.platform.startswith("win32") or sys.platform.startswith("linux-cross"):
        import winloop  # type: ignore[import-not-found]

        winloop.install()
        loop = winloop.new_event_loop()
        asyncio.set_event_loop(loop)
    else:
        # uv loop doesn't support windows or arm machines at the moment
        # but uv loop is much faster than native asyncio
        import uvloop  # type: ignore[import-not-found]

        uvloop.install()
        loop = uvloop.new_event_loop()
        asyncio.set_event_loop(loop)


# Load environments variables from .env file (from root folder)
def load_environments_from_file() -> Dict[str, str]:
    root_path: str = str(Path(__file__).parents[2])
    dotenv_path = os.path.join(root_path, ".env")
    config: Dict[str, str] = dotenv_values(dotenv_path=dotenv_path)
    return config


async def try_load_datasource_from_db(
    request: Request, datasource_id: str
) -> DataSource:
    # Try load DataSource from DB
    ds_from_db: Optional[Dict[str, Any]] = await load_datasource_from_db(
        request.app.state.db_pool, datasource_id
    )
    if ds_from_db is not None:
        ds = save_datasource_from_db_to_app_state(request, ds_from_db)
        return ds
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Datasource ID '{datasource_id}' not found",
        )


def load_config_app(app: FastAPI):
    config_path: str = os.path.join(app.state.root_path, "config_app.json")
    app.state.config = Config(config_path)
