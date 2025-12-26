import os
import reqsnaked
import datetime
import logging
import logging.config

from pathlib import Path
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Dict, List, Any
from concurrent_log_handler import ConcurrentTimedRotatingFileHandler as _

from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.encoders import jsonable_encoder
from fastapi.staticfiles import StaticFiles
from anyio import to_thread

from server.fapi.tile import tiles_router
from server.fapi.ds import ds_router
from server.fapi.pyramid import pyramids_router

from server.fapi.utils import (
    load_environments_from_file,
    initialize_event_loop,
    load_config_app,
)
from server.datasources import load_datasources_from_db
from server.fapi.db import connect_to_db, close_db_connection

logging.config.fileConfig("log_app.ini", disable_existing_loggers=False)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    initialize_event_loop()
    app.state.env_vars = load_environments_from_file()
    to_thread.current_default_thread_limiter().total_tokens = int(
        app.state.env_vars.get("ANYIO_TOTAL_TOKENS")
    )

    # Init:
    # - database pool
    # - pgsql_version
    # - postgis_version
    await connect_to_db(app)

    app.state.global_dependencies: Dict[str, Any] = {}
    app.state.datasource_keys: Dict[str, List[str]] = {}
    app.state.invalid_keys: Dict[str, datetime.datetime] = {}
    app.state.datasources, app.state.datasources_from_db = (
        await load_datasources_from_db(app)
    )
    app.state.http_client = reqsnaked.Client(user_agent="ISONE", store_cookie=True)
    app.state.root_path = str(Path(__file__).parents[2])
    load_config_app(app)

    yield

    if hasattr(app.state, "db_pool"):
        await close_db_connection(app)


app = FastAPI(lifespan=lifespan, title="ISONE Tiler Server")

prefix = "/api"
app.include_router(pyramids_router, prefix=prefix)
app.include_router(tiles_router, prefix=prefix)
app.include_router(ds_router, prefix=prefix)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.exception_handler(status.HTTP_404_NOT_FOUND)
def not_found_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"message": "The resource you requested was not found."},
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    details = exc.errors()
    modified_details = []
    for error in details:
        modified_details.append(
            {
                "location": error["loc"],
                "message": error["msg"],
                "type": error["type"],
            }
        )
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder({"detail": modified_details}),
    )


@app.get(
    "/api/health",
    responses={200: {"content": {"application/json": {}}}},
    response_class=JSONResponse,
)
async def health():
    pid = os.getpid()
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "worker_pid": pid,
            "worker_type": "granian",
            "worker_status": "running",
        },
    )


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse("static/favicon.ico")
