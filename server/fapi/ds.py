import os
import orjson
import multiprocessing
import logging
import shutil

from uuid import uuid4
from typing import Dict, List, Any, Optional, Union, Literal, Tuple, Annotated
from urllib.parse import unquote

from fastapi import APIRouter, Request, Body, HTTPException, status
from fastapi.responses import JSONResponse, Response
from asyncpg.pool import Pool
from asyncpg import Connection, UniqueViolationError
from pydantic_core import ErrorDetails
from pydantic import (
    BaseModel,
    RootModel,
    validator,
    model_validator,
    BeforeValidator,
    HttpUrl,
    TypeAdapter,
    ValidationError,
)


from raster_tiles.defaults import PIXEL_SELECTION_METHOD
from server.datasources import (
    load_datasources_from_db,
    BUFFER,
    EXTENT,
    STORE_TYPE,
    LAYER_TYPE,
    VECTOR,
    RASTER,
    EXTENSIONS,
    RESAMPLING,
    MINZOOM,
    MAXZOOM,
    EncodingType,
    StoreType,
    DataType,
    DataSourceRaster,
    DataSourceVector,
    DataStoreVectorInternal,
    DataStoreVectorTiles,
    DataSource,
)

from vector_tiles.jfe.parser import parse_jfe
from vector_tiles.jfe.evaluate import to_sql_where
from vector_tiles.jfe.ast import Node

logger = logging.getLogger(__name__)

http_url_adapter = TypeAdapter(HttpUrl)
Url = Annotated[
    str, BeforeValidator(lambda value: str(http_url_adapter.validate_python(value)))
]

ds_router = APIRouter()

QUERY_INSERT_DATASOURCE: str = """
    INSERT INTO datasource(
        identifier,
        data_type,
        store_type,
        host,
        port,
        mbtiles,
        description,
        attribution,
        minzoom,
        maxzoom,
        bounds,
        center,
        data
    )
    VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
"""

QUERY_UPDATE_DATASOURCE: str = """
    UPDATE datasource
    SET        
        data_type=$2,
        store_type=$3,
        host=$4,
        port=$5,
        mbtiles=$6,
        description=$7,
        attribution=$8,
        minzoom=$9,
        maxzoom=$10,
        bounds=$11,
        center=$12,
        data=$13
    WHERE
        identifier=$1
"""


def query_exists_table(table: str, schema: str = "public") -> str:
    query: str = (
        f"\
        SELECT EXISTS (\
            SELECT FROM information_schema.tables \
            WHERE  table_schema = '{schema}'\
            AND    table_name   = '{table}'\
        );"
    )
    return query


def query_exists_column(table: str, column: str) -> str:
    query: str = (
        f"\
        SELECT EXISTS (\
            SELECT column_name \
            FROM information_schema.columns \
            WHERE table_name='{table}' and column_name='{column}'\
        );"
    )
    return query


def save_datasource_from_db_to_app_state(
    request: Request, ds_from_db: Dict[str, Any]
) -> DataSource:
    identifier: str = ds_from_db["id"]
    if ds_from_db["type"] == DataType.vector:
        try:
            ds_vector = DataSourceVector(ds_from_db)
        except Exception as e:
            message: str = (
                f"'save_datasource_from_db_to_app_state': error load Vector DataSource '{identifier}' from DB: {str(e)}"
            )
            logger.error(message)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message
            )

        if isinstance(ds_vector.data_store, DataStoreVectorInternal) and hasattr(
            request.app.state, "postgis_version"
        ):
            # Check version PostGIS/GEOS
            if (request.app.state.postgis_version[0] < 3) or (
                request.app.state.postgis_version[0] == 3
                and request.app.state.postgis_version[1] < 1
            ):
                ds_vector.margin = ""
        if isinstance(ds_vector.data_store, DataStoreVectorTiles):
            request.app.state.datasource_keys[identifier] = ds_vector.data_store.keys

        request.app.state.datasources[identifier] = ds_vector
    elif ds_from_db["type"] == DataType.raster:
        try:
            ds_raster = DataSourceRaster(ds_from_db)
        except Exception as e:
            message: str = (
                f"Error load Raster DataSource '{identifier}' from DB: {str(e)}"
            )
            logger.error(message)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message
            )
        request.app.state.datasources[identifier] = ds_raster
    else:
        message: str = f'Unsupported type of DataSource: {ds_from_db["type"]}'
        logger.error(message)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message
        )

    request.app.state.datasources_from_db[identifier] = ds_from_db
    return request.app.state.datasources[identifier]


async def load_datasource_from_db(
    db_pool: Pool,
    datasource_id: str,
) -> Optional[Dict[str, Any]]:
    try:
        connection: Connection
        async with db_pool.acquire() as connection:
            datasource = await connection.fetchrow(
                f"SELECT data FROM datasource WHERE identifier='{datasource_id}'"
            )
    except Exception as e:
        message: str = (
            f"'load_datasource_from_db' error load DataSource '{datasource_id}' from DB: {str(e)}"
        )
        logger.error(message)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message
        )

    if datasource is not None:
        return dict(datasource["data"])

    return None


@ds_router.get(
    "/datasources/{datasource_id}",
    responses={200: {"content": {"application/json": {}}}},
    response_class=JSONResponse,
)
async def get_datasource(
    request: Request,
    datasource_id: str,
) -> JSONResponse:
    db_pool: Pool = request.app.state.db_pool
    datasource: Optional[Dict[str, Any]] = await load_datasource_from_db(
        db_pool, datasource_id
    )

    if datasource is None:
        content: Dict[str, Any] = {
            "message": f"DataSource id '{datasource_id}' not found in DB"
        }
        return JSONResponse(content=content)

    return JSONResponse(content=datasource)


@ds_router.get(
    "/datasources",
    responses={200: {"content": {"application/json": {}}}},
    response_class=JSONResponse,
)
async def get_datasources(
    request: Request,
) -> JSONResponse:
    try:
        request.app.state.datasources, request.app.state.datasources_from_db = (
            await load_datasources_from_db(request.app)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )

    ds_list: List[Dict[str, Any]] = [
        dict(datasource["data"])
        for datasource in request.app.state.datasources_from_db.values()
    ]
    return JSONResponse(content=ds_list)


# ====================================================================
# ============================== Common ==============================
# ====================================================================


class Center(RootModel):
    root: List[Union[float, int]]

    @validator("root")
    def validate_root(cls, value: List[Union[float, int]]):
        if len(value) < 2 or len(value) > 3:
            raise ValueError(
                f"In the 'center' field the number of elements should be 2 or 3. Got {value}"
            )
        if value[0] > 180 or value[0] < -180:
            raise ValueError(
                f"Center.lng must must be in range [-180, 180] degrees. Got {value[0]}"
            )
        if value[1] > 90 or value[1] < -90:
            raise ValueError(
                f"Center.lat must must be in range [-90, 90] degrees. Got {value[1]}"
            )
        if len(value) == 3:
            if value[2] > MAXZOOM or value[2] < MINZOOM:
                raise ValueError(
                    f"Center.zoom must be in range [0-20], found {value[2]}"
                )
            value[2] = int(value[2])

        return value


class Bounds(BaseModel):
    lng_w: float
    lat_s: float
    lng_e: float
    lat_n: float

    @model_validator(mode="after")
    def validate_fields(self):
        if self.lng_w > 180 or self.lng_w < -180:
            raise ValueError(
                f"Bounds.lng_w must must be in range [-180, 180] degrees. Got {self.lng_w}"
            )
        if self.lng_e > 180 or self.lng_e < -180:
            raise ValueError(
                f"Bounds.lng_e must must be in range [-180, 180] degrees. Got {self.lng_e}"
            )
        if self.lat_s > 90 or self.lat_s < -90:
            raise ValueError(
                f"Bounds.lat_s must must be in range [-90, 90] degrees. Got {self.lat_s}"
            )
        if self.lat_n > 90 or self.lat_n < -90:
            raise ValueError(
                f"Bounds.lat_n must must be in range [-90, 90] degrees. Got {self.lat_n}"
            )
        return self


# ====================================================================
# ============================== Raster ==============================
# ====================================================================


class DataStoreRasterBase(BaseModel):
    type: str
    store: str
    host: Optional[str] = None
    port: Optional[int] = None
    dataset: Optional[str] = None
    file: Optional[str] = None
    folder: Optional[str] = None

    @validator("type")
    def validate_type(cls, value):
        if value != RASTER:
            raise ValueError(
                f"DataStoreRasterBase.type must have one of the values ['{RASTER}']"
            )
        return value

    @validator("store")
    def validate_store(cls, value):
        if value not in STORE_TYPE:
            raise ValueError(
                f"DataStoreRasterBase.store must have one of the values {STORE_TYPE}"
            )
        return value

    @validator("file")
    def validate_file(cls, value):
        if value is not None:
            filename, ext = os.path.splitext(value)
            if ext not in EXTENSIONS:
                raise ValueError(
                    f"DataStoreRasterBase.file must have one of the values {EXTENSIONS}"
                )
        return value

    @model_validator(mode="after")
    def validate_fields(self):
        if self.file is not None and self.folder is not None:
            raise ValueError(
                f"DataStoreRasterBase.[file, folder] the 'file' and 'folder' fields cannot be specified at the same time"
            )
        return self

    def model_post_init(self, __context):
        if self.dataset is None:
            if self.file is not None:
                self.dataset, _ = os.path.splitext(self.file)
            if self.folder is not None:
                self.dataset = self.folder


class PyramidSettings(BaseModel):
    verbose: Optional[bool] = False
    resampling: Optional[str] = "average"
    tiledriver: Optional[str] = "PNG"
    tile_size: Optional[int] = 256
    xyz: Optional[bool] = True
    count_processes: Optional[int] = multiprocessing.cpu_count()
    minzoom: int
    maxzoom: int
    mbtiles: Optional[bool] = True
    warnings: Optional[bool] = False
    save_tile_detail_db: Optional[bool] = True
    warp: Optional[bool] = False
    resampling_warp: Optional[str] = "average"
    remove_processing_raster_files: Optional[bool] = False
    encode_to_rgba: Optional[bool] = True
    mosaic_merge: Optional[bool] = False
    nodata_default: Optional[float] = -999999
    pixel_selection_method: Optional[str] = "FirstMethod"
    merge: Optional[bool] = True

    @validator("resampling")
    def validate_resampling(cls, value):
        if value not in RESAMPLING:
            raise ValueError(
                f"PyramidSettings.resampling must have one of the values {RESAMPLING}"
            )
        return value

    @validator("resampling_warp")
    def validate_resampling_warp(cls, value):
        if value not in RESAMPLING:
            raise ValueError(
                f"PyramidSettings.resampling must have one of the values {RESAMPLING}"
            )
        return value

    @validator("tiledriver")
    def validate_tiledriver(cls, value):
        if value not in ["PNG"]:
            raise ValueError(
                f"PyramidSettings.tiledriver must have one of the values ['PNG']"
            )
        return value

    @validator("tile_size")
    def validate_tile_size(cls, value):
        if value > 512 or value < 128:
            raise ValueError(
                f"PyramidSettings.tile_size must have one of the values [128, 256, 512]"
            )
        return value

    @validator("count_processes")
    def validate_count_processes(cls, value):
        cpus = multiprocessing.cpu_count()
        if value > cpus or value < 1:
            raise ValueError(
                f"PyramidSettings.count_processes must be in range 1...{cpus}, got '{value}'"
            )
        return value

    @validator("minzoom")
    def validate_minzoom(cls, value):
        if not isinstance(value, int):
            raise ValueError(
                f"PyramidSettings.minzoom: must be integer. Got: '{type(value)}'"
            )
        if value < MINZOOM or value > MAXZOOM:
            raise ValueError(
                f"PyramidSettings.minzoom: value must be in range 0...20, got '{value}'"
            )
        return value

    @validator("maxzoom")
    def validate_maxzoom(cls, value):
        if not isinstance(value, int):
            raise ValueError(
                f"PyramidSettings.maxzoom: must be integer. Got: '{type(value)}'"
            )
        if value < MINZOOM or value > MAXZOOM:
            raise ValueError(
                f"PyramidSettings.maxzoom: value must be in range 0...20, got '{value}'"
            )
        return value

    @validator("pixel_selection_method")
    def validate_pixel_selection_method(cls, value):
        if value not in PIXEL_SELECTION_METHOD:
            raise ValueError(
                f"PyramidSettings.pixel_selection_method must have one of the values ['FirstMethod', 'HighestMethod', 'LowestMethod', 'MeanMethod']"
            )
        return value


class DataSourceRasterBase(BaseModel):
    type: Literal[RASTER]  # type: ignore
    id: Optional[str] = None
    mosaics: Optional[bool] = False
    dataStore: DataStoreRasterBase
    pyramidSettings: PyramidSettings
    attribution: Optional[str] = None
    description: Optional[str] = None
    version: Optional[str] = None
    minzoom: int
    maxzoom: int
    mbtiles: Optional[bool] = True
    center: Optional[Center] = None
    bounds: Optional[Bounds] = None
    encoding: Optional[str] = EncodingType.f32
    use_cache_only: Optional[bool] = False
    compress_tiles: Optional[bool] = False

    @validator("dataStore")
    def validate_type(cls, value: DataStoreRasterBase, values):
        if values["mosaics"]:
            if value.folder is None:
                raise ValueError(
                    f"DataSourceRasterBase.dataStore.folder for Mosaic shouldn't be None"
                )
        else:
            if value.file is None:
                raise ValueError(
                    f"DataSourceRasterBase.dataStore.file for single Raster shouldn't be None"
                )
        return value

    @model_validator(mode="after")
    def validate_fields(self):
        if (
            self.minzoom < MINZOOM
            or self.minzoom > MAXZOOM
            or self.minzoom > self.maxzoom
        ):
            raise ValueError(
                f"DataSourceRasterBase.minzoom must be in range [0-20] and less than maxzoom. Got {self.minzoom}"
            )
        if (
            self.maxzoom < MINZOOM
            or self.maxzoom > MAXZOOM
            or self.maxzoom < self.minzoom
        ):
            raise ValueError(
                f"DataSourceRasterBase.maxzoom must be in range [0-20] and more than minzoom. Got {self.maxzoom}"
            )
        if self.bounds is not None and self.center is not None:
            center_lng: float = self.center.root[0]
            center_lat: float = self.center.root[1]
            if center_lng > self.bounds.lng_e or center_lng < self.bounds.lng_w:
                raise ValueError(
                    f"Longitude of center '{center_lng}' is out of bounds {self.bounds.lng_w}..{self.bounds.lng_e} degrees"
                )
            if center_lat > self.bounds.lat_n or center_lat < self.bounds.lat_s:
                raise ValueError(
                    f"Latitude of center '{center_lat}' is out of bounds {self.bounds.lat_s}..{self.bounds.lat_n} degrees"
                )

        if self.center is not None and len(self.center.root) == 3:
            center_zoom: int = self.center.root[2]
            if center_zoom > self.maxzoom or center_zoom < self.minzoom:
                raise ValueError(
                    f"Zoom of center '{center_zoom}' is out of bounds {self.minzoom}..{self.maxzoom}"
                )

        if self.pyramidSettings.minzoom < self.minzoom:
            raise ValueError(
                f"'minzoom' of pyramid must be more or equal than DataSourceRasterBase.minzoom. Got {self.pyramidSettings.minzoom}"
            )
        if self.pyramidSettings.maxzoom > self.maxzoom:
            raise ValueError(
                f"'maxzoom' of pyramid must be less or equal than DataSourceRasterBase.maxzoom. Got {self.pyramidSettings.maxzoom}"
            )

        if self.encoding is not None:
            EncodingType(self.encoding)

        return self

    def model_post_init(self, __context):
        if self.id is None:
            self.id = str(uuid4())


# ====================================================================
# ============================== Vector ==============================
# ====================================================================


class DataStoreVectorBase(BaseModel):
    type: str
    store: str
    tiles: Optional[List[Url]] = None
    keys: Optional[List[str]] = None

    @validator("type")
    def validate_type(cls, value):
        if value != VECTOR:
            raise ValueError(
                f"DataStoreVectorBase.type must have one of the values ['{VECTOR}']"
            )
        return value

    @validator("store")
    def validate_store(cls, value):
        if value not in STORE_TYPE:
            raise ValueError(
                f"DataStoreVectorBase.store must have one of the values {STORE_TYPE}"
            )
        return value

    @model_validator(mode="after")
    def validate_fields(self):
        if self.store == StoreType.tiles and self.tiles is None:
            raise ValueError(
                f"DataStoreVectorBase.tiles must be defined for store type 'tiles'"
            )
        return self


class LayerQuerySQL(BaseModel):
    minzoom: int
    maxzoom: int
    sql: str

    @model_validator(mode="after")
    def validate_fields(self):
        if (
            self.minzoom < MINZOOM
            or self.minzoom > MAXZOOM
            or self.minzoom > self.maxzoom
        ):
            raise ValueError(
                f"LayerQuerySQL.minzoom must be in range [{MINZOOM}-{MAXZOOM}] and less than maxzoom. Got {self.minzoom}"
            )
        if (
            self.maxzoom < MINZOOM
            or self.maxzoom > MAXZOOM
            or self.maxzoom < self.minzoom
        ):
            raise ValueError(
                f"LayerQuerySQL.maxzoom must be in range [{MINZOOM}-{MAXZOOM}] and more than minzoom. Got {self.maxzoom}"
            )


class Field(BaseModel):
    name: str
    name_in_db: Optional[str] = None
    encode: Optional[bool] = False
    description: Optional[str] = None

    def model_post_init(self, __context):
        if self.name_in_db is None:
            self.name_in_db = self.name


class VectorLayer(BaseModel):
    id: str
    type: str
    storeLayer: Optional[str] = None
    geomField: Optional[str] = None
    description: Optional[str] = None
    minzoom: int
    maxzoom: int
    simplify: Optional[bool] = False
    filter: Optional[List[Any]] = None
    fields: Optional[List[Field]] = None
    queries: Optional[List[LayerQuerySQL]] = None

    @validator("type")
    def validate_type(cls, value):
        if value not in LAYER_TYPE:
            raise ValueError(
                f"VectorLayer.type must have one of the values {LAYER_TYPE}"
            )
        return value

    @model_validator(mode="after")
    def validate_fields(self):
        if self.filter is not None and self.queries is not None:
            raise ValueError(
                f"VectorLayer.[filter, queries] cannot be defined simultaneously"
            )
        if (
            self.minzoom < MINZOOM
            or self.minzoom > MAXZOOM
            or self.minzoom > self.maxzoom
        ):
            raise ValueError(
                f"VectorLayer.minzoom must be in range [{MINZOOM}-{MAXZOOM}] and less than maxzoom. Got {self.minzoom}"
            )
        if (
            self.maxzoom < MINZOOM
            or self.maxzoom > MAXZOOM
            or self.maxzoom < self.minzoom
        ):
            raise ValueError(
                f"VectorLayer.maxzoom must be in range [{MINZOOM}-{MAXZOOM}] and more than minzoom. Got {self.maxzoom}"
            )

        # validate filter and fields
        if self.fields is not None and self.filter is not None:
            field_mapping: Dict[str, str] = {
                field.name: field.name_in_db for field in self.fields
            }
            try:
                parsed_jfe: Node = parse_jfe(self.filter, self.geomField)
                where_clause: str = to_sql_where(parsed_jfe, field_mapping)
            except Exception as e:
                raise ValueError(
                    f"VectorLayer.[filter, fields] must be synchronized according to the list of fields used: {str(e)}"
                )
        return self


class DataSourceVectorBase(BaseModel):
    type: Literal[VECTOR]  # type: ignore
    id: Optional[str] = None
    dataStore: DataStoreVectorBase
    attribution: Optional[str] = None
    description: Optional[str] = None
    version: Optional[str] = None
    buffer: Optional[int] = BUFFER
    extent: Optional[int] = EXTENT
    minzoom: int
    maxzoom: int
    mbtiles: Optional[bool] = True
    pyramidSettings: Optional[PyramidSettings] = None
    center: Optional[Center] = None
    bounds: Optional[Bounds] = None
    layers: Optional[List[VectorLayer]] = None
    use_cache_only: Optional[bool] = False
    compress_tiles: Optional[bool] = False

    @validator("type")
    def validate_type(cls, value):
        if value != VECTOR:
            raise ValueError(
                f"DataSourceVectorBase.type must have one of the values ['{VECTOR}']"
            )
        return value

    @model_validator(mode="after")
    def validate_fields(self):
        if (
            self.minzoom < MINZOOM
            or self.minzoom > MAXZOOM
            or self.minzoom > self.maxzoom
        ):
            raise ValueError(
                f"DataSourceVectorBase.minzoom must be in range [0-20] and less than maxzoom. Got {self.minzoom}"
            )

        if (
            self.maxzoom < MINZOOM
            or self.maxzoom > MAXZOOM
            or self.maxzoom < self.minzoom
        ):
            raise ValueError(
                f"DataSourceVectorBase.maxzoom must be in range [0-20] and more than minzoom. Got {self.maxzoom}"
            )
        if self.dataStore.store == StoreType.internal and self.layers is None:
            raise ValueError(
                f"DataSourceVectorBase.layers must be defined for store type 'internal'"
            )
        if self.bounds is not None and self.center is not None:
            center_lng: float = self.center.root[0]
            center_lat: float = self.center.root[1]
            if center_lng > self.bounds.lng_e or center_lng < self.bounds.lng_w:
                raise ValueError(
                    f"Longitude of center '{center_lng}' is out of bounds {self.bounds.lng_w}..{self.bounds.lng_e} degrees"
                )
            if center_lat > self.bounds.lat_n or center_lat < self.bounds.lat_s:
                raise ValueError(
                    f"Latitude of center '{center_lat}' is out of bounds {self.bounds.lat_s}..{self.bounds.lat_n} degrees"
                )
        # Pyramid of vector tiles only for internal store
        if (
            self.dataStore.store != StoreType.internal
            and self.pyramidSettings is not None
        ):
            raise ValueError(
                f"Creating a pyramid of vector tiles is only possible for the internal type store. Got '{self.dataStore.store}'"
            )

        if self.center is not None and len(self.center.root) == 3:
            center_zoom: int = self.center.root[2]
            if center_zoom > self.maxzoom or center_zoom < self.minzoom:
                raise ValueError(
                    f"Zoom of center '{center_zoom}' is out of bounds {self.minzoom}..{self.maxzoom}"
                )

        if self.pyramidSettings is not None:
            if self.pyramidSettings.minzoom < self.minzoom:
                raise ValueError(
                    f"'minzoom' of pyramid must be more or equal than DataSourceVectorBase.minzoom. Got {self.pyramidSettings.minzoom}"
                )
            if self.pyramidSettings.maxzoom > self.maxzoom:
                raise ValueError(
                    f"'maxzoom' of pyramid must be less or equal than DataSourceVectorBase.maxzoom. Got {self.pyramidSettings.maxzoom}"
                )

        return self

    def model_post_init(self, __context):
        if self.id is None:
            self.id = str(uuid4())


async def save_datasource_to_db(
    request: Request,
    ds: Dict[str, Any],
) -> Response:

    identifier = ds.get("id")
    data_type: str = ds.get("type")
    data_store: Dict[str, Any] = ds.get("dataStore")
    store_type: str = data_store.get("store")
    host: str = data_store.get("host")
    port: int = data_store.get("port")
    description: Optional[str] = ds.get("description")
    attribution: Optional[str] = ds.get("attribution")
    minzoom: int = ds.get("minzoom") or MINZOOM
    maxzoom: int = ds.get("maxzoom") or MAXZOOM
    bounds: Optional[Dict[str, float]] = ds.get("bounds")
    center: Optional[List[float]] = ds.get("center")
    mbtiles: bool = ds.get("mbtiles")

    pyr_settings: Dict[str, Any] = ds.get("pyramidSettings")
    if pyr_settings is not None and data_type == DataType.vector:
        ps_vector: Dict[str, Any] = {
            key: pyr_settings[key]
            for key in pyr_settings
            if key in ["minzoom", "maxzoom", "count_processes"]
        }
        ds["pyramidSettings"] = ps_vector

    params = (
        identifier,
        data_type,
        store_type,
        host,
        port,
        mbtiles,
        description,
        attribution,
        minzoom,
        maxzoom,
        bounds,
        center,
        ds,
    )

    method: str = request.method
    update: bool = False
    if method == "POST":
        query: str = QUERY_INSERT_DATASOURCE
        action: str = "created"
    elif method == "PUT" or method == "PATCH":
        query = QUERY_UPDATE_DATASOURCE
        action = "updated"
        update = True
    else:
        HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"'save_datasource_to_db' error unsupported request method '{method}'",
        )

    try:
        db_pool: Pool = request.app.state.db_pool
        connection: Connection
        async with db_pool.acquire() as connection:
            try:
                if update:
                    record = await connection.fetchval(
                        f"SELECT EXISTS(SELECT 1 FROM datasource WHERE identifier='{identifier}')"
                    )
                    if record:
                        await connection.execute(
                            query,
                            *params,
                        )
                    else:
                        return Response(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            content=orjson.dumps(
                                {
                                    "message": f"'save_datasource_to_db' error update DataSource: '{identifier}' not exists"
                                }
                            ),
                        )
                else:
                    await connection.execute(
                        query,
                        *params,
                    )
            except UniqueViolationError:
                new_id: str = str(uuid4())
                ds["id"] = new_id
                new_params = (new_id,) + params[1:]
                await connection.execute(
                    query,
                    *new_params,
                )

    except Exception as e:
        ds_id: str = ds.get("id")
        message: str = (
            f"'save_datasource_to_db' error insert DataSource '{ds_id}' in DB: {str(e)}"
        )
        logger.error(message)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message
        )

    return Response(
        content=orjson.dumps(
            {
                "datasource_id": ds.get("id"),
                "message": f"DataSource successfully {action}",
            }
        ),
        status_code=status.HTTP_200_OK,
        media_type="application/json",
    )


async def validate_vector_layers(
    datasource: DataSourceVectorBase, connection: Connection
) -> Optional[List[ErrorDetails]]:
    validation_errors: Optional[List[ErrorDetails]] = None
    layers: Optional[List[VectorLayer]] = datasource.layers

    if layers is not None:
        for layer in layers:
            # skip validation if SQL used
            if layer.queries is not None:
                continue

            table: str = layer.storeLayer
            is_table_exists: bool = await connection.fetchval(query_exists_table(table))
            if not is_table_exists:
                if validation_errors is None:
                    validation_errors = []
                validation_errors.append(
                    ErrorDetails(
                        type="missing",
                        loc=("layer", "id", f"{layer.id}"),
                        msg=f"Table '{table}' not found",
                        input=f"{table}",
                        ctx={"datasource_id": f"{datasource.id}"},
                    )
                )
                # if table missing then skip validation fields
                continue

            # validate geomFiled of layer
            is_geo_column_exists: bool = await connection.fetchval(
                query_exists_column(table, layer.geomField)
            )
            if not is_geo_column_exists:
                if validation_errors is None:
                    validation_errors = []
                validation_errors.append(
                    ErrorDetails(
                        type="missing",
                        loc=(
                            "layer",
                            "id",
                            f"{layer.id}",
                            "geomField",
                            f"{layer.geomField}",
                        ),
                        msg=f"Field '{layer.geomField}' not found in '{table}'",
                        input=f"{layer.geomField}",
                        ctx={"datasource_id": f"{datasource.id}"},
                    )
                )

            # validate fields of layer
            fields: Optional[List[Field]] = layer.fields
            if fields is not None:
                for field in fields:
                    column: str = field.name_in_db
                    is_column_exists: bool = await connection.fetchval(
                        query_exists_column(table, column)
                    )
                    if not is_column_exists:
                        if validation_errors is None:
                            validation_errors = []
                        validation_errors.append(
                            ErrorDetails(
                                type="missing",
                                loc=(
                                    "layer",
                                    "id",
                                    f"{layer.id}",
                                    "field",
                                    f"{column}",
                                ),
                                msg=f"Field '{column}' not found in '{table}'",
                                input=f"{column}",
                                ctx={"datasource_id": f"{datasource.id}"},
                            )
                        )

    return validation_errors


@ds_router.put(
    "/datasources",
    responses={200: {"content": {"application/json": {}}}},
    response_class=Response,
)
@ds_router.post(
    "/datasources",
    responses={200: {"content": {"application/json": {}}}},
    response_class=Response,
)
async def save_datasource(
    request: Request,
    datasource: Union[DataSourceVectorBase, DataSourceRasterBase] = Body(
        ..., discriminator="type"
    ),
) -> Response:
    if datasource.dataStore.store == StoreType.tiles:
        for i in range(0, len(datasource.dataStore.tiles)):
            datasource.dataStore.tiles[i] = unquote(datasource.dataStore.tiles[i])

    if isinstance(datasource, DataSourceVectorBase):
        try:
            db_pool: Pool = request.app.state.db_pool
            connection: Connection
            async with db_pool.acquire() as connection:
                validation_errors = await validate_vector_layers(datasource, connection)
        except Exception as e:
            message: str = f"'save_datasource' error validate vector layers: {str(e)}"
            logger.error(message)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message
            )

        if validation_errors is not None:
            content: bytes = orjson.dumps(validation_errors)
            return Response(content=content, status_code=status.HTTP_400_BAD_REQUEST)

    ds: Dict[str, Any] = datasource.model_dump(exclude_none=True)
    response = await save_datasource_to_db(request, ds)

    # Reload DataSources from DB
    try:
        request.app.state.datasources, request.app.state.datasources_from_db = (
            await load_datasources_from_db(request.app)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )

    return response


async def upload_datasources_to_db(
    ds_path: str, connection: Connection, datasource_ids: Optional[List[str]] = None
) -> Tuple[int, List[ErrorDetails]]:
    validation_errors: List[ErrorDetails] = []
    count: int = 0

    for ds_file in os.listdir(ds_path):
        file = os.path.join(ds_path, ds_file)
        ds: Optional[Dict[str, Any]] = None
        with open(file, "rb") as f:
            ds = orjson.loads(f.read())

        if ds is not None:
            identifier = ds.get("id")
            if not isinstance(identifier, str):
                validation_errors.append(
                    ErrorDetails(
                        type="type",
                        loc=("id",),
                        msg=f"Invalid type of 'id', must be string",
                        input=f"{identifier}",
                        ctx={"datasource_id": f"{identifier}"},
                    )
                )
                continue

            if datasource_ids is not None:
                if identifier in datasource_ids:
                    del_query: str = (
                        f"DELETE FROM datasource WHERE identifier='{identifier}'"
                    )
                    try:
                        await connection.execute(del_query)
                    except Exception as e:
                        message: str = (
                            f"'reupload_datasources_to_db': error delete DataSource '{identifier}' {str(e)}"
                        )
                        logger.error(message)
                        validation_errors.append(
                            ErrorDetails(
                                type="Database",
                                loc=("delete_datasources",),
                                input=f"{identifier}",
                                msg=f"Error delete DataSource in DB: {e}",
                                ctx={"datasource_id": f"{identifier}"},
                            )
                        )
                        continue

            record = await connection.fetchrow(
                f"SELECT data FROM datasource WHERE identifier='{identifier}'"
            )
            if record is None:
                data_type: str = ds.get("type")

                try:
                    # Validation DataSources
                    if data_type == DataType.raster:
                        pds: DataSourceRasterBase = DataSourceRasterBase.model_validate(
                            ds
                        )
                    elif data_type == DataType.vector:
                        pds: DataSourceVectorBase = DataSourceVectorBase.model_validate(
                            ds
                        )
                        # Validate vector layers
                        validation_vl_errors = await validate_vector_layers(
                            pds, connection
                        )
                        if validation_vl_errors is not None:
                            validation_errors.extend(validation_vl_errors)
                            continue
                    else:
                        validation_errors.append(
                            ErrorDetails(
                                type="invalid",
                                loc=("type",),
                                msg=f"Invalid type of DataSource, must be 'raster' or 'vector'",
                                input=f"{data_type}",
                                ctx={"datasource_id": f"{identifier}"},
                            )
                        )
                        continue

                # ValidationError - General Exception describing all validation errors
                except ValidationError as e:
                    errors: List[ErrorDetails] = [
                        ErrorDetails(
                            type=err.get("type"),
                            loc=err.get("loc"),
                            msg=err.get("msg"),
                            input=err.get("input"),
                            ctx={"datasource_id": f"{identifier}"},
                        )
                        for err in e.errors()
                    ]
                    validation_errors.extend(errors)
                    continue
                except Exception as e:
                    logger.error(f"Error validate DataSource {identifier}: {e}")
                    validation_errors.append(
                        ErrorDetails(
                            type="internal",
                            loc=("model_validate",),
                            msg=f"Error validate DataSource '{identifier}': {e}",
                            input=f"{identifier}",
                            ctx={"datasource_id": f"{identifier}"},
                        )
                    )
                    continue

                data_store_dict: Dict[str, Any] = ds.get("dataStore")
                if isinstance(pds, DataSourceRasterBase):
                    data_store_dict["dataset"] = pds.dataStore.dataset
                    ds["dataStore"] = data_store_dict

                store_type: str = data_store_dict.get("store")
                host: str = data_store_dict.get("host")
                port: int = data_store_dict.get("port")
                description: Optional[str] = ds.get("description")
                attribution: Optional[str] = ds.get("attribution")
                minzoom: int = ds.get("minzoom") or MINZOOM
                maxzoom: int = ds.get("maxzoom") or MAXZOOM
                bounds: Dict[str, float] = ds.get("bounds")
                center: List[float] = ds.get("center")
                mbtiles: bool = ds.get("mbtiles") or False

                params = (
                    identifier,
                    data_type,
                    store_type,
                    host,
                    port,
                    mbtiles,
                    description,
                    attribution,
                    minzoom,
                    maxzoom,
                    bounds,
                    center,
                    ds,
                )

                try:
                    await connection.execute(
                        QUERY_INSERT_DATASOURCE,
                        *params,
                    )
                    count += 1
                except Exception as e:
                    validation_errors.append(
                        ErrorDetails(
                            type="Database",
                            loc=("insert_datasource",),
                            input=f"{identifier}",
                            msg=f"Error insert DataSource in DB: {e}",
                            ctx={"datasource_id": f"{identifier}"},
                        )
                    )

    return count, validation_errors


def default_dump(obj):
    if isinstance(obj, ValueError):
        return obj.args[0]


@ds_router.post(
    "/datasources/reload_files",
    responses={200: {"content": {"application/json": {}}}},
    response_class=Response,
)
@ds_router.post(
    "/datasources/load_files",
    responses={200: {"content": {"application/json": {}}}},
    response_class=Response,
)
async def load_datasources_from_files(
    request: Request, datasource_ids: Optional[List[str]] = None
) -> Response:
    ds_path: str = os.path.join(request.app.state.root_path, "datasources")
    ds_vector_path: str = os.path.join(ds_path, "vector")
    ds_raster_path: str = os.path.join(ds_path, "raster")

    try:
        db_pool: Pool = request.app.state.db_pool
        connection: Connection
        errors: List[ErrorDetails] = []
        async with db_pool.acquire() as connection:
            # Load Vector DataSources
            if os.path.isdir(ds_vector_path):
                vector_count, vector_errors = await upload_datasources_to_db(
                    ds_vector_path, connection, datasource_ids
                )
            # Load Raster DataSources
            if os.path.isdir(ds_raster_path):
                raster_count, raster_errors = await upload_datasources_to_db(
                    ds_raster_path, connection, datasource_ids
                )
            vector_errors.extend(raster_errors)
            if len(vector_errors) > 0:
                errors = vector_errors
    except Exception as e:
        message: str = (
            f"'load_datasources_from_files' error upload DataSources from files: {str(e)}"
        )
        logger.error(message)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message
        )

    try:
        # Reload DataSources from DB
        request.app.state.datasources, request.app.state.datasources_from_db = (
            await load_datasources_from_db(request.app)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        )

    data = {
        "load_vector_datasources": vector_count,
        "load_raster_datasources": raster_count,
        "errors": errors,
    }
    content: bytes = orjson.dumps(data, default=default_dump)

    return Response(
        content=content,
        media_type="application/json",
    )


class DeleteDataSource(BaseModel):
    datasource_id: str


@ds_router.delete(
    "/datasources",
    responses={200: {"content": {"application/json": {}}}},
    response_class=JSONResponse,
)
async def delete_datasource(
    request: Request,
    del_ds: DeleteDataSource,
) -> JSONResponse:
    ds_id: str = del_ds.datasource_id
    query: str = f"DELETE FROM datasource WHERE identifier='{ds_id}'"
    try:
        db_pool: Pool = request.app.state.db_pool
        connection: Connection
        async with db_pool.acquire() as connection:
            await connection.execute(query)
    except Exception as e:
        message: str = (
            f"'delete_datasource': error delete DataSource '{ds_id}' {str(e)}"
        )
        logger.error(message)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message
        )

    pid = os.getpid()

    # 1. Remove DataSource from state ALL python workers
    try:
        if ds_id in request.app.state.datasources_from_db:
            del request.app.state.datasources_from_db[ds_id]
        if ds_id in request.app.state.datasources:
            del request.app.state.datasources[ds_id]
    except Exception as e:
        message: str = (
            f"Worker {pid}, error remove DataSource '{ds_id}' from state: {str(e)}"
        )
        logger.error(message)

    # 2. Clear 'data' and 'tiles' directories from DataSource folders
    try:
        tiles_dir: str = os.path.join(request.app.state.root_path, "tiles", ds_id)
        shutil.rmtree(tiles_dir, ignore_errors=True)
        data_dir: str = os.path.join(request.app.state.root_path, "data", ds_id)
        shutil.rmtree(data_dir, ignore_errors=True)
    except Exception as e:
        message = (
            f"Worker {pid}, error remove DataSource '{ds_id}' directories: {str(e)}"
        )
        logger.error(message)

    content: Dict[str, str] = {
        "message": f"Worker {pid}, DataSource '{ds_id}' successfully remove"
    }
    return JSONResponse(content=content)
