import sqlite3
import aiosqlite

TABLE_TILES: str = """
    CREATE TABLE tiles (
        zoom_level integer NOT NULL,
        tile_column integer NOT NULL,
        tile_row integer NOT NULL,
        tile_data blob,
        PRIMARY KEY(zoom_level, tile_column, tile_row)
    );
"""

TABLE_METADATA: str = """CREATE TABLE metadata (name text, value text);"""

TABLE_GRIDS: str = """
    CREATE TABLE grids (
        zoom_level integer NOT NULL,
        tile_column integer NOT NULL,
        tile_row integer NOT NULL,
        grid blob
    );
"""

TABLE_GRID_DATA: str = """
    CREATE TABLE grid_data (
        zoom_level integer NOT NULL,
        tile_column integer NOT NULL,
        tile_row integer NOT NULL,
        key_name text,
        key_json text
    );
"""


def mbtiles_setup(cursor: sqlite3.Cursor):
    cursor.execute(TABLE_TILES)
    cursor.execute(TABLE_METADATA)
    cursor.execute(TABLE_GRIDS)
    cursor.execute(TABLE_GRID_DATA)


def create_index(cursor: sqlite3.Cursor):
    cursor.execute("""create unique index name on metadata (name);""")


async def async_mbtiles_setup(cursor: aiosqlite.Cursor):
    await cursor.execute(TABLE_TILES)
    await cursor.execute(TABLE_METADATA)
    await cursor.execute(TABLE_GRIDS)
    await cursor.execute(TABLE_GRID_DATA)
