import sqlite3


class TileDetail(object):

    __slots__ = (
        "tx",
        "ty",
        "tz",
        "rx",
        "ry",
        "rxsize",
        "rysize",
        "wx",
        "wy",
        "wxsize",
        "wysize",
        "querysize",
    )

    def __init__(self, **kwargs):

        self.tx: int = 0
        self.ty: int = 0
        self.tz: int = 0
        self.rx: int = 0
        self.ry: int = 0
        self.rxsize: int = 0
        self.rysize: int = 0
        self.wx: int = 0
        self.wy: int = 0
        self.wxsize: int = 0
        self.wysize: int = 0
        self.querysize: int = 0

        for key in kwargs:
            if hasattr(self, key):
                setattr(self, key, kwargs[key])

    def __unicode__(self):
        return f"TileDetail tz: {self.tz}, tx: {self.tx}, ty: {self.ty}, rx: {self.rx}, ry: {self.ry},\
            rxsize: {self.rxsize}, rysize: {self.rysize}, wx: {self.wx}, wy: {self.wy}, wxsize: {self.wxsize}, wysize: {self.wysize},\
            querysize: {self.querysize}"

    def __str__(self):
        return f"TileDetail tz: {self.tz}, tx: {self.tx}, ty: {self.ty}, rx: {self.rx}, ry: {self.ry},\
            rxsize: {self.rxsize}, rysize: {self.rysize}, wx: {self.wx}, wy: {self.wy}, wxsize: {self.wxsize}, wysize: {self.wysize},\
            querysize: {self.querysize}"

    def __repr__(self):
        return f"TileDetail tz: {self.tz}, tx: {self.tx}, ty: {self.ty}, rx: {self.rx}, ry: {self.ry},\
            rxsize: {self.rxsize}, rysize: {self.rysize}, wx: {self.wx}, wy: {self.wy}, wxsize: {self.wxsize}, wysize: {self.wysize},\
            querysize: {self.querysize}"


def tiles_detail_setup(cursor: sqlite3.Cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE tiles_detail (
            tz integer NOT NULL,
            tx integer NOT NULL,
            ty integer NOT NULL,
            rx integer,
            ry integer,
            rxsize integer,
            rysize integer,
            wx integer,
            wy integer,
            wxsize integer,
            wysize integer,
            querysize integet,
            PRIMARY KEY(tz, tx, ty)
        );
        """
    )

    cursor.execute(
        """CREATE TABLE tile_job (
            data_bands_count integer NOT NULL,
            nodata real,
            src_file text NOT NULL,
            tile_extension text NOT NULL,
            tile_size integer NOT NULL,
            tile_driver text NOT NULL,
            profile text NOT NULL,
            querysize integer NOT NULL,
            xyz integer NOT NULL,
            in_file text NOT NULL,
            input_file text NOT NULL,
            encode_to_rgba integer,
            has_alpha_band integer,
            pixel_selection_method text,
            resampling_method text,
            merge integer
        );"""
    )

    cursor.execute(
        """CREATE TABLE tminmax (
            id integer PRIMARY KEY AUTOINCREMENT,
            tz integer NOT NULL,
            tminx integer NOT NULL,
            tmaxx integer NOT NULL,
            tminy integer NOT NULL,
            tmaxy integer NOT NULL,
            asset text            
        );"""
    )

    cursor.execute(
        """CREATE TABLE tminz_tmaxz (
            id integer PRIMARY KEY AUTOINCREMENT,            
            tminz integer NOT NULL,
            tmaxz integer NOT NULL,            
            asset text            
        );"""
    )

    cursor.execute(
        """
        CREATE TABLE empty_tiles (
            id integer PRIMARY KEY AUTOINCREMENT,
            x integer NOT NULL,
            y integer NOT NULL,
            z integer NOT NULL,            
            UNIQUE(x, y, z)
        );
        """
    )

    cursor.execute(
        """
        CREATE TABLE assets (
            id integer PRIMARY KEY AUTOINCREMENT,
            asset text
        );
        """
    )
