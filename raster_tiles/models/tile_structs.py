from collections import namedtuple

TileOverview = namedtuple("TileOverview", ("x", "y", "tileposx", "tileposy"))
Tile = namedtuple("Tile", ("dataset", "z", "x", "y", "ext"))
