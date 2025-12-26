import enum
import json


class TileDriver(enum.Enum):
    PNG = 1
    JPG = 2
    JPEG = 3
    MEM = 4
    GTiff = 5
    WEBP = 6

    def toJSON(self):
        return json.dumps(self.name)
