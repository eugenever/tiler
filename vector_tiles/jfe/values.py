from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, List, Optional, Union

from pygeoif import shape


@dataclass
class Geometry:
    geometry: dict

    @property
    def __geo_interface__(self):
        return self.geometry

    def __eq__(self, o: object) -> bool:
        return shape(self).__geo_interface__ == shape(o).__geo_interface__


@dataclass
class Envelope:
    x1: float
    x2: float
    y1: float
    y2: float

    @property
    def geometry(self):
        return {
            "type": "Polygon",
            "coordinates": [
                [
                    [self.x1, self.y1],
                    [self.x1, self.y2],
                    [self.x2, self.y2],
                    [self.x2, self.y1],
                    [self.x1, self.y1],
                ]
            ],
        }

    @property
    def __geo_interface__(self):
        return self.geometry

    def __eq__(self, o: object) -> bool:
        return shape(self).__geo_interface__ == shape(o).__geo_interface__


@dataclass
class Interval:
    start: Optional[Union[date, datetime, timedelta]] = None
    end: Optional[Union[date, datetime, timedelta]] = None

    def get_sub_nodes(self) -> List[Any]:  # TODO: find way to type this
        return [self.start, self.end]


# used for handler declaration
LITERALS = (list, str, float, int, bool, datetime, date, time, timedelta)

# used for type checking

SpatialValueType = Union[Geometry, Envelope]

TemporalValueType = Union[date, datetime, timedelta, Interval]

ValueType = Union[
    SpatialValueType,
    TemporalValueType,
    bool,
    float,
    int,
    str,
]
