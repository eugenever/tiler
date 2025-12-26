import orjson

from datetime import datetime
from typing import Any, Dict, List, Type, Union, cast
from pygeofilter.util import parse_datetime

from vector_tiles.jfe import ast
from vector_tiles.jfe import values


COMPARISON_MAP: Dict[str, Type] = {
    "==": ast.Equal,
    "!=": ast.NotEqual,
    "<": ast.LessThan,
    "<=": ast.LessEqual,
    ">": ast.GreaterThan,
    ">=": ast.GreaterEqual,
}

SPATIAL_PREDICATES_MAP = {
    "intersects": ast.GeometryIntersects,
    "within": ast.GeometryWithin,
}

TEMPORAL_PREDICATES_MAP = {
    "before": ast.TimeBefore,
    "after": ast.TimeAfter,
    "during": ast.TimeDuring,
}

ARITHMETIC_MAP = {
    "+": ast.Add,
    "-": ast.Sub,
    "*": ast.Mul,
    "/": ast.Div,
}

FUNCTION_MAP = {
    "%": "mod",
    "^": "pow",
}

TYPING_PREDICATES = [
    "array",
    "boolean",
    "number",
    "string",
    "literal",
    "to-boolean",
    "to-number",
    "to-string",
]

SPATIAL_PREDICATES = ["$type", "geometry-type"]

ParseResult = Union[
    ast.Node,
    str,
    float,
    int,
    datetime,
    values.Geometry,
    values.Interval,
    Dict[Any, Any],  # TODO: for like wildcards.
    List[Any],
]


def _parse_node(node: Union[list, dict], geom_field: str) -> ParseResult:  # noqa: C901
    if isinstance(node, (str, float, int)):
        return node
    elif isinstance(node, dict):
        # wrap geometry, we say that the 'type' property defines if it is a
        # geometry
        if "type" in node:
            return values.Geometry(node)

        # just return objects for example `like` wildcards
        else:
            return node

    if not isinstance(node, list):
        raise ValueError(f"Invalid node class {type(node)}")

    op = node[0]
    arguments = [_parse_node(sub, geom_field) for sub in node[1:]]

    if op in ["all", "any"]:
        cls = ast.And if op == "all" else ast.Or
        return cls.from_items(*arguments)

    elif op == "!":
        return ast.Not(*cast(List[ast.Node], arguments))

    elif op in COMPARISON_MAP:
        """
        For cases without GET expression:
        ["<=", "admin_level", 3]
        """
        assert len(arguments) > 1

        if (arguments[0] == "$type") or (
            isinstance(arguments[0], list)
            and len(arguments[0]) == 1
            and arguments[0][0] == "geometry-type"
        ):
            return COMPARISON_MAP[op](
                *[f"ST_GeometryType({geom_field})", f"ST_{arguments[1]}"]
            )

        if not isinstance(arguments[0], ast.Attribute):
            return COMPARISON_MAP[op](*[ast.Attribute(arguments[0]), *arguments[1:]])

        # with GET expression
        return COMPARISON_MAP[op](*arguments)

    elif op == "like":
        wildcard = "%"
        if len(arguments) > 2:
            wildcard = cast(dict, arguments[2]).get("wildCard", "%")
        return ast.Like(
            cast(ast.Node, arguments[0]),
            cast(str, arguments[1]),
            nocase=False,
            wildcard=wildcard,
            singlechar=".",
            escapechar="\\",
            not_=False,
        )

    elif op == "in":
        arg0 = arguments[0]
        if not isinstance(arguments[0], ast.Attribute):
            arg0 = ast.Attribute(arg0)
        else:
            arg0 = arguments[0]
        return ast.In(
            cast(ast.Node, arg0),
            cast(List[ast.AstType], arguments[1:]),
            not_=False,
        )

    elif op == "!in":
        arg0 = arguments[0]
        if not isinstance(arguments[0], ast.Attribute):
            arg0 = ast.Attribute(arg0)
        else:
            arg0 = arguments[0]
        return ast.In(
            cast(ast.Node, arg0),
            cast(List[ast.AstType], arguments[1:]),
            not_=True,
        )

    elif op == "has":
        arg0 = arguments[0]
        if not isinstance(arguments[0], ast.Attribute):
            arg0 = ast.Attribute(arg0)
        else:
            arg0 = arguments[0]
        return ast.IsNull(
            cast(ast.Node, arg0),
            not_=True,
        )

    elif op == "!has":
        arg0 = arguments[0]
        if not isinstance(arguments[0], ast.Attribute):
            arg0 = ast.Attribute(arg0)
        else:
            arg0 = arguments[0]
        return ast.IsNull(
            cast(ast.Node, arg0),
            not_=False,
        )

    elif op in SPATIAL_PREDICATES_MAP:
        return SPATIAL_PREDICATES_MAP[op](*cast(List[ast.SpatialAstType], arguments))

    elif op in TEMPORAL_PREDICATES_MAP:
        # parse strings to datetimes
        dt_args = [
            parse_datetime(arg) if isinstance(arg, str) else arg for arg in arguments
        ]
        if len(arguments) == 3:
            if isinstance(dt_args[0], datetime) and isinstance(dt_args[1], datetime):
                dt_args = [
                    values.Interval(dt_args[0], dt_args[1]),
                    dt_args[2],
                ]
            if isinstance(dt_args[1], datetime) and isinstance(dt_args[2], datetime):
                dt_args = [
                    dt_args[0],
                    values.Interval(dt_args[1], dt_args[2]),
                ]

        return TEMPORAL_PREDICATES_MAP[op](*cast(List[ast.TemporalAstType], dt_args))

    # normal property getter
    elif op == "get":
        return ast.Attribute(arguments[0])

    elif op in TYPING_PREDICATES:
        assert len(node) > 1
        return _parse_node(node[1], geom_field)

    elif op == "bbox":
        pass  # TODO

    elif op in ARITHMETIC_MAP:
        return ARITHMETIC_MAP[op](*cast(List[ast.ScalarAstType], arguments))

    elif op in ["%", "floor", "ceil", "abs", "^", "min", "max"]:
        return ast.Function(
            FUNCTION_MAP.get(op, op), cast(List[ast.AstType], arguments)
        )

    # https://maplibre.org/maplibre-style-spec/deprecations/
    elif isinstance(node, list):
        return node

    raise ValueError(f"Invalid expression operation '{op}'")


def parse_jfe(jfe: Union[str, list, dict], geom_field: str) -> ast.Node:
    """
    Parses the given JFE expression (either a string or an already
    parsed JSON) to an AST.
    If a string is passed, it will be parsed as JSON.

    """
    if isinstance(jfe, str):
        root = orjson.loads(jfe)
    else:
        root = jfe

    return cast(ast.Node, _parse_node(root, geom_field))
