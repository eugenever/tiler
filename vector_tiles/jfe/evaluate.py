from typing import Dict, Optional

import shapely.geometry

from vector_tiles.jfe import ast
from vector_tiles.jfe import values
from pygeofilter.backends.evaluator import Evaluator, handle

COMPARISON_OP_MAP = {
    ast.ComparisonOp.EQ: "=",
    ast.ComparisonOp.NE: "<>",
    ast.ComparisonOp.LT: "<",
    ast.ComparisonOp.LE: "<=",
    ast.ComparisonOp.GT: ">",
    ast.ComparisonOp.GE: ">=",
}


ARITHMETIC_OP_MAP = {
    ast.ArithmeticOp.ADD: "+",
    ast.ArithmeticOp.SUB: "-",
    ast.ArithmeticOp.MUL: "*",
    ast.ArithmeticOp.DIV: "/",
}

SPATIAL_COMPARISON_OP_MAP = {
    ast.SpatialComparisonOp.INTERSECTS: "ST_Intersects",
    ast.SpatialComparisonOp.DISJOINT: "ST_Disjoint",
    ast.SpatialComparisonOp.CONTAINS: "ST_Contains",
    ast.SpatialComparisonOp.WITHIN: "ST_Within",
    ast.SpatialComparisonOp.TOUCHES: "ST_Touches",
    ast.SpatialComparisonOp.CROSSES: "ST_Crosses",
    ast.SpatialComparisonOp.OVERLAPS: "ST_Overlaps",
    ast.SpatialComparisonOp.EQUALS: "ST_Equals",
}


class SQLEvaluator(Evaluator):
    def __init__(self, field_mapping: Dict[str, str], function_map: Dict[str, str]):
        self.field_mapping = field_mapping
        self.function_map = function_map

    @handle(ast.Not)
    def not_(self, node, sub):
        return f"NOT {sub}"

    @handle(ast.And, ast.Or)
    def combination(self, node, lhs, rhs):
        return f"({lhs} {node.op.value} {rhs})"

    @handle(ast.Comparison, subclasses=True)
    def comparison(self, node, lhs, rhs):
        # To exclude quotes from spatial calls
        if "ST_GeometryType" in lhs:
            lhs = lhs.replace("'", "")
        return f"({lhs} {COMPARISON_OP_MAP[node.op]} {rhs})"

    @handle(ast.Between)
    def between(self, node, lhs, low, high):
        return f"({lhs} {'NOT ' if node.not_ else ''}BETWEEN {low} AND {high})"

    @handle(ast.Like)
    def like(self, node, lhs):
        pattern = node.pattern
        if node.wildcard != "%":
            # TODO: not preceded by escapechar
            pattern = pattern.replace(node.wildcard, "%")
        if node.singlechar != "_":
            # TODO: not preceded by escapechar
            pattern = pattern.replace(node.singlechar, "_")

        # TODO: handle node.nocase
        return (
            f"{lhs} {'NOT ' if node.not_ else ''}LIKE "
            f"'{pattern}' ESCAPE '{node.escapechar}'"
        )

    @handle(ast.In)
    def in_(self, node, lhs, *options):
        return f"{lhs} {'NOT ' if node.not_ else ''}IN ({', '.join(options)})"

    @handle(ast.IsNull)
    def null(self, node, lhs):
        return f"{lhs} IS {'NOT ' if node.not_ else ''}NULL"

    # @handle(ast.TemporalPredicate, subclasses=True)
    # def temporal(self, node, lhs, rhs):
    #     pass

    @handle(ast.SpatialComparisonPredicate, subclasses=True)
    def spatial_operation(self, node, lhs, rhs):
        func = SPATIAL_COMPARISON_OP_MAP[node.op]
        return f"{func}({lhs},{rhs})"

    @handle(ast.BBox)
    def bbox(self, node, lhs):
        func = SPATIAL_COMPARISON_OP_MAP[ast.SpatialComparisonOp.INTERSECTS]
        rhs = f"ST_GeomFromText('POLYGON(({node.minx} {node.miny}, {node.minx} {node.maxy}, {node.maxx} {node.maxy}, {node.maxx} {node.miny}, {node.minx} {node.miny}))')"  # noqa
        return f"{func}({lhs},{rhs})"

    @handle(ast.Attribute)
    def attribute(self, node: ast.Attribute):
        if self.field_mapping is None:
            raise Exception(f"SQLEvaluator.field_mapping can't be None")
        # Fields from WHERE clause must be present in SELECT clause
        if node.name not in self.field_mapping:
            raise Exception(f"Field '{node.name}' not present in SELECT clause")

        map_name = self.field_mapping[node.name]
        return f'"{map_name}"'

    @handle(ast.Arithmetic, subclasses=True)
    def arithmetic(self, node: ast.Arithmetic, lhs, rhs):
        op = ARITHMETIC_OP_MAP[node.op]
        return f"({lhs} {op} {rhs})"

    @handle(ast.Function)
    def function(self, node, *arguments):
        func = self.function_map[node.name]
        return f"{func}({','.join(arguments)})"

    @handle(*values.LITERALS)
    def literal(self, node):
        if isinstance(node, str):
            return f"'{node}'"
        else:
            # TODO:
            return str(node)

    @handle(values.Geometry)
    def geometry(self, node: values.Geometry):
        wkb_hex = shapely.geometry.shape(node).wkb_hex
        return f"ST_GeomFromWKB(x'{wkb_hex}')"

    @handle(values.Envelope)
    def envelope(self, node: values.Envelope):
        wkb_hex = shapely.geometry.box(node.x1, node.y1, node.x2, node.y2).wkb_hex
        return f"ST_GeomFromWKB(x'{wkb_hex}')"


def to_sql_where(
    root: ast.Node,
    field_mapping: Dict[str, str],
    function_map: Optional[Dict[str, str]] = None,
) -> str:
    return SQLEvaluator(field_mapping, function_map or {}).evaluate(root)
