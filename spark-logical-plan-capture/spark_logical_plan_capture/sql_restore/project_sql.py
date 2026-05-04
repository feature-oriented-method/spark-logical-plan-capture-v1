from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence


class UnsupportedPlanError(ValueError):
    """Raised when SQL cannot be reconstructed from the provided JSON plan."""


def project_json_to_sql(plan_json: Any) -> str:
    """Restore SQL text from serialized Spark logical plan JSON.

    The function targets plans containing
    `org.apache.spark.sql.catalyst.plans.logical.Project`.
    Input can be a JSON string, parsed dict/list, or bytes.
    """

    parsed = _parse_json(plan_json)
    normalized = _normalize_node(parsed)
    project = _find_first_project(normalized)
    if project is None:
        raise UnsupportedPlanError("Project node is not present in the logical plan JSON")
    return _ProjectSqlBuilder().build(project)


def _parse_json(plan_json: Any) -> Any:
    if isinstance(plan_json, bytes):
        return json.loads(plan_json.decode("utf-8"))
    if isinstance(plan_json, str):
        return json.loads(plan_json)
    return plan_json


def _find_first_project(node: Any) -> Optional[Dict[str, Any]]:
    if isinstance(node, dict):
        class_name = _class_name(node)
        if class_name == "Project":
            return node
        for value in node.values():
            found = _find_first_project(value)
            if found is not None:
                return found
        return None
    if isinstance(node, list):
        for item in node:
            found = _find_first_project(item)
            if found is not None:
                return found
    return None


def _normalize_node(node: Any) -> Any:
    if isinstance(node, list):
        return [_normalize_node(item) for item in node]
    if isinstance(node, dict):
        normalized = {k: _normalize_node(v) for k, v in node.items()}
        if "class" in normalized:
            normalized.setdefault("_short_class", _short_class_name(str(normalized["class"])))
        return normalized
    return node


def _short_class_name(full_name: str) -> str:
    return full_name.split(".")[-1]


def _class_name(node: Dict[str, Any]) -> str:
    if "_short_class" in node:
        return str(node["_short_class"])
    if "class" in node:
        return _short_class_name(str(node["class"]))
    return str(node.get("nodeName", ""))


def _sql_quote_identifier(identifier: str) -> str:
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        return identifier
    return "`" + identifier.replace("`", "``") + "`"


def _sql_quote_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _coalesce_keys(node: Dict[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        if key in node:
            return node[key]
    return None


def _to_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _flatten_expression_list(value: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    def walk(item: Any) -> None:
        if isinstance(item, dict):
            if "class" in item:
                out.append(item)
                return
            for nested in item.values():
                walk(nested)
            return
        if isinstance(item, list):
            for nested in item:
                walk(nested)

    walk(value)
    return out


@dataclass
class _ProjectSqlBuilder:
    def build(self, project: Dict[str, Any]) -> str:
        select_exprs = self._render_project_list(project)
        from_sql = self._render_from_clause(project)
        return f"SELECT {', '.join(select_exprs)}{from_sql}"

    def _render_project_list(self, project: Dict[str, Any]) -> List[str]:
        project_list = _coalesce_keys(project, ("projectList", "project_list", "list"))
        expressions = _to_list(project_list)
        if not expressions:
            raise UnsupportedPlanError("Project node does not contain projectList")

        rendered: List[str] = []
        for expr in expressions:
            if isinstance(expr, list):
                for nested in _flatten_expression_list(expr):
                    rendered.append(self._render_named_expression(nested))
            elif isinstance(expr, dict):
                rendered.append(self._render_named_expression(expr))
            else:
                raise UnsupportedPlanError(f"Unsupported project expression container: {type(expr)!r}")
        return rendered

    def _render_named_expression(self, expr: Dict[str, Any]) -> str:
        class_name = _class_name(expr)
        if class_name == "Alias":
            child = _coalesce_keys(expr, ("child", "children", "expr"))
            child_expr = self._render_expression(self._first_child(child))
            alias_name = str(_coalesce_keys(expr, ("name", "alias", "to")) or "")
            if not alias_name:
                return child_expr
            return f"{child_expr} AS {_sql_quote_identifier(alias_name)}"
        if class_name in {"UnresolvedAlias", "NamedExpression"}:
            child = self._first_child(_coalesce_keys(expr, ("child", "children", "expr")))
            return self._render_expression(child)
        if class_name in {"Star", "UnresolvedStar"}:
            target = _coalesce_keys(expr, ("target", "qualifier", "nameParts"))
            if isinstance(target, list) and target:
                return ".".join(_sql_quote_identifier(str(v)) for v in target) + ".*"
            return "*"
        return self._render_expression(expr)

    def _render_from_clause(self, project: Dict[str, Any]) -> str:
        child = _coalesce_keys(project, ("child", "children"))
        child_node = self._first_child(child)
        if child_node is None:
            return ""
        rendered = self._render_relation(child_node)
        return f" FROM {rendered}" if rendered else ""

    def _render_relation(self, node: Dict[str, Any]) -> str:
        class_name = _class_name(node)

        if class_name == "OneRowRelation":
            return "(SELECT 1)"
        if class_name == "LocalRelation":
            return "VALUES (...)"
        if class_name in {"UnresolvedRelation", "DataSourceV2Relation", "LogicalRelation"}:
            table = _coalesce_keys(node, ("tableName", "multipartIdentifier", "identifier", "name", "relationName"))
            if isinstance(table, list) and table:
                return ".".join(_sql_quote_identifier(str(v)) for v in table)
            if isinstance(table, dict):
                parts = _coalesce_keys(table, ("parts", "nameParts", "identifier"))
                if isinstance(parts, list) and parts:
                    return ".".join(_sql_quote_identifier(str(v)) for v in parts)
            if table:
                return _sql_quote_identifier(str(table))
            return "unknown_relation"
        if class_name == "SubqueryAlias":
            alias = str(_coalesce_keys(node, ("alias", "name")) or "subq")
            child = self._first_child(_coalesce_keys(node, ("child", "children")))
            if not child:
                return _sql_quote_identifier(alias)
            return f"({self._render_select_like(child)}) AS {_sql_quote_identifier(alias)}"
        if class_name in {"Project", "Filter", "Aggregate", "Join", "Sort", "Limit", "Distinct"}:
            return f"({self._render_select_like(node)})"
        return f"({self._render_select_like(node)})"

    def _render_select_like(self, node: Dict[str, Any]) -> str:
        class_name = _class_name(node)
        if class_name == "Project":
            return self.build(node)
        if class_name == "Filter":
            child = self._first_child(_coalesce_keys(node, ("child", "children")))
            condition = self._render_expression(self._first_child(_coalesce_keys(node, ("condition", "predicate", "children"))))
            from_sql = self._render_relation(child) if child else "(SELECT 1)"
            return f"SELECT * FROM {from_sql} WHERE {condition}"
        if class_name == "Aggregate":
            groups = _to_list(_coalesce_keys(node, ("groupingExpressions", "groupingExprs", "groupExprs")))
            aggs = _to_list(_coalesce_keys(node, ("aggregateExpressions", "aggregateExprs", "resultExpressions")))
            child = self._first_child(_coalesce_keys(node, ("child", "children")))
            from_sql = self._render_relation(child) if child else "(SELECT 1)"
            select_exprs = [self._render_named_expression(expr) for expr in aggs] if aggs else ["*"]
            group_sql = ", ".join(self._render_expression(expr) for expr in groups)
            if group_sql:
                return f"SELECT {', '.join(select_exprs)} FROM {from_sql} GROUP BY {group_sql}"
            return f"SELECT {', '.join(select_exprs)} FROM {from_sql}"
        if class_name == "Join":
            left = self._first_child(_coalesce_keys(node, ("left", "leftChild", "children")))
            right = self._second_child(_coalesce_keys(node, ("right", "rightChild", "children")))
            join_type = str(_coalesce_keys(node, ("joinType", "type")) or "inner").upper()
            condition_node = self._first_child(_coalesce_keys(node, ("condition", "joinCondition")))
            if not left or not right:
                raise UnsupportedPlanError("Join node misses left/right children")
            if condition_node:
                cond = self._render_expression(condition_node)
                return f"SELECT * FROM {self._render_relation(left)} {join_type} JOIN {self._render_relation(right)} ON {cond}"
            return f"SELECT * FROM {self._render_relation(left)} {join_type} JOIN {self._render_relation(right)}"
        if class_name == "Sort":
            child = self._first_child(_coalesce_keys(node, ("child", "children")))
            orders = _to_list(_coalesce_keys(node, ("order", "sortOrder", "orderExpressions")))
            order_sql = ", ".join(self._render_expression(expr) for expr in orders)
            from_sql = self._render_relation(child) if child else "(SELECT 1)"
            if order_sql:
                return f"SELECT * FROM {from_sql} ORDER BY {order_sql}"
            return f"SELECT * FROM {from_sql}"
        if class_name == "Limit":
            child = self._first_child(_coalesce_keys(node, ("child", "children")))
            limit = self._render_expression(self._first_child(_coalesce_keys(node, ("limitExpr", "limit", "children"))))
            from_sql = self._render_relation(child) if child else "(SELECT 1)"
            return f"SELECT * FROM {from_sql} LIMIT {limit}"
        if class_name == "Distinct":
            child = self._first_child(_coalesce_keys(node, ("child", "children")))
            from_sql = self._render_relation(child) if child else "(SELECT 1)"
            return f"SELECT DISTINCT * FROM {from_sql}"
        return "SELECT *"

    def _render_expression(self, expr: Any) -> str:
        if expr is None:
            return "NULL"
        if not isinstance(expr, dict):
            if isinstance(expr, str):
                return _sql_quote_string(expr)
            return str(expr)

        class_name = _class_name(expr)
        children = self._children(expr)

        if class_name in {"Literal"}:
            return self._render_literal(expr)
        if class_name in {"AttributeReference", "UnresolvedAttribute"}:
            parts = _coalesce_keys(expr, ("nameParts", "parts", "qualifier"))
            name = _coalesce_keys(expr, ("name", "sql", "attr")) or ""
            if isinstance(parts, list) and parts:
                return ".".join(_sql_quote_identifier(str(v)) for v in parts)
            if isinstance(name, str) and name:
                if "." in name and not name.startswith("`"):
                    return ".".join(_sql_quote_identifier(part) for part in name.split("."))
                return _sql_quote_identifier(name)
            return "col"
        if class_name in {"Alias", "UnresolvedAlias", "NamedExpression"}:
            return self._render_named_expression(expr)
        if class_name in {"Cast"}:
            data_type = _coalesce_keys(expr, ("dataType", "targetType", "to")) or "STRING"
            return f"CAST({self._render_expression(children[0] if children else None)} AS {str(data_type).upper()})"
        if class_name in {"SortOrder"}:
            direction = str(_coalesce_keys(expr, ("direction", "sortDirection")) or "ASC").upper()
            null_ordering = str(_coalesce_keys(expr, ("nullOrdering",)) or "")
            ordering = f"{self._render_expression(children[0] if children else None)} {direction}"
            if null_ordering:
                ordering += f" {null_ordering.upper()}"
            return ordering

        binary_op = {
            "Add": "+",
            "Subtract": "-",
            "Multiply": "*",
            "Divide": "/",
            "Remainder": "%",
            "EqualTo": "=",
            "EqualNullSafe": "<=>",
            "GreaterThan": ">",
            "GreaterThanOrEqual": ">=",
            "LessThan": "<",
            "LessThanOrEqual": "<=",
            "And": "AND",
            "Or": "OR",
            "BitwiseAnd": "&",
            "BitwiseOr": "|",
            "BitwiseXor": "^",
        }
        if class_name in binary_op and len(children) >= 2:
            left = self._render_expression(children[0])
            right = self._render_expression(children[1])
            return f"({left} {binary_op[class_name]} {right})"

        if class_name in {"Not"} and children:
            return f"(NOT {self._render_expression(children[0])})"
        if class_name in {"UnaryMinus"} and children:
            return f"(-{self._render_expression(children[0])})"
        if class_name in {"IsNull"} and children:
            return f"({self._render_expression(children[0])} IS NULL)"
        if class_name in {"IsNotNull"} and children:
            return f"({self._render_expression(children[0])} IS NOT NULL)"
        if class_name in {"In"} and len(children) >= 2:
            target = self._render_expression(children[0])
            values = ", ".join(self._render_expression(v) for v in children[1:])
            return f"({target} IN ({values}))"
        if class_name in {"InSet"}:
            target = self._render_expression(children[0] if children else None)
            values = _to_list(_coalesce_keys(expr, ("hset", "values", "list")))
            rendered = ", ".join(self._render_expression(v) for v in values)
            return f"({target} IN ({rendered}))"
        if class_name in {"Like"} and len(children) >= 2:
            return f"({self._render_expression(children[0])} LIKE {self._render_expression(children[1])})"
        if class_name in {"RLike"} and len(children) >= 2:
            return f"({self._render_expression(children[0])} RLIKE {self._render_expression(children[1])})"
        if class_name in {"StartsWith"} and len(children) >= 2:
            return f"({self._render_expression(children[0])} LIKE CONCAT({self._render_expression(children[1])}, '%'))"
        if class_name in {"EndsWith"} and len(children) >= 2:
            return f"({self._render_expression(children[0])} LIKE CONCAT('%', {self._render_expression(children[1])}))"
        if class_name in {"Contains"} and len(children) >= 2:
            return (
                f"({self._render_expression(children[0])} "
                f"LIKE CONCAT('%', {self._render_expression(children[1])}, '%'))"
            )
        if class_name in {"CaseWhen"}:
            branches = _to_list(_coalesce_keys(expr, ("branches", "cases")))
            else_value = _coalesce_keys(expr, ("elseValue", "else", "otherwise"))
            chunks: List[str] = ["CASE"]
            if branches:
                for pair in branches:
                    if isinstance(pair, list) and len(pair) >= 2:
                        chunks.append(
                            f"WHEN {self._render_expression(pair[0])} THEN {self._render_expression(pair[1])}"
                        )
                    elif isinstance(pair, dict):
                        cond = self._first_child(_coalesce_keys(pair, ("condition", "when", "left", "children")))
                        val = self._first_child(_coalesce_keys(pair, ("value", "then", "right", "children")))
                        if cond is not None and val is not None:
                            chunks.append(
                                f"WHEN {self._render_expression(cond)} THEN {self._render_expression(val)}"
                            )
            if else_value is not None:
                chunks.append(f"ELSE {self._render_expression(self._first_child(else_value) or else_value)}")
            chunks.append("END")
            return " ".join(chunks)
        if class_name in {"If"} and len(children) >= 3:
            return (
                f"CASE WHEN {self._render_expression(children[0])} "
                f"THEN {self._render_expression(children[1])} "
                f"ELSE {self._render_expression(children[2])} END"
            )

        if class_name in {"UnresolvedFunction", "ScalaUDF", "PythonUDF"}:
            name = _coalesce_keys(expr, ("name", "funcName", "functionName")) or "function"
            return self._render_function_call(str(name), children)

        if class_name in {"UnresolvedStar", "Star"}:
            return self._render_named_expression(expr)

        if class_name in {"GetStructField"} and children:
            base = self._render_expression(children[0])
            field = _coalesce_keys(expr, ("name", "fieldName", "field"))
            if field:
                return f"{base}.{_sql_quote_identifier(str(field))}"
            ordinal = _coalesce_keys(expr, ("ordinal", "index"))
            if ordinal is not None:
                return f"{base}[{ordinal}]"

        if class_name in {"GetArrayItem"} and len(children) >= 2:
            return f"{self._render_expression(children[0])}[{self._render_expression(children[1])}]"
        if class_name in {"GetMapValue"} and len(children) >= 2:
            return f"{self._render_expression(children[0])}[{self._render_expression(children[1])}]"

        func_name = _camel_to_snake(class_name)
        return self._render_function_call(func_name, children)

    def _render_literal(self, expr: Dict[str, Any]) -> str:
        value = _coalesce_keys(expr, ("value", "literal", "v"))
        data_type = str(_coalesce_keys(expr, ("dataType", "type")) or "").lower()
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, str):
            upper = value.upper()
            if data_type in {"date", "timestamp"}:
                return _sql_quote_string(value)
            if upper in {"TRUE", "FALSE"} and data_type == "boolean":
                return upper
            if re.fullmatch(r"-?\d+(\.\d+)?", value) and data_type in {
                "byte",
                "short",
                "integer",
                "long",
                "float",
                "double",
                "decimal",
            }:
                return value
            return _sql_quote_string(value)
        if isinstance(value, list):
            return "(" + ", ".join(self._render_expression(v) for v in value) + ")"
        return _sql_quote_string(str(value))

    def _render_function_call(self, name: str, children: Sequence[Any]) -> str:
        args = ", ".join(self._render_expression(child) for child in children)
        return f"{name}({args})"

    def _children(self, expr: Dict[str, Any]) -> List[Any]:
        explicit_children = _coalesce_keys(expr, ("children", "arguments", "args", "inputs"))
        children: List[Any] = []
        if explicit_children is not None:
            children.extend(_to_list(explicit_children))

        for key in (
            "child",
            "left",
            "right",
            "argument",
            "predicate",
            "condition",
            "trueValue",
            "falseValue",
            "start",
            "stop",
            "step",
            "key",
            "value",
            "collection",
            "field",
            "query",
            "ordinal",
        ):
            if key in expr:
                children.append(expr[key])

        flattened: List[Any] = []
        for child in children:
            if isinstance(child, list):
                flattened.extend(child)
            else:
                flattened.append(child)
        return [c for c in flattened if isinstance(c, (dict, str, int, float, bool)) and c is not None]

    def _first_child(self, value: Any) -> Optional[Any]:
        if value is None:
            return None
        if isinstance(value, list):
            return value[0] if value else None
        return value

    def _second_child(self, value: Any) -> Optional[Any]:
        if value is None:
            return None
        if isinstance(value, list):
            return value[1] if len(value) > 1 else None
        return None


def _camel_to_snake(name: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
