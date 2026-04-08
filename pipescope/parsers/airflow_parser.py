"""Airflow DAG parsing via Python AST (no Airflow imports required)."""

from __future__ import annotations

import ast

from pipescope.models import Asset, AssetType, Edge


def parse_airflow_file(file_path: str, content: str) -> tuple[list[Asset], list[Edge]]:
    """Parse a Python DAG file and extract Airflow DAG/task assets + edges."""
    try:
        tree = ast.parse(content, filename=file_path)
    except SyntaxError:
        return [], []

    visitor = AirflowVisitor(file_path=file_path)
    visitor.visit(tree)
    return visitor.to_assets_edges()


class AirflowVisitor(ast.NodeVisitor):
    """AST visitor that extracts DAGs, tasks, and task dependencies."""

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.dags: set[str] = set()
        self.current_dag: str | None = None
        self.task_vars: dict[str, str] = {}  # python variable -> task_id
        self.task_meta: dict[str, dict[str, str]] = {}  # task_id -> metadata
        self.dependencies: set[tuple[str, str]] = set()  # (upstream_task_id, downstream_task_id)

    def visit_Assign(self, node: ast.Assign) -> None:
        if isinstance(node.value, ast.Call):
            func_name = self._get_func_name(node.value)
            if func_name == "DAG":
                dag_id = self._extract_dag_id(node.value)
                if dag_id:
                    self.dags.add(dag_id)
                    self.current_dag = dag_id
            elif self._looks_like_operator(func_name):
                task_id = self._extract_keyword_str(node.value, "task_id")
                if task_id:
                    self.task_meta.setdefault(
                        task_id,
                        {
                            "dag_id": self.current_dag or "",
                            "operator": func_name,
                            "sql": self._extract_keyword_str(node.value, "sql") or "",
                        },
                    )
                    for target in node.targets:
                        var_name = self._resolve_var_name(target)
                        if var_name:
                            self.task_vars[var_name] = task_id
        self.generic_visit(node)

    def visit_With(self, node: ast.With) -> None:
        prev_dag = self.current_dag
        for item in node.items:
            if isinstance(item.context_expr, ast.Call):
                if self._get_func_name(item.context_expr) == "DAG":
                    dag_id = self._extract_dag_id(item.context_expr)
                    if dag_id:
                        self.dags.add(dag_id)
                        self.current_dag = dag_id
        self.generic_visit(node)
        self.current_dag = prev_dag

    def visit_BinOp(self, node: ast.BinOp) -> None:
        if isinstance(node.op, ast.RShift):
            left_ids = self._resolve_names_to_task_ids(_flatten_dep_expr(node.left))
            right_ids = self._resolve_names_to_task_ids(_flatten_dep_expr(node.right))
            for upstream in left_ids:
                for downstream in right_ids:
                    self.dependencies.add((upstream, downstream))
        elif isinstance(node.op, ast.LShift):
            # a << b means b -> a
            left_ids = self._resolve_names_to_task_ids(_flatten_dep_expr(node.left))
            right_ids = self._resolve_names_to_task_ids(_flatten_dep_expr(node.right))
            for downstream in left_ids:
                for upstream in right_ids:
                    self.dependencies.add((upstream, downstream))
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        # task_a.set_downstream(task_b) / task_b.set_upstream(task_a)
        if isinstance(node.func, ast.Attribute):
            owner = self._resolve_expr_to_task_ids(node.func.value)
            args = self._resolve_args_to_task_ids(node.args)
            if node.func.attr == "set_downstream":
                for o in owner:
                    for a in args:
                        self.dependencies.add((o, a))
            elif node.func.attr == "set_upstream":
                for o in owner:
                    for a in args:
                        self.dependencies.add((a, o))
        self.generic_visit(node)

    def to_assets_edges(self) -> tuple[list[Asset], list[Edge]]:
        assets: list[Asset] = []
        edges: list[Edge] = []

        for dag_id in sorted(self.dags):
            assets.append(
                Asset(
                    name=dag_id,
                    asset_type=AssetType.AIRFLOW_DAG,
                    file_path=self.file_path,
                )
            )

        for task_id in sorted(self.task_meta):
            meta = self.task_meta[task_id]
            dag_id = meta.get("dag_id") or "unknown_dag"
            sql = meta.get("sql", "")
            tags = {"dag_id": dag_id, "operator": meta.get("operator", "")}
            if sql:
                tags["sql"] = sql
            assets.append(
                Asset(
                    name=f"{dag_id}.{task_id}",
                    asset_type=AssetType.AIRFLOW_TASK,
                    file_path=self.file_path,
                    tags=tags,
                )
            )
            edges.append(Edge(source=dag_id, target=f"{dag_id}.{task_id}"))

        for upstream, downstream in sorted(self.dependencies):
            up_full = self._task_full_name(upstream)
            down_full = self._task_full_name(downstream)
            if up_full and down_full:
                edges.append(Edge(source=up_full, target=down_full))

        return assets, edges

    def _task_full_name(self, task_id: str) -> str | None:
        meta = self.task_meta.get(task_id)
        if not meta:
            return None
        dag_id = meta.get("dag_id") or "unknown_dag"
        return f"{dag_id}.{task_id}"

    def _extract_dag_id(self, call: ast.Call) -> str | None:
        # DAG("my_dag", ...) or DAG(dag_id="my_dag", ...)
        dag_id = self._extract_keyword_str(call, "dag_id")
        if dag_id:
            return dag_id
        if (
            call.args
            and isinstance(call.args[0], ast.Constant)
            and isinstance(call.args[0].value, str)
        ):
            return call.args[0].value
        return None

    def _extract_keyword_str(self, call: ast.Call, name: str) -> str | None:
        for kw in call.keywords:
            if kw.arg != name:
                continue
            if isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                return kw.value.value
        return None

    def _get_func_name(self, call: ast.Call) -> str:
        func = call.func
        if isinstance(func, ast.Name):
            return func.id
        if isinstance(func, ast.Attribute):
            return func.attr
        return ""

    def _looks_like_operator(self, name: str) -> bool:
        return name.endswith("Operator") or name in {"TaskGroup"}

    def _resolve_var_name(self, node: ast.AST) -> str | None:
        if isinstance(node, ast.Name):
            return node.id
        return None

    def _resolve_expr_to_task_ids(self, expr: ast.AST) -> list[str]:
        if isinstance(expr, ast.Name):
            task_id = self.task_vars.get(expr.id, expr.id if expr.id in self.task_meta else None)
            return [task_id] if task_id else []
        if isinstance(expr, ast.List):
            out: list[str] = []
            for elt in expr.elts:
                out.extend(self._resolve_expr_to_task_ids(elt))
            return out
        return []

    def _resolve_args_to_task_ids(self, args: list[ast.AST]) -> list[str]:
        out: list[str] = []
        for arg in args:
            out.extend(self._resolve_expr_to_task_ids(arg))
        return out

    def _resolve_names_to_task_ids(self, names: list[str]) -> list[str]:
        out: list[str] = []
        for name in names:
            task_id = self.task_vars.get(name, name if name in self.task_meta else None)
            if task_id:
                out.append(task_id)
        return out


def _flatten_dep_expr(expr: ast.AST) -> list[str]:
    if isinstance(expr, ast.Name):
        return [expr.id]
    if isinstance(expr, ast.List):
        out: list[str] = []
        for elt in expr.elts:
            out.extend(_flatten_dep_expr(elt))
        return out
    if isinstance(expr, ast.BinOp) and isinstance(expr.op, (ast.RShift, ast.LShift)):
        return _flatten_dep_expr(expr.left) + _flatten_dep_expr(expr.right)
    return []
