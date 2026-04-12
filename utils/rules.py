from __future__ import annotations

import ast
import json
import os
import re
from functools import lru_cache
from typing import Any, Dict, Optional


DEFAULT_RULES: Dict[str, Any] = {
    "TIER_DEFINITIONS": {"value_percentile": 80, "volume_percentile": 80},
    "CONTEXTUAL_RULES": {
        "Estrela Digital": [
            {
                "condition": "satisfaction >= 4.0 and growth_rate < 100",
                "key_insight": "Manter investimento pesado para solidificar liderança.",
                "plano_capital": [
                    "Alocar budget de MKT e estoque para acompanhar o crescimento."
                ],
                "plano_operacional": [
                    "Revisar margem de lucro. O volume pode estar mascarando ineficiências de custo."
                ],
                "plano_mercado": [
                    "Defender Market Share. Mapear 2 principais concorrentes."
                ],
            }
        ],
        "Vaca Leiteira": [
            {
                "condition": "growth_rate <= 0",
                "key_insight": "Otimizar margem e eficiência operacional para manter lucratividade.",
                "plano_capital": [
                    "Redirecionar parte do budget para iniciativas de margem."
                ],
                "plano_operacional": [
                    "Lean ops e revisão de mix de SKUs de baixo giro."
                ],
                "plano_mercado": [
                    "Defender share com ofertas táticas."
                ],
            }
        ],
        "Interrogação": [
            {
                "condition": "growth_rate > 0",
                "key_insight": "Validar rapidamente PMF com testes controlados.",
                "plano_capital": [
                    "Micro-budget incremental atrelado a metas semanais de retenção."
                ],
                "plano_operacional": [
                    "Pilotos de fulfillment rápido em 1-2 regiões."
                ],
                "plano_mercado": [
                    "Campanhas de experimentação com foco em LTV."
                ],
            }
        ],
        "Abacaxi": [
            {
                "condition": "composite_score < 0.2",
                "key_insight": "Desinvestir gradualmente e realocar capital.",
                "plano_capital": [
                    "Cortar investimentos de MKT e estoque; liquidar inventário."
                ],
                "plano_operacional": [
                    "Encerrar SKUs com baixa margem e alto cancelamento."
                ],
                "plano_mercado": [
                    "Comunicação de liquidação e saída planejada."
                ],
            }
        ],
    },
}


@lru_cache(maxsize=4)
def load_strategic_rules(path: str = "config/strategic_rules.json") -> Dict[str, Any]:
    """Load strategic rules JSON with safe defaults and light caching."""
    try:
        if not os.path.exists(path):
            return DEFAULT_RULES
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return _validate_rules(data)
    except Exception:
        # Fallback to defaults on any error
        return DEFAULT_RULES


def _validate_rules(rules: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure minimal structure and defaults for missing keys."""
    if not isinstance(rules, dict):
        return DEFAULT_RULES
    tier_defs = rules.get("TIER_DEFINITIONS") or {}
    ctx_rules = rules.get("CONTEXTUAL_RULES") or {}
    value_p = int(tier_defs.get("value_percentile", 80))
    volume_p = int(tier_defs.get("volume_percentile", 80))
    cleaned = {
        "TIER_DEFINITIONS": {
            "value_percentile": value_p,
            "volume_percentile": volume_p,
        },
        "CONTEXTUAL_RULES": ctx_rules if isinstance(ctx_rules, dict) else {},
    }
    if not cleaned["CONTEXTUAL_RULES"]:
        cleaned["CONTEXTUAL_RULES"] = DEFAULT_RULES["CONTEXTUAL_RULES"]
    return cleaned


class _SafeEvaluator(ast.NodeVisitor):
    """Evaluate a boolean expression safely over a context dict.

    Supported:
      - Boolean ops: and/or/not
      - Comparisons: <, <=, >, >=, ==, !=
      - Arithmetic: +, -, *, /, %
      - Names resolved from provided context only
      - Literals (numbers, strings, True/False)
    """

    ALLOWED_BINOPS = (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Mod)
    ALLOWED_CMPOPS = (ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE)
    ALLOWED_BOOL = (ast.And, ast.Or)

    def __init__(self, context: Dict[str, Any]):
        self.context = context

    def visit(self, node):  # type: ignore[override]
        method = "visit_" + node.__class__.__name__
        visitor = getattr(self, method, None)
        if visitor is None:
            raise ValueError(f"Expressão não permitida: {node.__class__.__name__}")
        return visitor(node)

    def visit_Module(self, node: ast.Module):  # Python >=3.8 wraps expr in Module
        if len(node.body) != 1 or not isinstance(node.body[0], ast.Expr):
            raise ValueError("Expressão inválida")
        return self.visit(node.body[0])

    def visit_Expr(self, node: ast.Expr):
        return self.visit(node.value)

    def visit_BoolOp(self, node: ast.BoolOp):
        if not isinstance(node.op, self.ALLOWED_BOOL):
            raise ValueError("Operador booleano não permitido")
        values = [self.visit(v) for v in node.values]
        if isinstance(node.op, ast.And):
            result = True
            for v in values:
                result = result and bool(v)
                if not result:
                    break
            return result
        else:  # Or
            result = False
            for v in values:
                result = result or bool(v)
                if result:
                    break
            return result

    def visit_UnaryOp(self, node: ast.UnaryOp):
        if isinstance(node.op, ast.Not):
            return not bool(self.visit(node.operand))
        if isinstance(node.op, ast.USub):
            return -self.visit(node.operand)
        if isinstance(node.op, ast.UAdd):
            return +self.visit(node.operand)
        raise ValueError("Operador unário não permitido")

    def visit_Compare(self, node: ast.Compare):
        left = self.visit(node.left)
        for op, comparator in zip(node.ops, node.comparators):
            right = self.visit(comparator)
            if not isinstance(op, self.ALLOWED_CMPOPS):
                raise ValueError("Operador de comparação não permitido")
            if isinstance(op, ast.Eq):
                ok = left == right
            elif isinstance(op, ast.NotEq):
                ok = left != right
            elif isinstance(op, ast.Lt):
                ok = left < right
            elif isinstance(op, ast.LtE):
                ok = left <= right
            elif isinstance(op, ast.Gt):
                ok = left > right
            elif isinstance(op, ast.GtE):
                ok = left >= right
            else:
                ok = False
            if not ok:
                return False
            left = right
        return True

    def visit_BinOp(self, node: ast.BinOp):
        if not isinstance(node.op, self.ALLOWED_BINOPS):
            raise ValueError("Operador aritmético não permitido")
        left = self.visit(node.left)
        right = self.visit(node.right)
        if isinstance(node.op, ast.Add):
            return left + right
        if isinstance(node.op, ast.Sub):
            return left - right
        if isinstance(node.op, ast.Mult):
            return left * right
        if isinstance(node.op, ast.Div):
            return left / right
        if isinstance(node.op, ast.Mod):
            return left % right
        raise ValueError("Operador não suportado")

    def visit_Name(self, node: ast.Name):
        if node.id not in self.context:
            # names ausentes tratam como False/0
            return False
        return self.context[node.id]

    def visit_Constant(self, node: ast.Constant):
        return node.value

    # Compat: Python <3.8 usa Num/Str/NameConstant
    def visit_NameConstant(self, node):  # type: ignore[override]
        return node.value

    def visit_Num(self, node):  # type: ignore[override]
        return node.n

    def visit_Str(self, node):  # type: ignore[override]
        return node.s


def evaluate_condition(expr: str, context: Dict[str, Any]) -> bool:
    """Safely evaluate a boolean expression over the given context.

    Converts case-insensitive 'true'/'false' to Python booleans.
    Returns False if expression is invalid.
    """
    try:
        expr_norm = re.sub(r"\btrue\b", "True", expr, flags=re.IGNORECASE)
        expr_norm = re.sub(r"\bfalse\b", "False", expr_norm, flags=re.IGNORECASE)
        tree = ast.parse(expr_norm, mode="exec")
        evaluator = _SafeEvaluator(context)
        return bool(evaluator.visit(tree))
    except Exception:
        return False


def select_prescriptive_rule(quadrant: str, context: Dict[str, Any], rules: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """Return first matching rule for quadrant given a context, or None.

    The returned dict is the rule itself (contains key_insight and plan arrays).
    """
    all_rules = rules or load_strategic_rules()
    quad_rules = (all_rules.get("CONTEXTUAL_RULES") or {}).get(quadrant) or []
    for rule in quad_rules:
        cond = str(rule.get("condition", ""))
        if not cond:
            continue
        if evaluate_condition(cond, context):
            return rule
    return None



