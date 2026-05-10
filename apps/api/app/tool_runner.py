"""
tool_runner.py — безопасный запуск инструментов Metrigo AI.
"""

import json
from app.load_tools import TOOLS


def run_tool(tool_name: str, args: dict) -> dict:
    """
    Выполнить инструмент по имени.
    """

    func = TOOLS.get(tool_name)

    if not func:
        return {
            "ok": False,
            "error": f"Tool {tool_name} not found",
        }

    try:
        result = func(**args)

        return {
            "ok": True,
            "tool": tool_name,
            "args": args,
            "result": result,
        }

    except Exception as e:
        return {
            "ok": False,
            "tool": tool_name,
            "args": args,
            "error": str(e),
        }


def parse_tool_call(text: str) -> dict | None:
    """
    Найти TOOL_CALL в ответе ИИ.

    Формат:
    TOOL_CALL: {"tool":"get_stock_context","args":{"seller_id":"main","limit":5}}
    """

    marker = "TOOL_CALL:"

    if marker not in text:
        return None

    raw = text.split(marker, 1)[1].strip()

    try:
        data = json.loads(raw)
    except Exception:
        return None

    if not isinstance(data, dict):
        return None

    tool = data.get("tool")
    args = data.get("args", {})

    if not tool:
        return None

    if not isinstance(args, dict):
        args = {}

    return {
        "tool": tool,
        "args": args,
    }