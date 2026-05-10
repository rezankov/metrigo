"""
load_tools.py — единый реестр инструментов Metrigo AI.

Файл:
- автоматически импортирует функции из app/tools/*.py
- собирает описание инструментов для ИИ
- хранит словарь функций для backend-вызова
- готовит базу для будущего tool loop
"""

import os
import importlib.util
from typing import Any, Callable, Dict, List

TOOLS_DIR = os.path.join(os.path.dirname(__file__), "tools")


def _load_tool_function(module_name: str, module_path: str) -> Callable | None:
    """
    Импортировать tool-файл и вернуть функцию с именем файла.
    """

    spec = importlib.util.spec_from_file_location(module_name, module_path)

    if not spec or not spec.loader:
        return None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    func = getattr(module, module_name, None)

    if not callable(func):
        return None

    return func


def load_tools_registry() -> Dict[str, Dict[str, Any]]:
    """
    Собрать единый реестр инструментов.

    Формат:
    {
        "tool_name": {
            "func": callable,
            "name": "tool_name",
            "description": "...",
            "args": ["seller_id", "sku"],
            "entrypoint": "/tools/tool_name"
        }
    }
    """

    registry: Dict[str, Dict[str, Any]] = {}

    for filename in sorted(os.listdir(TOOLS_DIR)):
        if not filename.endswith(".py") or filename == "__init__.py":
            continue

        tool_name = filename[:-3]
        module_path = os.path.join(TOOLS_DIR, filename)

        func = _load_tool_function(
            module_name=tool_name,
            module_path=module_path,
        )

        if not func:
            continue

        args = list(func.__code__.co_varnames[: func.__code__.co_argcount])

        registry[tool_name] = {
            "func": func,
            "name": tool_name,
            "description": (func.__doc__ or "").strip(),
            "args": args,
            "entrypoint": f"/tools/{tool_name}",
        }

    return registry


TOOLS_REGISTRY = load_tools_registry()

# Для вызова инструментов в backend.
TOOLS = {
    name: item["func"]
    for name, item in TOOLS_REGISTRY.items()
}

# Для передачи ИИ в prompt/context без Python-функций.
ALL_TOOLS = [
    {
        "name": item["name"],
        "description": item["description"],
        "args": item["args"],
        "entrypoint": item["entrypoint"],
    }
    for item in TOOLS_REGISTRY.values()
]