"""
tools.py — собирает все инструменты для ИИ

- Автоматически импортирует функции из файлов в папке tools/
- Извлекает docstring каждой функции
- Формирует:
  - TOOLS — список словарей для передачи ИИ
  - TOOLS_DICT — словарь name → функция для универсального вызова
"""

import os
import importlib.util
from typing import List, Dict, Callable

TOOLS_DIR = os.path.join(os.path.dirname(__file__), "tools")


def load_tools() -> List[Dict]:
    """
    Сканирует папку tools/, импортирует функции и собирает описание.
    """
    tools_list = []

    for filename in os.listdir(TOOLS_DIR):
        if not filename.endswith(".py") or filename in ("__init__.py",):
            continue

        module_name = filename[:-3]
        module_path = os.path.join(TOOLS_DIR, filename)

        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if not spec or not spec.loader:
            continue

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Ищем функцию с именем файла
        func: Callable = getattr(module, module_name, None)
        if not func:
            continue

        description = func.__doc__ or ""
        args = list(func.__code__.co_varnames[:func.__code__.co_argcount])

        tools_list.append({
            "name": module_name,
            "description": description.strip(),
            "args": args,
            "entrypoint": f"/tools/{module_name}",
        })

    return tools_list


# --- Список инструментов для ИИ ---
TOOLS = load_tools()

# --- Словарь name → функция для вызова через /tools/{tool_name} ---
TOOLS_DICT = {
    tool["name"]: getattr(
        __import__(f"app.tools.{tool['name']}", fromlist=[tool['name']]),
        tool["name"]
    )
    for tool in TOOLS
}