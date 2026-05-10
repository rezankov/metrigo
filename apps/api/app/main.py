"""
Metrigo API entrypoint.

- Универсальный: использует build_today_context для агрегаций и контекста
- Инструменты подключаются из load_tools.TOOLS_DICT
- POST /chat обрабатывает запросы через ChatGPT + инструменты
- POST /tools/{tool_name} — универсальный вызов инструмента с JSON-аргументами
"""

import os
import httpx
import json
from fastapi import Request, FastAPI
from pydantic import BaseModel
from app.build_today_context import build_today_context
from app.load_tools import TOOLS, TOOLS_DICT as TOOLS

# --- Конфиги ---
OPENROUTER_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "gpt-4o-mini")
SELLER_ID = os.getenv("DEFAULT_SELLER_ID", "main").strip() or "main"

app = FastAPI(title="Metrigo API")


# --- Модель запроса чата ---
class ChatRequest(BaseModel):
    message: str


# --- /chat endpoint ---
@app.post("/chat")
async def chat(request: ChatRequest):
    text = (request.message or "").strip()

    # --- Формируем контекст для ИИ ---
    try:
        context = build_today_context(seller_id=SELLER_ID)
    except Exception as e:
        return {"type": "text", "text": f"Ошибка соединения с API: {e}"}

    if not OPENROUTER_KEY:
        return {"type": "text", "text": "OpenRouter API ключ не задан"}

    # --- Системный промпт для ChatGPT ---
    tools_description = "\n".join([f"{name}: {func.__doc__ or ''}" for name, func in TOOLS.items()])
    system_prompt = f"""
Вы помощник продавца Wildberries. Используйте только предоставленные данные.
Доступные инструменты для анализа и получения метрик:
{tools_description}

Бизнес-контекст (build_today_context):
{json.dumps(context, ensure_ascii=False, indent=2)}

Отвечайте на вопросы пользователя корректно, используя инструменты по мере необходимости.
Если пользователь укажет конкретный SKU, добавьте его через get_sku_context.
"""

    # --- Запрос к OpenRouter (ChatGPT) ---
    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": text}
        ]
    }

    headers = {
        "Authorization": f"Bearer {OPENROUTER_KEY}",
        "Content-Type": "application/json"
    }

    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            resp = await client.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=headers,
                json=payload
            )
            resp.raise_for_status()
            result = resp.json()
            chat_text = result["choices"][0]["message"]["content"]
            return {
                "type": "text",
                "text": chat_text,
                "context": context,
                "available_tools": list(TOOLS.keys())
            }
        except Exception as e:
            return {"type": "text", "text": f"Ошибка соединения с ИИ: {str(e)}"}


# --- Универсальный вызов любого инструмента ---
@app.post("/tools/{tool_name}")
async def call_tool(tool_name: str, request: Request):
    """
    Вызов инструмента по имени файла/функции.
    Аргументы передаются JSON-объектом в теле запроса.
    """
    func = TOOLS.get(tool_name)
    if not func:
        return {"error": f"Tool {tool_name} not found"}

    try:
        body = await request.json()
        result = func(**body)
    except Exception as e:
        return {"error": f"Error calling tool {tool_name}: {e}"}

    return {"result": result}