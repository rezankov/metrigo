"""
Metrigo API entrypoint.

- Универсальный: использует build_today_context для агрегаций и контекста
- Инструменты подключаются из load_tools.TOOLS
- POST /chat обрабатывает запросы через ChatGPT + инструменты
- POST /tools/{tool_name} — универсальный вызов инструмента с JSON-аргументами
"""

import os
import json
from fastapi import Request, FastAPI
from pydantic import BaseModel
from app.build_today_context import build_today_context
from app.load_tools import TOOLS
from app.chat_store import (
    get_or_create_thread,
    save_message,
    get_last_messages,
)
from app.agent import run_agent

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

    if not text:
        return {"type": "text", "text": "Введите сообщение."}

    try:
        context = build_today_context(seller_id=SELLER_ID)
    except Exception as e:
        return {"type": "text", "text": f"Ошибка соединения с API: {e}"}

    if not OPENROUTER_KEY:
        return {"type": "text", "text": "OpenRouter API ключ не задан"}

    thread_id = get_or_create_thread(SELLER_ID)

    history = get_last_messages(
        thread_id=thread_id,
        limit=20,
    )

    save_message(
        thread_id=thread_id,
        seller_id=SELLER_ID,
        role="user",
        content=text,
    )

    tools_description = "\n".join(
        [f"{name}: {func.__doc__ or ''}" for name, func in TOOLS.items()]
    )

    system_prompt = f"""
Вы помощник продавца Wildberries. Используйте только предоставленные данные.

Доступные инструменты:
{tools_description}

Бизнес-контекст:
{json.dumps(context, ensure_ascii=False, indent=2)}

Если для ответа нужны дополнительные данные, верни строго одну строку в формате:
TOOL_CALL: {{"tool":"get_stock_context","args":{{"limit":5}}}}

Правила TOOL_CALL:
- Не добавляй пояснений рядом с TOOL_CALL.
- seller_id не указывай, backend добавит его сам.
- Если данных уже хватает, отвечай обычным Markdown.
- Не придумывай данные, которых нет в контексте или результате инструмента.
"""

    try:
        chat_text = await run_agent(
            openrouter_key=OPENROUTER_KEY,
            model=OPENROUTER_MODEL,
            system_prompt=system_prompt,
            history_messages=[
                {
                    "role": msg["role"],
                    "content": msg["content"],
                }
                for msg in history
                if msg["role"] in ("user", "assistant")
            ],
            user_message=text,
            seller_id=SELLER_ID,
        )

        save_message(
            thread_id=thread_id,
            seller_id=SELLER_ID,
            role="assistant",
            content=chat_text,
        )

        return {
            "type": "text",
            "text": chat_text,
            "context": context,
            "available_tools": list(TOOLS.keys()),
        }

    except Exception as e:
        return {
            "type": "text",
            "text": f"Ошибка соединения с ИИ: {str(e)}",
        }


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


@app.get("/chat/history")
async def chat_history(limit: int = 30):
    """
    Получить последние сообщения чата.
    """

    thread_id = get_or_create_thread(SELLER_ID)

    messages = get_last_messages(
        thread_id=thread_id,
        limit=limit,
    )

    return {
        "messages": messages
    }