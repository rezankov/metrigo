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
from app.business_context import BUSINESS_CONTEXT
from app.dashboard.sku_list import get_sku_list
from app.dashboard.sku_detail import get_sku_detail
from app.dashboard.shop_profit import get_shop_profit
from app.dashboard import monthly_profit

# --- Конфиги ---
OPENROUTER_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "gpt-4o-mini")
SELLER_ID = os.getenv("DEFAULT_SELLER_ID", "main").strip() or "main"

app = FastAPI(title="Metrigo API")

app.include_router(monthly_profit.router, prefix="/dashboard")

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
Ты AI-оператор бизнеса Wildberries внутри системы Metrigo.

Твоя задача:
- анализировать бизнес
- помогать принимать решения
- искать проблемы
- анализировать рекламу
- контролировать остатки
- анализировать продажи
- помогать владельцу бизнеса

Ты работаешь ТОЛЬКО через реальные данные и инструменты.

=====================
БИЗНЕС-КОНТЕКСТ
=====================

{BUSINESS_CONTEXT}

=====================
АКТУАЛЬНЫЕ ДАННЫЕ
=====================

{json.dumps(context, ensure_ascii=False, indent=2)}

=====================
ДОСТУПНЫЕ ИНСТРУМЕНТЫ
=====================

{tools_description}

=====================
ПРАВИЛА TOOL_CALL
=====================

Если нужны дополнительные данные:

Верни СТРОГО ОДНУ строку:

TOOL_CALL: {{"tool":"tool_name","args":{{}}}}

Пример:

TOOL_CALL: {{"tool":"get_stock_context","args":{{"limit":5}}}}

=====================
КАК ВЫБИРАТЬ ИНСТРУМЕНТЫ
=====================

Если пользователь просит:
- общий обзор бизнеса
- ключевые показатели
- состояние бизнеса
- прибыльность
- проблемы
- что важно сейчас
- рекомендации
- анализ ситуации
- анализ бизнеса

сначала используй:

TOOL_CALL: {{"tool":"get_ai_business_insights","args":{{"days":7}}}}

Если нужны:
- остатки
- прибыль
- продажи
- реклама
- snapshot бизнеса

используй:

TOOL_CALL: {{"tool":"get_full_business_snapshot","args":{{"days":7}}}}

Если пользователь спрашивает про конкретный SKU:

TOOL_CALL: {{"tool":"get_sku_context","args":{{"sku":"SKU"}}}}

=====================
ПРАВИЛА АНАЛИЗА
=====================

Правила:
- никаких пояснений рядом с TOOL_CALL
- только одна строка TOOL_CALL
- seller_id не указывать
- сначала собирай данные инструментами
- потом формируй выводы
- не придумывай цифры
- не фантазируй
- если данных нет — так и скажи
- финальный ответ всегда Markdown

=====================
ЖЕСТКИЕ ОГРАНИЧЕНИЯ
=====================

Запрещено:
- придумывать конкурентов
- говорить о рынке без данных
- говорить о спросе без данных
- советовать ассортимент без данных
- делать выводы, которых нет в инструментах
- говорить о внешних причинах падения продаж без данных
- говорить "всё хорошо", если есть warnings
- сглаживать риски
- скрывать проблемы

Если данных нет:
- прямо говори, что данных нет
- НЕ додумывай

- не упоминать конкурентов, рынок, спрос и предпочтения клиентов без данных из инструментов
- не называть SKU критичным, если days_cover >= 15
- не называть SKU низким остатком, если days_cover >= 45
- не пересчитывать net_profit вручную, если инструмент уже вернул net_profit
- не писать формулу чистой прибыли как revenue - расходы, если расчет идет по wb_for_pay

=====================
ПРАВИЛА ИНТЕРПРЕТАЦИИ
=====================

Если get_ai_business_insights возвращает warnings:
- обязательно отрази их в ответе

Если margin_percent < 10:
- называй это низкой маржой
- это зона внимания бизнеса

Если ad_percent > 8:
- говори, что рекламу нужно контролировать

Если ad_percent > 15:
- говори, что реклама опасно высокая

Если stock days_cover < 25:
- явно говори, что нужен контроль поставки

Если stock days_cover < 15:
- говори о риске дефицита товара

Если stock_qty = 0:
- говори, что товар закончился

Если есть warnings:
- не называй бизнес стабильным
- не называй бизнес "в хорошей форме"

Если инструмент вернул net_profit:
- используй net_profit как готовое значение
- не пытайся пересчитать его вручную
- объясняй, что расчет основан на выплатах WB, рекламе, налоге и COGS

=====================
СТИЛЬ ОТВЕТА
=====================

Отвечай:
- кратко
- структурно
- по делу
- как операционный аналитик бизнеса

Используй:
- Markdown
- списки
- акценты
- конкретные цифры

Не используй:
- воду
- общие фразы
- мотивационные формулировки
- абстрактные советы
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
async def chat_history(
    limit: int = 30,
    before_id: int | None = None,
):
    """
    Получить сообщения чата.

    По умолчанию — последние limit сообщений.
    Если before_id указан — сообщения старше before_id.
    """

    thread_id = get_or_create_thread(SELLER_ID)

    messages = get_last_messages(
        thread_id=thread_id,
        limit=limit,
        before_id=before_id,
    )

    return {
        "messages": messages
    }


@app.post("/dashboard/sku_list")
async def dashboard_sku_list(request: Request):
    """
    Dashboard:
    список SKU с остатками и оборотом.
    """

    body = await request.json()

    seller_id = body.get("seller_id", SELLER_ID)
    days = int(body.get("days", 30))

    result = get_sku_list(
        seller_id=seller_id,
        days=days,
    )

    return {
        "result": result
    }


@app.post("/dashboard/shop_profit")
async def dashboard_shop_profit(payload: dict):
    seller_id = payload.get("seller_id") or DEFAULT_SELLER_ID

    return {
        "result": get_shop_profit(seller_id=seller_id)
    }


@app.post("/dashboard/sku_detail")
async def dashboard_sku_detail(request: Request):
    body = await request.json()

    seller_id = body.get("seller_id", SELLER_ID)
    sku = body.get("sku")
    days = int(body.get("days", 30))

    if not sku:
        return {"error": "sku is required"}

    return {
        "result": get_sku_detail(
            seller_id=seller_id,
            sku=sku,
            days=days,
        )
    }