"""
agent.py — AI-agent loop для Metrigo.

Логика:
1. Отправляем ИИ контекст, историю и вопрос.
2. ИИ может запросить TOOL_CALL.
3. Backend выполняет инструмент.
4. Результат инструмента возвращается ИИ.
5. ИИ может запросить следующий TOOL_CALL.
6. После завершения возвращается финальный ответ.
"""

import json
import httpx

from app.tool_runner import parse_tool_call, run_tool
from app.tool_log import save_tool_call


MAX_TOOL_STEPS = 5


async def ask_llm(
    openrouter_key: str,
    model: str,
    messages: list[dict],
    timeout: float = 60.0,
) -> str:
    """
    Отправить messages в OpenRouter и вернуть текст ответа.
    """

    headers = {
        "Authorization": f"Bearer {openrouter_key}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": model,
        "messages": messages,
    }

    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers=headers,
            json=payload,
        )

        resp.raise_for_status()

        result = resp.json()

        return result["choices"][0]["message"]["content"]


async def run_agent(
    openrouter_key: str,
    model: str,
    system_prompt: str,
    history_messages: list[dict],
    user_message: str,
    seller_id: str,
) -> str:
    """
    Запустить AI-agent loop с поддержкой нескольких TOOL_CALL.
    """

    messages = [
        {
            "role": "system",
            "content": system_prompt,
        }
    ]

    messages.extend(history_messages)

    messages.append(
        {
            "role": "user",
            "content": user_message,
        }
    )

    for step in range(MAX_TOOL_STEPS):

        answer = await ask_llm(
            openrouter_key=openrouter_key,
            model=model,
            messages=messages,
        )

        tool_call = parse_tool_call(answer)

        # Если TOOL_CALL нет — это финальный ответ.
        if not tool_call:
            return answer

        tool_name = tool_call["tool"]
        args = tool_call.get("args", {})

        # seller_id всегда контролирует backend.
        args["seller_id"] = seller_id

        try:
            tool_result = run_tool(
                tool_name=tool_name,
                args=args,
            )

            save_tool_call(
                seller_id=seller_id,
                tool_name=tool_name,
                args=args,
                result=tool_result,
            )

        except Exception as e:
            tool_result = {
                "error": str(e)
            }

        # Сохраняем TOOL_CALL как assistant message
        messages.append(
            {
                "role": "assistant",
                "content": answer,
            }
        )

        # Возвращаем результат инструмента модели
        messages.append(
            {
                "role": "user",
                "content": (
                    f"Результат инструмента {tool_name}:\n\n"
                    f"{json.dumps(tool_result, ensure_ascii=False, indent=2)}\n\n"
                    "Теперь:"
                    "\n- либо дай следующий TOOL_CALL"
                    "\n- либо дай финальный ответ пользователю"
                    "\n- TOOL_CALL не объясняй"
                    "\n- TOOL_CALL должен быть строго одной строкой"
                    "\n- финальный ответ пиши Markdown"
                ),
            }
        )

    return (
        "Не удалось завершить цепочку инструментов. "
        "Попробуйте переформулировать запрос."
    )