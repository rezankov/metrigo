"""
agent.py — AI-agent loop для Metrigo.

Логика:
1. Отправляем ИИ контекст, историю и вопрос.
2. ИИ может запросить инструмент через TOOL_CALL.
3. Backend выполняет инструмент.
4. Результат инструмента отправляется ИИ.
5. ИИ формирует финальный ответ.
"""

import json
import httpx
from app.tool_runner import parse_tool_call, run_tool


async def ask_llm(
    openrouter_key: str,
    model: str,
    messages: list[dict],
    timeout: float = 30.0,
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
    Выполнить один цикл общения с ИИ.

    Пока поддерживаем максимум 1 tool call за запрос.
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

    first_answer = await ask_llm(
        openrouter_key=openrouter_key,
        model=model,
        messages=messages,
    )

    tool_call = parse_tool_call(first_answer)

    if not tool_call:
        return first_answer

    args = tool_call["args"]

    # seller_id всегда контролирует backend, а не ИИ.
    args["seller_id"] = seller_id

    tool_result = run_tool(
        tool_name=tool_call["tool"],
        args=args,
    )

    messages.append(
        {
            "role": "assistant",
            "content": first_answer,
        }
    )

    messages.append(
        {
            "role": "user",
            "content": (
                "Результат инструмента:\n"
                f"{json.dumps(tool_result, ensure_ascii=False, indent=2)}\n\n"
                "Теперь дай финальный ответ пользователю. "
                "Не показывай TOOL_CALL. Ответь обычным Markdown."
            ),
        }
    )

    final_answer = await ask_llm(
        openrouter_key=openrouter_key,
        model=model,
        messages=messages,
    )

    return final_answer