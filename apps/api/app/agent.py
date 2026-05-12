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

        return result["choices"][0]["message"]["content"] or ""


def _tool_call_key(tool_name: str, args: dict) -> str:
    """
    Создать стабильный ключ tool-call для защиты от повторов.
    """

    return json.dumps(
        {
            "tool": tool_name,
            "args": args,
        },
        ensure_ascii=False,
        sort_keys=True,
    )


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

    Важно:
    - seller_id всегда подставляет backend
    - пользователь не видит сырые TOOL_CALL
    - повтор одинакового tool-call останавливается
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

    seen_tool_calls: set[str] = set()

    for step in range(MAX_TOOL_STEPS):
        answer = await ask_llm(
            openrouter_key=openrouter_key,
            model=model,
            messages=messages,
        )

        tool_call = parse_tool_call(answer)

        if not tool_call:
            return answer

        tool_name = tool_call.get("tool")
        args = tool_call.get("args") or {}

        if not tool_name:
            return (
                "Не удалось разобрать запрос инструмента. "
                "Попробуйте переформулировать вопрос."
            )

        args["seller_id"] = seller_id

        call_key = _tool_call_key(tool_name=tool_name, args=args)

        if call_key in seen_tool_calls:
            messages.append(
                {
                    "role": "user",
                    "content": (
                        "Ты повторно запросил тот же инструмент с теми же аргументами. "
                        "Не вызывай его снова. Сформируй финальный ответ на основе уже полученных данных."
                    ),
                }
            )

            final_answer = await ask_llm(
                openrouter_key=openrouter_key,
                model=model,
                messages=messages,
            )

            return final_answer

        seen_tool_calls.add(call_key)

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
                "error": str(e),
            }

        messages.append(
            {
                "role": "assistant",
                "content": answer,
            }
        )

        messages.append(
            {
                "role": "user",
                "content": (
                    f"Результат инструмента {tool_name}:\n\n"
                    f"{json.dumps(tool_result, ensure_ascii=False, indent=2)}\n\n"
                    "Продолжай рассуждение.\n"
                    "Если нужны ещё данные — верни следующий TOOL_CALL строго одной строкой.\n"
                    "Если данных достаточно — дай финальный ответ пользователю Markdown.\n"
                    "Не показывай пользователю TOOL_CALL в финальном ответе."
                ),
            }
        )

    return (
        "Я выполнил несколько шагов анализа, но не смог завершить ответ за лимит инструментов. "
        "Попробуйте сузить вопрос."
    )