"use client";

import { useEffect, useRef, useState } from "react";

import { sendChatMessage, TodaySummary } from "@/lib/api";

type Message = {
  role: "ai" | "user";
  text: string;
};

export function ChatHome({ summary }: { summary: TodaySummary }) {
  const [messages, setMessages] = useState<Message[]>([
    {
      role: "ai",
      text: summary.summary_text,
    },
  ]);

  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);

  const chatRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (chatRef.current) {
      chatRef.current.scrollTop = chatRef.current.scrollHeight;
    }
  }, [messages, loading]);

  async function ask(text: string) {
    const clean = text.trim();

    if (!clean || loading) {
      return;
    }

    setMessages((current) => [
      ...current,
      {
        role: "user",
        text: clean,
      },
    ]);

    setInput("");
    setLoading(true);

    try {
      const response = await sendChatMessage(clean);

      setMessages((current) => [
        ...current,
        {
          role: "ai",
          text: response.text,
        },
      ]);
    } catch {
      setMessages((current) => [
        ...current,
        {
          role: "ai",
          text: "Ошибка соединения с API.",
        },
      ]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <>
      <section className="chat" ref={chatRef}>
        {messages.map((message, index) => (
          <div
            key={`${message.role}-${index}`}
            className={`message ${message.role}`}
          >
            <p>{message.text}</p>
          </div>
        ))}

        {loading && (
          <div className="message ai loading">
            <p>Metrigo думает…</p>
          </div>
        )}
      </section>

      <section className="chips">
        {summary.suggested_actions.map((action) => (
          <button
            key={action}
            onClick={() => ask(action)}
            disabled={loading}
          >
            {action}
          </button>
        ))}
      </section>

      <footer className="inputBar">
        <input
          value={input}
          placeholder="Спросите Metrigo…"
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") {
              ask(input);
            }
          }}
        />

        <button
          onClick={() => ask(input)}
          disabled={loading}
        >
          ↑
        </button>
      </footer>
    </>
  );
}