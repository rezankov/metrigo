"use client";

import { useEffect, useRef, useState } from "react";
import {
  getChatHistory,
  sendChatMessage,
  TodaySummary,
} from "@/lib/api";
import ReactMarkdown from "react-markdown";
import ChatChart from "./ChatChart";

type Message = {
  role: "user" | "ai";
  text: string;
  type?: string;
  chart?: {
    endpoint: string;
  };
};

export function ChatHome({ summary }: { summary: TodaySummary }) {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [historyLoaded, setHistoryLoaded] = useState(false);

  const chatRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    async function loadHistory() {
      try {
        const history = await getChatHistory(30);

        if (history.length > 0) {
          setMessages(
            history.map((message) => ({
              role: message.role === "assistant" ? "ai" : "user",
              text: message.content,
              type: "text",
            })),
          );
        } else {
          setMessages([
            {
              role: "ai",
              text: summary.summary_text,
              type: "text",
            },
          ]);
        }
      } catch {
        setMessages([
          {
            role: "ai",
            text: summary.summary_text,
            type: "text",
          },
        ]);
      } finally {
        setHistoryLoaded(true);
      }
    }

    loadHistory();
  }, [summary.summary_text]);

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
        type: "text",
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
          type: response.type || "text",
          ...(response.chart ? { chart: response.chart } : {}),
        },
      ]);
    } catch {
      setMessages((current) => [
        ...current,
        {
          role: "ai",
          text: "Ошибка соединения с API.",
          type: "text",
        },
      ]);
    } finally {
      setLoading(false);
    }
  }

  if (!historyLoaded) {
    return (
      <>
        <section className="chat" ref={chatRef}>
          <div className="message ai loading">
            <p>Загружаю историю чата…</p>
          </div>
        </section>

        <footer className="inputBar">
          <input disabled placeholder="Загрузка…" />
          <button disabled>↑</button>
        </footer>
      </>
    );
  }

  return (
    <>
      <section className="chat" ref={chatRef}>
        {messages.map((message, index) => (
          <div
            key={`${message.role}-${index}`}
            className={`message ${message.role}`}
          >
            <ReactMarkdown>{message.text}</ReactMarkdown>

            {message.type === "chart" && message.chart && (
              <ChatChart endpoint={message.chart.endpoint} />
            )}
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
          onChange={(event) => setInput(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter") {
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