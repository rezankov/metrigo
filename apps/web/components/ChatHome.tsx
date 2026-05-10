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
  id?: number;
  role: "user" | "ai";
  text: string;
  type?: string;
  chart?: {
    endpoint: string;
  };
  created_at?: string;
};

export function ChatHome({ summary }: { summary: TodaySummary }) {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [historyLoaded, setHistoryLoaded] = useState(false);
  const [loadingOlder, setLoadingOlder] = useState(false);
  const [hasOlder, setHasOlder] = useState(true);

  const chatRef = useRef<HTMLDivElement>(null);

  function mapHistoryMessage(message: {
    id: number;
    role: "user" | "assistant";
    content: string;
    created_at: string;
  }): Message {
    return {
      id: message.id,
      role: message.role === "assistant" ? "ai" : "user",
      text: message.content,
      type: "text",
      created_at: message.created_at,
    };
  }

  function formatMessageTime(value?: string) {
    if (!value) {
      return "";
    }

    return new Date(value).toLocaleString("ru-RU", {
      day: "2-digit",
      month: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    });
  }
 
  useEffect(() => {
    async function loadHistory() {
      try {
        const history = await getChatHistory(30);

        if (history.length > 0) {
          setMessages(history.map(mapHistoryMessage));
          setHasOlder(history.length === 30);

          requestAnimationFrame(() => {
            requestAnimationFrame(() => {
              if (chatRef.current) {
                chatRef.current.scrollTop = chatRef.current.scrollHeight;
              }
            });
          });
        } else {
          setMessages([
            {
              role: "ai",
              text: summary.summary_text,
              type: "text",
            },
          ]);
          setHasOlder(false);
        }
      } catch {
        setMessages([
          {
            role: "ai",
            text: summary.summary_text,
            type: "text",
          },
        ]);
        setHasOlder(false);
      } finally {
        setHistoryLoaded(true);
      }
    }

    loadHistory();
  }, [summary.summary_text]);

  async function loadOlderMessages() {
    if (loadingOlder || !hasOlder || messages.length === 0) {
      return;
    }

    const firstMessageWithId = messages.find((message) => message.id);

    if (!firstMessageWithId?.id) {
      setHasOlder(false);
      return;
    }

    const chatEl = chatRef.current;
    const oldScrollHeight = chatEl?.scrollHeight || 0;

    setLoadingOlder(true);

    try {
      const older = await getChatHistory(30, firstMessageWithId.id);

      if (older.length === 0) {
        setHasOlder(false);
        return;
      }

      setMessages((current) => [
        ...older.map(mapHistoryMessage),
        ...current,
      ]);

      setHasOlder(older.length === 30);

      setTimeout(() => {
        if (chatRef.current) {
          const newScrollHeight = chatRef.current.scrollHeight;
          chatRef.current.scrollTop = newScrollHeight - oldScrollHeight;
        }
      }, 0);
    } finally {
      setLoadingOlder(false);
    }
  }

  function handleScroll() {
    if (!chatRef.current) {
      return;
    }

    if (chatRef.current.scrollTop < 80) {
      loadOlderMessages();
    }
  }

  useEffect(() => {
    if (!loading && historyLoaded && chatRef.current) {
      chatRef.current.scrollTop = chatRef.current.scrollHeight;
    }
  }, [loading]);

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
        created_at: new Date().toISOString(),
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
          created_at: new Date().toISOString(),
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
          created_at: new Date().toISOString(),
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
      <section className="chat" ref={chatRef} onScroll={handleScroll}>
        {loadingOlder && (
          <div className="historyLoader">Загружаю старые сообщения…</div>
        )}

        {messages.map((message, index) => (
          <div
            key={message.id ? `msg-${message.id}` : `${message.role}-${index}`}
            className={`message ${message.role}`}
          >
            <ReactMarkdown>{message.text}</ReactMarkdown>
			
			{message.created_at && (
              <div className="messageTime">
                {formatMessageTime(message.created_at)}
              </div>
            )}

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