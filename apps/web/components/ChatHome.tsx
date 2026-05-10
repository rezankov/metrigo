"use client";

import { useEffect, useRef, useState } from "react";
import { sendChatMessage, TodaySummary } from "@/lib/api";
import ReactMarkdown from "react-markdown";
import ChatChart from "./ChatChart";

type Message = { role: "user" | "ai"; text: string; type?: string; chart?: { endpoint: string } };

export function ChatHome({ summary }: { summary: TodaySummary }) {
  const [messages, setMessages] = useState<Message[]>([{ role: "ai", text: summary.summary_text }]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const chatRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (chatRef.current) chatRef.current.scrollTop = chatRef.current.scrollHeight;
  }, [messages, loading]);

  async function ask(text: string) {
    const clean = text.trim();
    if (!clean || loading) return;
    setMessages(prev => [...prev, { role: "user", text: clean }]);
    setInput(""); setLoading(true);

    try {
      const response = await sendChatMessage(clean);
      setMessages(prev => [
        ...prev,
        {
          role: "ai",
          text: response.text,
          type: response.type || "text",
          ...(response.chart ? { chart: response.chart } : {}),
        },
      ]);
    } catch {
      setMessages(prev => [...prev, { role: "ai", text: "Ошибка соединения с API." }]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <>
      <section className="chat" ref={chatRef}>
        {messages.map((m, i) => (
          <div key={i} className={`message ${m.role}`}>
            <ReactMarkdown>{m.text}</ReactMarkdown>
            {m.type === "chart" && m.chart && <ChatChart endpoint={m.chart.endpoint} />}
          </div>
        ))}
        {loading && <div className="message ai loading"><p>Metrigo думает…</p></div>}
      </section>

      <section className="chips">
        {summary.suggested_actions.map(a => <button key={a} onClick={() => ask(a)} disabled={loading}>{a}</button>)}
      </section>

      <footer className="inputBar">
        <input value={input} placeholder="Спросите Metrigo…" onChange={e => setInput(e.target.value)} onKeyDown={e => { if (e.key === "Enter") ask(input); }} />
        <button onClick={() => ask(input)} disabled={loading}>↑</button>
      </footer>
    </>
  );
}