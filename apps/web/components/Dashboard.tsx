"use client";

import { useEffect, useState } from "react";
import { TodaySummary, getTodaySummary } from "@/lib/api";
import { ChatHome } from "./ChatHome";

export default function Dashboard() {
  const [summary, setSummary] = useState<TodaySummary | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // --- Загружаем TodaySummary после монтирования ---
  useEffect(() => {
    async function fetchSummary() {
      setLoading(true);
      setError(null);
      try {
        const data = await getTodaySummary();
        setSummary(data);
      } catch (e: any) {
        setError("Ошибка загрузки данных: " + (e.message || e));
      } finally {
        setLoading(false);
      }
    }

    fetchSummary();
  }, []);

  if (loading) {
    return (
      <div style={{ padding: 20 }}>
        <p>Загрузка данных...</p>
      </div>
    );
  }

  if (error || !summary) {
    return (
      <div style={{ padding: 20, color: "red" }}>
        <p>{error || "Данные отсутствуют"}</p>
      </div>
    );
  }

  return (
    <div style={{ padding: 16 }}>
      <h1>Добро пожаловать в Metrigo</h1>
      <section style={{ marginBottom: 20 }}>
        <p>
          Сегодняшний контекст: {summary.sales_count} продаж,{" "}
          {summary.orders_count} заказов, выручка{" "}
          {summary.revenue.toLocaleString("ru-RU")} ₽, расход рекламы{" "}
          {summary.ad_spend.toLocaleString("ru-RU")} ₽
        </p>
      </section>

      {/* --- Основной чат с ИИ --- */}
      <ChatHome summary={summary} />
    </div>
  );
}