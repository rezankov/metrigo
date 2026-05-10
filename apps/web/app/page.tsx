"use client";

import { useEffect, useState } from "react";
import { ChatHome } from "@/components/ChatHome";
import { getTodaySummary, TodaySummary } from "@/lib/api";

function formatRub(value: number) {
  return `${Math.round(value).toLocaleString("ru-RU")} ₽`;
}

function MiniSalesChart() {
  const bars = [42, 48, 38, 50, 56, 64, 58, 46, 68, 62, 70, 64, 66, 72, 78];

  return (
    <section className="miniChartCard">
      {bars.map((height, index) => (
        <div
          key={index}
          className="miniBar"
          style={{ height: `${height}%` }}
        />
      ))}
    </section>
  );
}

export default function Page() {
  const [summary, setSummary] = useState<TodaySummary | null>(null);
  const [error, setError] = useState(false);

  useEffect(() => {
    getTodaySummary()
      .then(setSummary)
      .catch(() => setError(true));
  }, []);

  if (error) {
    return (
      <main className="page">
        <div className="phone">
          <div className="message ai">
            <p>Ошибка загрузки данных.</p>
          </div>
        </div>
      </main>
    );
  }

  if (!summary) {
    return (
      <main className="page">
        <div className="phone">
          <div className="message ai">
            <p>Загрузка Metrigo…</p>
          </div>
        </div>
      </main>
    );
  }

  return (
    <main className="page">
      <div className="phone">
        <header className="fixedHeader">
          <section className="headerTop">
            <div className="brand">
              <img
                src="/logo.svg"
                alt="Metrigo"
                className="logoImage"
              />

              <div className="brandText">
                <div className="logo">Metrigo</div>
                <div className="subtitle">AI-кабинет WB</div>
              </div>
            </div>

            <div className="headerMetric">
              <span>Сегодня</span>
              <strong>
                {summary.sales_count} / {summary.orders_count} /{" "}
                {formatRub(summary.revenue)}
              </strong>
            </div>

            <div className="headerMetric green">
              <span>7 дней</span>
              <strong>
                {summary.sales_count} / {summary.orders_count} /{" "}
                {formatRub(summary.revenue)}
              </strong>
            </div>

            <div
              className="statusDotOnly"
              title={`Статус системы: ${summary.system_status.toUpperCase()}`}
            />
          </section>

          <MiniSalesChart />
        </header>

        <ChatHome summary={summary} />
      </div>
    </main>
  );
}