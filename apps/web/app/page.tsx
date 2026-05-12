"use client";

import { useEffect, useState } from "react";
import { ChatHome } from "@/components/ChatHome";
import {
  getHeaderMetrics,
  getSalesMiniChart,
  getTodaySummary,
  HeaderMetrics,
  SalesMiniChart,
  TodaySummary,
} from "@/lib/api";
import Dashboard from "@/components/Dashboard";

function formatRub(value: number) {
  return `${Math.round(value).toLocaleString("ru-RU")} ₽`;
}

function MiniSalesChart({ chart }: { chart: SalesMiniChart | null }) {
  if (!chart || chart.values.length === 0) {
    return (
      <section className="miniChartCard">
        <div className="miniChartEmpty">Нет данных</div>
      </section>
    );
  }

  const maxValue = chart.max_value || Math.max(...chart.values, 1);

  return (
    <section className="miniChartCard">
      {chart.values.map((value, index) => {
        const height = maxValue > 0 ? Math.max(4, (value / maxValue) * 100) : 4;

        return (
          <div
            key={`${chart.labels[index]}-${index}`}
            className="miniBar"
            title={`${chart.labels[index]}: ${Math.round(value).toLocaleString("ru-RU")} ₽`}
            style={{ height: `${height}%` }}
          />
        );
      })}
    </section>
  );
}

function formatCompactRub(value: number) {
  return `${Math.round(value).toLocaleString("ru-RU")} ₽`;
}

export default function Page() {
  const [summary, setSummary] = useState<TodaySummary | null>(null);
  const [salesChart, setSalesChart] = useState<SalesMiniChart | null>(null);
  const [error, setError] = useState(false);
  const [todayMetrics, setTodayMetrics] = useState<HeaderMetrics | null>(null);
  const [sevenDaysMetrics, setSevenDaysMetrics] = useState<HeaderMetrics | null>(null);
  const [view, setView] = useState<"chat" | "dashboard">("chat");

  useEffect(() => {
    Promise.all([
      getTodaySummary(),
      getSalesMiniChart(60),
      getHeaderMetrics(1),
      getHeaderMetrics(7),
    ])
      .then(([summaryData, chartData, todayData, sevenDaysData]) => {
        setSummary(summaryData);
        setSalesChart(chartData);
        setTodayMetrics(todayData);
        setSevenDaysMetrics(sevenDaysData);
      })
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
            <div className="brand logoOnly">
              <img src="/apple-touch-icon.png" alt="Metrigo" className="logoImage" />
            </div>

            <div className="headerMetric">
              <span>Сегодня</span>
              <strong>
                {todayMetrics
                  ? `${todayMetrics.orders_count} / ${todayMetrics.buyouts_count} / ${formatRub(todayMetrics.revenue)}`
                  : "—"}
              </strong>
            </div>

            <div className="headerMetric green">
              <span>7 дней</span>
              <strong>
                {sevenDaysMetrics
                  ? `${sevenDaysMetrics.orders_count} / ${sevenDaysMetrics.buyouts_count} / ${formatCompactRub(sevenDaysMetrics.revenue)}`
                  : "—"}
              </strong>
            </div>

            <div
              className="statusDotOnly"
              title={`Статус системы: ${summary.system_status.toUpperCase()}`}
            />
          </section>

          <MiniSalesChart chart={salesChart} />
		  
          <div className="viewTabs">
            <button
              className={view === "chat" ? "active" : ""}
              onClick={() => setView("chat")}
            >
              Чат
            </button>
            <button
              className={view === "dashboard" ? "active" : ""}
              onClick={() => setView("dashboard")}
            >
              Дашборд
            </button>
          </div>
        </header>

        {view === "chat" ? <ChatHome summary={summary} /> : <Dashboard />}
      </div>
    </main>
  );
}