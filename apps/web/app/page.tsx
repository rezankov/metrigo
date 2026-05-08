import { ChatHome } from "@/components/ChatHome";
import { getTodaySummary } from "@/lib/api";

function formatMoney(value: number) {
  return new Intl.NumberFormat("ru-RU", {
    maximumFractionDigits: 0,
  }).format(value);
}

export default async function HomePage() {
  const summary = await getTodaySummary();

  return (
    <main className="page">
      <section className="phone">
        <header className="topBar">
          <div>
            <div className="logo">Metrigo</div>
            <div className="subtitle">AI-кабинет WB-селлера</div>
          </div>

          <div className="status">
            {summary.system_status === "ok" ? "🟢 OK" : "🔴 ERROR"}
          </div>
        </header>

        <section className="metrics">
          <div className="metric">
            <span>Продажи</span>
            <strong>{summary.sales_count}</strong>
          </div>

          <div className="metric">
            <span>Выручка</span>
            <strong>{formatMoney(summary.revenue)} ₽</strong>
          </div>

          <div className="metric">
            <span>ДРР</span>
            <strong>{summary.drr}%</strong>
          </div>
        </section>

        <ChatHome summary={summary} />
      </section>
    </main>
  );
}