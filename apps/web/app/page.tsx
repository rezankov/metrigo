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
            <div className="subtitle">
              AI-кабинет WB-селлера
            </div>
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

        <section className="chat">
          <div className="message ai">
            <p>{summary.summary_text}</p>
          </div>

          <div className="message user">
            <p>Что сегодня важно проверить?</p>
          </div>

          <div className="message ai">
            <p>
              Я бы посмотрел остатки лидеров продаж
              и эффективность рекламы по SKU.
            </p>
          </div>
        </section>

        <section className="chips">
          {summary.suggested_actions.map((action) => (
              <button key={action}>{action}</button>
))}
        </section>

        <footer className="inputBar">
          <input placeholder="Спросите Metrigo…" />
          <button>↑</button>
        </footer>
      </section>
    </main>
  );
}