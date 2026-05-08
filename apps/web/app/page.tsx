export default function HomePage() {
  return (
    <main className="page">
      <section className="phone">
        <header className="topBar">
          <div>
            <div className="logo">Metrigo</div>
            <div className="subtitle">AI-кабинет WB-селлера</div>
          </div>
          <div className="status">🟢 OK</div>
        </header>

        <section className="metrics">
          <div className="metric">
            <span>Продажи</span>
            <strong>22</strong>
          </div>
          <div className="metric">
            <span>Выручка</span>
            <strong>26 430 ₽</strong>
          </div>
          <div className="metric">
            <span>ДРР</span>
            <strong>0.64%</strong>
          </div>
        </section>

        <section className="chat">
          <div className="message ai">
            <p>
              Доброе утро. Продажи вчера были стабильными: 22 продажи,
              выручка 26 430 ₽. Реклама работает спокойно, ДРР 0.64%.
            </p>
          </div>

          <div className="message user">
            <p>Что сегодня важно проверить?</p>
          </div>

          <div className="message ai">
            <p>
              Я бы посмотрел остатки по лидерам продаж и эффективность рекламы
              по SKU bg-org-8-beige.
            </p>
          </div>
        </section>

        <section className="chips">
          <button>Почему просели продажи?</button>
          <button>Остатки</button>
          <button>Реклама</button>
          <button>Что заказать?</button>
        </section>

        <footer className="inputBar">
          <input placeholder="Спросите Metrigo…" />
          <button>↑</button>
        </footer>
      </section>
    </main>
  );
}