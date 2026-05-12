"use client";

import { useEffect, useMemo, useState } from "react";
import { DashboardSkuItem, DashboardSkuList, getDashboardSkuList } from "@/lib/api";

type SortKey = "turnover" | "cover" | "margin";

function formatRub(value: number) {
  return `${Math.round(value).toLocaleString("ru-RU")} ₽`;
}

function getCoverClass(daysCover: number) {
  if (daysCover < 25) return "critical";
  if (daysCover < 45) return "warning";
  return "ok";
}

function SkuRow({ item }: { item: DashboardSkuItem }) {
  const coverClass = getCoverClass(item.days_cover);

  return (
    <button className="skuRow compact" type="button">
      <div className="skuMain">
        <div>
          <strong>{item.sku}</strong>
          <span className="skuMargin">маржа ~{item.margin_percent.toFixed(1)}%</span>
        </div>
        <span>{formatRub(item.turnover_30d)}</span>
      </div>

      <div className="skuMetrics compact">
        <div>
          <span>Остаток</span>
          <strong>{item.stock_qty}</strong>
        </div>

        <div>
          <span>Покрытие</span>
          <strong className={`cover ${coverClass}`}>
            {item.days_cover.toFixed(1)} дн
          </strong>
        </div>

        <div>
          <span>Цена</span>
          <strong>{formatRub(item.current_price)}</strong>
        </div>
      </div>
    </button>
  );
}

export default function Dashboard() {
  const [data, setData] = useState<DashboardSkuList | null>(null);
  const [sortKey, setSortKey] = useState<SortKey>("turnover");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  useEffect(() => {
    getDashboardSkuList(30)
      .then(setData)
      .catch(() => setError(true))
      .finally(() => setLoading(false));
  }, []);

  const sortedItems = useMemo(() => {
    const items = [...(data?.items || [])];

    switch (sortKey) {
      case "turnover":
        return items.sort((a, b) => b.turnover_30d - a.turnover_30d);
      case "cover":
        return items.sort((a, b) => a.days_cover - b.days_cover);
      case "margin":
        return items.sort((a, b) => b.margin_percent - a.margin_percent);
    }
  }, [data, sortKey]);

  if (loading) {
    return (
      <section className="dashboard">
        <div className="dashboardCard">Загружаю дашборд…</div>
      </section>
    );
  }

  if (error || !data) {
    return (
      <section className="dashboard">
        <div className="dashboardCard">Ошибка загрузки дашборда.</div>
      </section>
    );
  }

  return (
    <section className="dashboard">
      <div className="dashboardTitle compact">
        <div>
          <h1>SKU</h1>
          <p>Остатки, покрытие, цена, маржа</p>
        </div>
        <span>{data.items.length} SKU</span>
      </div>

      <div className="sortTabs">
        <button className={sortKey === "turnover" ? "active" : ""} onClick={() => setSortKey("turnover")}>Оборот</button>
        <button className={sortKey === "cover" ? "active" : ""} onClick={() => setSortKey("cover")}>Покрытие</button>
        <button className={sortKey === "margin" ? "active" : ""} onClick={() => setSortKey("margin")}>Маржа</button>
      </div>

      <div className="skuList compact">
        {sortedItems.map((item) => (
          <SkuRow key={item.sku} item={item} />
        ))}
      </div>
    </section>
  );
}