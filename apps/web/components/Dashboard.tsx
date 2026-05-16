"use client";

import { useEffect, useMemo, useState } from "react";
import {
  DashboardSkuItem,
  DashboardSkuList,
  getDashboardSkuList,
  getShopProfit,
  ShopProfit,
  getMonthlyProfit,
  MonthlyProfit,
} from "@/lib/api";
import SkuDetail from "@/components/SkuDetail";
import { ShopProfitCard } from "@/components/ShopProfitCard";
import { Line } from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from "chart.js";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend);

type SortKey = "turnover" | "cover" | "margin";

function formatRub(value: number, short = false) {
  if (short) {
    return `${Math.round(value / 1000).toLocaleString("ru-RU")} тр`;
  }
  return `${Math.round(value || 0).toLocaleString("ru-RU")} р/мес.`;
}

function getCoverClass(daysCover: number) {
  if (daysCover < 25) return "critical";
  if (daysCover < 45) return "warning";
  return "ok";
}

function SkuRow({
  item,
  openedSku,
  setOpenedSku,
}: {
  item: DashboardSkuItem;
  openedSku: string | null;
  setOpenedSku: (sku: string | null) => void;
}) {
  const coverClass = getCoverClass(item.coverage_days);
  const isOpened = openedSku === item.sku;

  return (
    <>
      <button
        className={`skuRow compact ${isOpened ? "opened" : ""}`}
        type="button"
        onClick={() => setOpenedSku(isOpened ? null : item.sku)}
      >
        <div className="skuMain">
          <div className="skuTitleLine">
            <strong>{item.sku}</strong>
            <span className="skuMargin">маржа {item.margin_percent.toFixed(1)}%</span>
          </div>
          <span className="skuRevenue">{formatRub(item.revenue)}</span>
        </div>

        <div className="skuMetrics compact five">
          <div>
            <span>Ост.</span>
            <strong>{item.stock_qty}</strong>
          </div>
          <div>
            <span>Покр.</span>
            <strong className={`cover ${coverClass}`}>{item.coverage_days.toFixed(1)}</strong>
          </div>
          <div>
            <span>Цена</span>
            <strong>{formatRub(item.avg_price)}</strong>
          </div>
          <div>
            <span>Вчера</span>
            <strong>{item.orders_24h}/{item.buyouts_24h}</strong>
          </div>
          <div>
            <span>Неделя</span>
            <strong>{item.orders_7d}/{item.buyouts_7d}</strong>
          </div>
        </div>
      </button>

      {isOpened && <SkuDetail sku={item.sku} />}
    </>
  );
}

export default function Dashboard() {
  const [skuData, setSkuData] = useState<DashboardSkuList | null>(null);
  const [profitData, setProfitData] = useState<ShopProfit | null>(null);
  const [monthlyProfit, setMonthlyProfit] = useState<MonthlyProfit[]>([]);
  const [sortKey, setSortKey] = useState<SortKey>("turnover");
  const [openedSku, setOpenedSku] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  // --- Период для карточек ---
  const periodLabel = "Экономика 1-16 мая 2026 г.";

  useEffect(() => {
    Promise.all([
      getDashboardSkuList(30),
      getShopProfit("main"),
      getMonthlyProfit("main"),
    ])
      .then(([sku, profit, monthly]) => {
        setSkuData(sku);
        setProfitData(profit);
        setMonthlyProfit(monthly);
      })
      .catch((err) => {
        console.error(err);
        setError(true);
      })
      .finally(() => setLoading(false));
  }, []);

  const sortedItems = useMemo(() => {
    const items = [...(skuData?.items || [])];
    if (sortKey === "turnover") return items.sort((a, b) => b.revenue - a.revenue);
    if (sortKey === "cover") return items.sort((a, b) => a.coverage_days - b.coverage_days);
    return items.sort((a, b) => a.margin_percent - b.margin_percent);
  }, [skuData, sortKey]);

  if (loading) return <div className="dashboardCard">Загрузка данных…</div>;
  if (error || !skuData || !profitData) return <div className="dashboardCard">Ошибка загрузки дашборда.</div>;

  // --- данные для графика ---
  const chartData = {
    labels: monthlyProfit.map((m) =>
      new Date(`${m.month}-01`).toLocaleString("ru-RU", { month: "long" })
    ),
    datasets: [
      {
        label: "Выручка",
        data: monthlyProfit.map((m) => m.revenue),
        borderColor: "#3b82f6",
        backgroundColor: "rgba(59, 130, 246, 0.2)",
        yAxisID: "y",
      },
      {
        label: "Чистая прибыль",
        data: monthlyProfit.map((m) => m.net_profit),
        borderColor: "#16a34a",
        backgroundColor: "rgba(22, 163, 52, 0.2)",
        yAxisID: "y1",
      },
    ],
  };

  const chartOptions = {
    responsive: true,
    interaction: { mode: "index" as const, intersect: false },
    stacked: false,
    plugins: { legend: { position: "top" as const } },
    scales: {
      x: { title: { display: true, text: "Месяцы" } },
      y: {
        type: "linear" as const,
        position: "left" as const,
        title: { display: true, text: "Выручка" },
        ticks: {
          callback: (value: any) => formatRub(Number(value), true), // сокращение тр
        },
      },
      y1: {
        type: "linear" as const,
        position: "right" as const,
        title: { display: true, text: "Чистая прибыль" },
        grid: { drawOnChartArea: false },
        ticks: {
          callback: (value: any) => formatRub(Number(value), true), // сокращение тр
        },
      },
    },
  };

  return (
    <section className="dashboard">
      <ShopProfitCard profit={profitData} periodLabel={periodLabel} />

      {monthlyProfit.length > 0 && (
        <div className="monthly-profit-chart">
          <Line data={chartData} options={chartOptions} />
        </div>
      )}

      <div className="dashboardTitle compact">
        <div>
          <h1>SKU</h1>
        </div>
        <span>
          {`178 288 р/мес.`}
        </span>
      </div>

      <div className="sortTabs">
        <button className={sortKey === "turnover" ? "active" : ""} onClick={() => setSortKey("turnover")}>
          Оборот
        </button>
        <button className={sortKey === "cover" ? "active" : ""} onClick={() => setSortKey("cover")}>
          Покрытие
        </button>
        <button className={sortKey === "margin" ? "active" : ""}>
          Маржа
        </button>
      </div>

      <div className="skuList compact">
        {sortedItems.map((item) => (
          <SkuRow key={item.sku} item={item} openedSku={openedSku} setOpenedSku={setOpenedSku} />
        ))}
      </div>
    </section>
  );
}