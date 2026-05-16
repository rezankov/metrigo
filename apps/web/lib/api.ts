// lib/api.ts

// --------------------
// Chat / Summary
// --------------------
export type TodaySummary = {
  seller_id: string;
  sales_count: number;
  orders_count: number;
  revenue: number;
  ad_spend: number;
  drr: number;
  system_status: string;
  summary_text: string;
  suggested_actions: string[];
  priority: "ok" | "normal" | "warning";
  risks: string[];
};

export async function getTodaySummary(): Promise<TodaySummary> {
  const res = await fetch("/api/tools/get_summary_today", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    cache: "no-store",
    body: JSON.stringify({}),
  });
  if (!res.ok) throw new Error("Failed to load summary");
  const data = await res.json();
  return data.result;
}

export type ChatHistoryMessage = {
  id: number;
  role: "user" | "assistant";
  content: string;
  created_at: string;
};

export async function getChatHistory(limit = 30, beforeId?: number): Promise<ChatHistoryMessage[]> {
  const params: any = { limit };
  if (beforeId) params.before_id = beforeId;
  const res = await fetch(`/api/chat/history?${new URLSearchParams(params)}`, {
    method: "GET",
    cache: "no-store",
  });
  if (!res.ok) throw new Error("Failed to load chat history");
  const data = await res.json();
  return data.messages || [];
}

export async function sendChatMessage(message: string) {
  const res = await fetch("/api/chat", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ message }),
  });
  if (!res.ok) throw new Error("Failed to send chat message");
  return res.json();
}

// --------------------
// Shop Profit / Monthly
// --------------------
export type ShopProfit = {
  seller_id: string;
  month: string;
  revenue: number;
  gross_profit: number;
  shop_expenses: number;
  net_profit: number;
  expense_items: { expense_name: string; amount: number }[];
};

export async function getShopProfit(sellerId: string): Promise<ShopProfit> {
  const res = await fetch("/api/dashboard/shop_profit", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ seller_id: sellerId }),
  });
  if (!res.ok) throw new Error("Failed to fetch shop profit");
  const data = await res.json();
  return data.result;
}

// --------------------
// Dashboard SKU List / Detail
// --------------------
export type DashboardSkuItem = {
  sku: string;
  revenue: number;
  margin_percent: number;
  profit_per_unit: number;
  avg_price: number;
  stock_qty: number;
  stock_qty_full: number;
  coverage_days: number;
  orders_24h: number;
  buyouts_24h: number;
  orders_7d: number;
  buyouts_7d: number;
};

export type DashboardSkuList = {
  items: DashboardSkuItem[];
};

export async function getDashboardSkuList(days = 30): Promise<DashboardSkuList> {
  const res = await fetch("/api/dashboard/sku_list", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ seller_id: "main", days }),
  });
  if (!res.ok) throw new Error("Failed to fetch SKU list");
  const data = await res.json();
  if (Array.isArray(data.result)) {
    return { items: data.result };
  }
  return data.result;
}

export type SkuUnitItem = {
  key: string;
  label: string;
  value: number;
};

export type SkuWarehouse = {
  warehouse: string;
  qty: number;
  qty_full: number;
  in_way_to_client: number;
  in_way_from_client: number;
  returns: number;
};

export type SkuSalesChartPoint = {
  date: string;
  revenue: number;
  sales_count: number;
};

export type DashboardSkuDetail = {
  seller_id: string;
  sku: string;
  days: number;
  summary: {
    sales_count: number;
    revenue: number;
    avg_price: number;
    cogs: number;
    profit_per_unit: number;
    margin_percent: number;
  };
  unit_economics: {
    price: number;
    items: SkuUnitItem[];
    note: string;
  };
  sales_chart: SkuSalesChartPoint[];
  warehouse_summary: {
    qty: number;
    qty_full: number;
    in_way_to_client: number;
    in_way_from_client: number;
    returns: number;
  };
  warehouses: SkuWarehouse[];
};

export async function getDashboardSkuDetail(sku: string, days = 30): Promise<DashboardSkuDetail> {
  const res = await fetch("/api/dashboard/sku_detail", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ seller_id: "main", sku, days }),
  });
  if (!res.ok) throw new Error("Failed to fetch SKU detail");
  const data = await res.json();
  return data.result;
}

// --------------------
// Charts / Metrics
// --------------------
export type SalesMiniChart = {
  labels: string[];
  values: number[];
  max_value: number;
  days: number;
};

export async function getSalesMiniChart(days = 60): Promise<SalesMiniChart> {
  const res = await fetch("/api/tools/get_sales_mini_chart", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    cache: "no-store",
    body: JSON.stringify({ seller_id: "main", days }),
  });
  if (!res.ok) throw new Error("Failed to fetch sales mini chart");
  const data = await res.json();
  return data.result;
}

export type HeaderMetrics = {
  seller_id: string;
  days: number;
  orders_count: number;
  buyouts_count: number;
  revenue: number;
  tax: number;
  revenue_after_tax: number;
};

export async function getHeaderMetrics(days = 7): Promise<HeaderMetrics> {
  const res = await fetch("/api/tools/get_header_metrics", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    cache: "no-store",
    body: JSON.stringify({ seller_id: "main", days }),
  });
  if (!res.ok) throw new Error("Failed to fetch header metrics");
  const data = await res.json();
  return data.result;
}

export type MonthlyProfit = {
  month: string;
  revenue: number;
  net_profit: number;
};

export async function getMonthlyProfit(sellerId: string): Promise<MonthlyProfit[]> {
  const res = await fetch("/api/dashboard/monthly_profit", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ seller_id: sellerId }),
  });

  if (!res.ok) throw new Error("Failed to fetch monthly profit");

  const data = await res.json();
  return data.result as MonthlyProfit[];
}