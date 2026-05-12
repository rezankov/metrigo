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

export type ChatResponse = {
  type: "text" | "chart";
  text: string;
  chart?: {
    endpoint: string;
  };
};

export async function getTodaySummary(): Promise<TodaySummary> {
  const response = await fetch("/api/tools/get_summary_today", { cache: "no-store", method: "POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({}) });
  if (!response.ok) throw new Error("Failed to load summary");
  return response.json().then(r => r.result);
}

export async function sendChatMessage(message: string): Promise<ChatResponse> {
  const response = await fetch("/api/chat", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ message }) });
  if (!response.ok) throw new Error("Failed to send chat message");
  return response.json();
}

export type ChatHistoryMessage = {
  id: number;
  role: "user" | "assistant";
  content: string;
  created_at: string;
};

export async function getChatHistory(
  limit = 30,
  beforeId?: number,
): Promise<ChatHistoryMessage[]> {
  const params = new URLSearchParams({
    limit: String(limit),
  });

  if (beforeId) {
    params.set("before_id", String(beforeId));
  }

  const response = await fetch(`/api/chat/history?${params.toString()}`, {
    method: "GET",
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error("Failed to load chat history");
  }

  const data = await response.json();

  return data.messages || [];
}

export type SalesMiniChart = {
  labels: string[];
  values: number[];
  max_value: number;
  days: number;
};

export async function getSalesMiniChart(days = 60): Promise<SalesMiniChart> {
  const response = await fetch("/api/tools/get_sales_mini_chart", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    cache: "no-store",
    body: JSON.stringify({
      seller_id: "main",
      days,
    }),
  });

  if (!response.ok) {
    throw new Error("Failed to load sales mini chart");
  }

  const data = await response.json();

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
  const response = await fetch("/api/tools/get_header_metrics", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    cache: "no-store",
    body: JSON.stringify({
      seller_id: "main",
      days,
    }),
  });

  if (!response.ok) {
    throw new Error("Failed to load header metrics");
  }

  const data = await response.json();

  return data.result;
}