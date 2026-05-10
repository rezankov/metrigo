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