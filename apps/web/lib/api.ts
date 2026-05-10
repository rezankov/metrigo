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