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
  const response = await fetch("https://metrigo.ru/api/summary/today", {
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error("Failed to load summary");
  }

  return response.json();
}

export type ChatResponse = {
  type: "text";
  text: string;
};

export async function sendChatMessage(message: string): Promise<ChatResponse> {
  const response = await fetch("https://metrigo.ru/api/chat", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    cache: "no-store",
    body: JSON.stringify({ message }),
  });

  if (!response.ok) {
    throw new Error("Failed to send chat message");
  }

  return response.json();
}