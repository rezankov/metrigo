"use client";

import { useEffect, useState } from "react";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";

type Props = { endpoint: string };

export default function ChatChart({ endpoint }: Props) {
  const [data, setData] = useState<{ date: string; revenue: number }[]>([]);

  useEffect(() => {
    fetch(endpoint)
      .then(r => r.json())
      .then(json => {
        const rows = json.labels.map((label: string, i: number) => ({
          date: label.slice(5),
          revenue: json.values[i],
        }));
        setData(rows);
      });
  }, [endpoint]);

  return (
    <div style={{ width: "100%", height: 220, marginTop: 12 }}>
      <ResponsiveContainer>
        <LineChart data={data} margin={{ top: 12, right: 12, bottom: 8, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="date" tick={{ fontSize: 11 }} minTickGap={10} />
          <YAxis width={46} tick={{ fontSize: 11 }} tickFormatter={v => `${Math.round(v / 1000)}к`} />
          <Tooltip formatter={v => [`${Number(v).toLocaleString("ru-RU")} ₽`, "Выручка"]} labelFormatter={l => `Дата: ${l}`} />
          <Line type="monotone" dataKey="revenue" strokeWidth={3} dot={{ r: 3 }} activeDot={{ r: 5 }} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}