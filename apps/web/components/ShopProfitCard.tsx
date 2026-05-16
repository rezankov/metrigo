"use client";

import React from "react";
import { ShopProfit } from "@/lib/api";

type Props = {
  profit: ShopProfit;
  periodLabel?: string;
};

export const ShopProfitCard: React.FC<Props> = ({ profit, periodLabel }) => {
  if (!profit) return null;

  return (
    <div className="shop-profit-card">
      <h2>{periodLabel || (profit.month ? `Экономика за ${new Date(profit.month).toLocaleString("ru-RU", { month: "long", year: "numeric" })}` : "")}</h2>

      <div className="profit-row">
        <span className="label">💰 Чистая прибыль:</span>
        <span className="value">{profit.net_profit.toLocaleString()} ₽</span>
      </div>

      <div className="profit-row">
        <span className="label">📈 Выручка:</span>
        <span className="value">{profit.revenue.toLocaleString()} ₽</span>
      </div>

      <div className="profit-row">
        <span className="label">🛍️ Прибыль по товарам:</span>
        <span className="value">{profit.gross_profit.toLocaleString()} ₽</span>
      </div>

      <div className="profit-row">
        <span className="label">🏢 Расходы магазина:</span>
        <span className="value">{profit.shop_expenses.toLocaleString()} ₽</span>
      </div>

      {profit.expense_items.length > 0 && (
        <div className="expense-items">
          {profit.expense_items.map((e, i) => (
            <div key={i} className="expense-row">
              <span className="label">🔹 {e.expense_name}</span>
              <span className="value">{e.amount.toLocaleString()} ₽</span>
            </div>
          ))}
        </div>
      )}

      <style jsx>{`
        .profit-row, .expense-row {
          display: flex;
          justify-content: space-between;
          margin-bottom: 4px;
        }

        .label {
          text-align: left;
        }

        .value {
          text-align: right;
          font-weight: bold;
        }

        .expense-items {
          margin-top: 8px;
          padding-left: 16px;
        }

        h2 {
          margin-bottom: 12px;
          font-size: 1.2rem;
          font-weight: bold;
        }
      `}</style>
    </div>
  );
};