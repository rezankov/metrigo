"use client";

import { useEffect, useMemo, useState } from "react";
import { DashboardSkuDetail, getDashboardSkuDetail } from "@/lib/api";

type SkuDetailProps = {
  sku: string;
};

function formatRub(value: number) {
  return `${Math.round(value || 0).toLocaleString("ru-RU")} ₽`;
}

function formatDecimalRub(value: number) {
  return `${Number(value || 0).toLocaleString("ru-RU", {
    maximumFractionDigits: 2,
  })} ₽`;
}

export default function SkuDetail({ sku }: SkuDetailProps) {
  const [data, setData] = useState<DashboardSkuDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  useEffect(() => {
    setLoading(true);
    setError(false);

    getDashboardSkuDetail(sku, 30)
      .then(setData)
      .catch(() => setError(true))
      .finally(() => setLoading(false));
  }, [sku]);

  const unitItems = data?.unit_economics.items || [];
  const unitTotal = useMemo(
    () => unitItems.reduce((sum, item) => sum + Math.max(0, item.value || 0), 0),
    [unitItems],
  );

  if (loading) {
    return <div className="skuDetailCard">Загрузка данных SKU…</div>;
  }

  if (error || !data) {
    return <div className="skuDetailCard">Ошибка загрузки данных SKU.</div>;
  }

  return (
    <div className="skuDetailCard">
      <div className="unitEconomyHeader">
        <span>Юнит-экономика</span>
        <strong>{formatRub(data.unit_economics.price)}</strong>
      </div>

      <div className="unitEconomy">
        {unitItems.map((item) => {
          const width = unitTotal > 0 ? (Math.max(0, item.value) / unitTotal) * 100 : 0;

          return (
            <div
              key={item.key}
              className={`unitBar ${item.key}`}
              style={{ width: `${width}%` }}
              title={`${item.label}: ${formatDecimalRub(item.value)}`}
            />
          );
        })}
      </div>

      <div className="unitValues">
        {unitItems.map((item) => (
          <div key={item.key} className="unitValueRow">
            <span>
              <i className={`unitDot ${item.key}`} />
              {item.label}
            </span>
            <strong>{formatDecimalRub(item.value)}</strong>
          </div>
        ))}
      </div>

      {data.warehouses.length > 0 && (
        <div className="warehouseTable">
          <div className="warehouseTableTitle">Остатки по складам</div>

          <div className="warehouseRow expanded total">
            <span>Итого</span>
            <strong>{data.warehouse_summary.qty_full}</strong>
            <strong>{data.warehouse_summary.qty}</strong>
            <strong>{data.warehouse_summary.in_way_to_client}</strong>
            <strong>{data.warehouse_summary.in_way_from_client}</strong>
            <strong>{data.warehouse_summary.returns}</strong>
          </div>

          <div className="warehouseHeader expanded">
            <span>Склад</span>
            <span>Всего</span>
            <span>Склад</span>
            <span>Клиент</span>
            <span>Назад</span>
            <span>Возвр.</span>
          </div>

          {data.warehouses.map((warehouse) => (
            <div key={warehouse.warehouse} className="warehouseRow expanded">
              <span>{warehouse.warehouse || "Без названия"}</span>
              <strong>{warehouse.qty_full}</strong>
              <strong>{warehouse.qty}</strong>
              <strong>{warehouse.in_way_to_client}</strong>
              <strong>{warehouse.in_way_from_client}</strong>
              <strong>{warehouse.returns}</strong>
            </div>
          ))}
        </div>
      )}

      <div className="graphPlaceholder">
        <p>График маржи и прибыли по фин. отчётам добавим следующим шагом.</p>
      </div>
    </div>
  );
}