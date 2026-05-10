#!/bin/bash

echo "=== Проверка фронтенда на старые точки ==="
grep -R "/summary/today" /opt/metrigo/apps/web || echo "Старые вызовы /summary/today не найдены"
grep -R "/health" /opt/metrigo/apps/web || echo "Старые вызовы /health не найдены"

echo "=== Проверка фронтенда на новые инструменты ==="
grep -R "/tools/get_summary_today" /opt/metrigo/apps/web || echo "Новые вызовы /tools/get_summary_today не найдены"
grep -R "/tools/get_business_health" /opt/metrigo/apps/web || echo "Новые вызовы /tools/get_business_health не найдены"
grep -R "/tools/get_sales_chart_insight" /opt/metrigo/apps/web || echo "Новые вызовы /tools/get_sales_chart_insight не найдены"
grep -R "/tools/get_sku_context" /opt/metrigo/apps/web || echo "Новые вызовы /tools/get_sku_context не найдены"
grep -R "/tools/get_ad_context" /opt/metrigo/apps/web || echo "Новые вызовы /tools/get_ad_context не найдены"

echo "=== Проверка работы инструментов через API ==="

# Список инструментов и минимальные параметры
declare -A tools
tools["get_summary_today"]='{}'
tools["get_business_health"]='{"seller_id":"main"}'
tools["get_sales_chart_insight"]='{}'
tools["get_sku_context"]='{"seller_id":"main","sku":"bg-org-8-beige"}'
tools["get_stock_context"]='{"seller_id":"main","limit":5}'
tools["get_ad_context"]='{"seller_id":"main"}'

for tool in "${!tools[@]}"; do
    echo "-> Проверка $tool ..."
    response=$(curl -s -X POST http://127.0.0.1:8000/tools/$tool \
        -H "Content-Type: application/json" \
        -d "${tools[$tool]}")
    if [[ $response == *"error"* ]]; then
        echo "❌ Ошибка: $response"
    else
        echo "✅ Работает, результат: $response"
    fi
done

echo "=== Проверка завершена ==="