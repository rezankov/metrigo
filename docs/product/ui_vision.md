# Metrigo UI Vision

## Product philosophy

Metrigo — не классический кабинет аналитики Wildberries.

Metrigo — AI-first conversational operating system для селлера.

Главная идея:
- пользователь не ищет проблемы вручную;
- AI сам объясняет ситуацию;
- AI помогает принимать решения;
- dashboards являются supporting layer, а не основным интерфейсом.

---

# Core UX Principles

## 1. Chat-first interface

Главный интерфейс Metrigo — чат.

Пользователь:
- задаёт вопросы;
- получает объяснения;
- получает графики прямо в переписке;
- получает рекомендации;
- управляет бизнесом через диалог.

Примеры:
- Почему просели продажи?
- Что заказать?
- Как работает реклама?
- Где завис товар?
- Что происходит сегодня?
- Покажи прибыль за 14 дней.

---

## 2. AI summary inside conversation

AI summary не является отдельным dashboard-блоком.

После открытия приложения AI публикует первое сообщение:
- summary за сегодня;
- anomalies;
- risks;
- recommendations.

Это сообщение становится частью истории диалога.

---

## 3. Compact persistent metrics

Верхняя часть интерфейса содержит компактные live metrics.

Например:
- заказы;
- выкупы;
- прибыль;
- DRR;
- состояние системы;
- риски.

Это:
- не dashboard;
- не BI;
- а "состояние бизнеса сейчас".

---

## 4. Smart action chips

Под чатом находятся contextual action chips.

Примеры:
- Почему просели продажи
- Остатки
- Реклама
- Что заказать
- Найди проблему
- Финансы
- Сравни недели

Chips работают как predictive actions и меняются динамически.

---

## 5. Dashboards are secondary layer

Dashboards существуют, но не являются главным интерфейсом.

Их задача:
- visual exploration;
- drill-down;
- historical analysis;
- verification.

Dashboards открываются:
- через AI;
- через action chips;
- через swipe/navigation.

---

## 6. Tables are deep layer

Таблицы — самый глубокий уровень интерфейса.

Они нужны:
- для проверки;
- для экспорта;
- для deep analytics;
- для troubleshooting.

Обычный пользователь не должен жить в таблицах.

---

# Suggested Navigation

## Layer 1 — Chat

Основной режим работы.

Содержит:
- AI dialogue;
- inline charts;
- recommendations;
- alerts;
- summaries.

---

## Layer 2 — Dashboards / Insights

Содержит:
- графики;
- KPI;
- trends;
- warehouse analytics;
- ads analytics;
- financial analytics.

---

## Layer 3 — Deep Data

Содержит:
- таблицы;
- raw data;
- exports;
- ETL details;
- system diagnostics.

---

# Dashboard Structure

Dashboards должны быть организованы не по таблицам БД, а по задачам бизнеса.

## Suggested sections

### Sales
- revenue
- profit
- buyouts
- returns
- trends
- SKU performance

### Ads
- DRR
- CPC
- CTR
- ROMI
- campaigns
- ads efficiency

### Stocks
- stock levels
- days cover
- warehouse balances
- stuck inventory
- replenishment forecast

### Finance
- commissions
- logistics
- storage
- penalties
- taxes
- net profit

### System
- ETL health
- jobs
- API status
- integrations
- alerts
- ingestion monitoring

---

# AI-first future direction

Metrigo развивается в сторону:
- conversational UX;
- voice interaction;
- proactive AI;
- dynamic dashboards;
- AI-generated insights;
- AI-generated recommendations;
- AI-controlled navigation.

Главная идея:
пользователь общается с бизнесом, а не ищет данные вручную.