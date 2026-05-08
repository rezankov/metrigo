# Metrigo Screen Architecture

## Purpose

Этот документ описывает экранную архитектуру Metrigo.

Metrigo строится как AI-first conversational analytics interface для WB-селлера.

Главный принцип:
чат — основной интерфейс,
дашборды — второй слой,
таблицы — глубокий слой проверки.

---

# 1. Home / Chat Screen

Главный экран приложения.

## Structure

### Top bar

Содержит:
- логотип Metrigo;
- текущий seller account;
- состояние системы;
- кнопку уведомлений;
- меню настроек.

### Compact metrics area

Небольшой закреплённый блок над чатом.

Показывает:
- продажи сегодня;
- заказы сегодня;
- выручку;
- DRR;
- остаточные риски;
- системный статус.

Цель блока:
быстро дать состояние бизнеса без перегруза.

### Conversation area

Основная часть экрана.

Содержит:
- AI summary как первое сообщение;
- диалог пользователя с AI;
- inline charts;
- inline tables;
- explanations;
- recommendations;
- business events.

### Smart action chips

Блок между чатом и input.

Примеры:
- Что происходит сегодня?
- Почему просели продажи?
- Что заказать?
- Покажи рекламу
- Остатки
- Финансы
- Найди проблему

Chips меняются динамически в зависимости от контекста.

### Message input

Поле ввода в стиле современных chat apps.

Будущее развитие:
- voice input;
- voice response;
- quick commands.

---

# 2. AI Message Types

AI в Metrigo может отвечать разными типами сообщений.

## Text insight

Обычный аналитический ответ.

Пример:
“Продажи вчера выросли на 18%, основной вклад дал SKU bg-org-8-beige.”

## Inline chart

График прямо в переписке.

Примеры:
- продажи за 14 дней;
- DRR по дням;
- остатки по SKU;
- расходы рекламы.

## Inline KPI card

Компактная карточка внутри чата.

Пример:
- выручка;
- прибыль;
- заказы;
- DRR.

## Inline table preview

Короткая таблица на 5–10 строк.

Для глубокой таблицы пользователь может открыть deep data layer.

## Recommendation

AI предлагает действие.

Примеры:
- снизить ставку;
- проверить карточку;
- подготовить поставку;
- перераспределить товар.

## Warning

Бизнес-предупреждение.

Примеры:
- остаток меньше 7 дней;
- реклама тратит деньги без заказов;
- возвраты выросли.

---

# 3. Dashboard Layer

Dashboards — второй слой интерфейса.

Они открываются:
- по свайпу;
- через кнопку;
- через action chip;
- по запросу из чата.

## Dashboard sections

### Overview

Общее состояние бизнеса:
- продажи;
- заказы;
- выручка;
- прибыль;
- реклама;
- остатки.

### Sales

- revenue;
- orders;
- buyouts;
- returns;
- SKU performance;
- trend comparison.

### Ads

- spend;
- CTR;
- CPC;
- DRR;
- orders from ads;
- campaign performance.

### Stocks

- stock levels;
- days cover;
- warehouse distribution;
- stuck inventory;
- replenishment forecast.

### Finance

- commissions;
- logistics;
- storage;
- penalties;
- tax base;
- net profit.

### System

- ETL health;
- collectors;
- last runs;
- errors;
- integrations;
- MAX alerts.

---

# 4. Notification Center

Notification Center — место для бизнес-событий и задач.

Важно:
системные аварии уходят в MAX сразу,
бизнес-события остаются в кабинете.

## Notification types

### Business insight

Например:
“Продажи SKU bg-org-8-gray выросли на 32%.”

### Risk

Например:
“Остатка bg-org-8-beige хватит на 5 дней.”

### Task

Например:
“Проверьте рекламу кампании 36249109.”

### System info

Например:
“Все сборщики работают нормально.”

## Statuses

- new;
- read;
- resolved;
- ignored.

---

# 5. Deep Data Layer

Глубокий слой данных.

Нужен для:
- проверки;
- экспорта;
- аудита;
- troubleshooting.

## Contains

- fact tables;
- raw events;
- filtered tables;
- SQL-like exports;
- CSV downloads.

Обычный пользователь не должен начинать работу отсюда.

---

# 6. Navigation Model

Metrigo не должен ощущаться как классический BI.

## Primary navigation

Главная навигация:
- Chat;
- Dashboards;
- Notifications;
- Settings.

## AI-driven navigation

Пользователь может написать:

“Покажи рекламу за 14 дней”

И Metrigo:
- строит график;
- добавляет объяснение;
- может открыть Ads dashboard.

## Context-aware transitions

Из любого графика:
- Explain;
- Drill down;
- Compare;
- Show table;
- Ask AI.

---

# 7. Mobile-first UX

Metrigo проектируется сначала для телефона.

## Principles

- одна главная мысль на экран;
- минимум таблиц;
- крупные цифры;
- короткие summary;
- свайпы вместо сложных меню;
- action chips вместо длинной навигации.

## Desktop

Desktop-версия может показывать больше:
- боковую панель;
- несколько графиков;
- расширенные таблицы.

Но основной UX всё равно chat-first.

---

# 8. Voice Future

Будущее направление Metrigo — голосовой интерфейс.

Примеры:
- “Что вчера было с продажами?”
- “Почему реклама стала дороже?”
- “Сколько товара осталось?”
- “Что заказать на следующей неделе?”

AI должен отвечать:
- голосом;
- текстом;
- графиком;
- действием.

---

# 9. Product Direction

Metrigo развивается не как BI-сервис,
а как AI operating system для WB-селлера.

Главная задача:
помогать продавцу понимать бизнес и принимать решения быстрее.