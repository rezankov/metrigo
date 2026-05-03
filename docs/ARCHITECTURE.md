# Архитектура Metrigo

## Общая схема

metrigo.ru
└── веб-кабинет:
    ├── чат с ИИ
    ├── дашборды
    ├── настройки
    └── пользователи

api.metrigo.ru
└── backend:
    ├── auth
    ├── users
    ├── sellers
    ├── wb api
    ├── dashboards
    └── ai

worker
└── фоновые задачи:
    ├── загрузка продаж
    ├── загрузка остатков
    ├── загрузка заказов
    ├── финансы
    └── перерасчёты

## Базы данных

### Postgres

- users
- accounts
- sellers
- access
- tokens
- settings

### ClickHouse

- raw_events
- fact_sales
- fact_orders
- fact_stocks
- fact_finance
- агрегаты

## Metabase

Пока используется на старом сервере.

В новый проект не переносится на первом этапе.