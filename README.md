# Metrigo

Metrigo — веб-кабинет аналитики и AI-помощник для продавцов Wildberries.

## Цель проекта

Создать масштабируемую систему для:
- загрузки данных из WB API
- хранения сырых и обработанных данных
- построения дашбордов
- анализа прибыли, продаж, остатков и поставок
- работы с AI-помощником
- поддержки нескольких продавцов

## Основные адреса

- https://metrigo.ru — основной кабинет
- https://api.metrigo.ru — backend API
- https://metrigo.ru/db — дашборды
- https://mb.metrigo.ru — Metabase (опционально)

## Архитектура

- apps/web — frontend
- apps/api — backend
- apps/worker — загрузчики WB
- packages/shared — общие модули
- packages/ai — AI логика
- db/clickhouse — аналитика
- db/postgres — пользователи и настройки
- nginx — прокси
- docs — документация

## Multi-tenant модель

Ключевая сущность:

- tenant_id / seller_id / account_id

Все данные привязаны к конкретному кабинету.