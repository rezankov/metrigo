# Metrigo Frontend Architecture

## Goal

Frontend Metrigo строится как mobile-first AI chat application с аналитическими слоями.

Главный экран — чат.
Dashboards — второй слой.
Tables — deep data layer.

---

# Recommended stack

## Web app

- Next.js
- TypeScript
- React
- Tailwind CSS
- shadcn/ui или собственные lightweight components

## Charts

- Recharts на первом этапе
- позже можно рассмотреть ECharts

## State

- React state для локального UI
- TanStack Query для API data fetching
- Zustand опционально для global UI state

## API

- frontend ходит в `api.metrigo.ru`
- backend читает ClickHouse/Postgres
- frontend не ходит напрямую в ClickHouse

---

# App structure

```text
apps/web/
├── app/
│   ├── page.tsx
│   ├── layout.tsx
│   ├── globals.css
│   ├── chat/
│   ├── dashboards/
│   ├── notifications/
│   └── settings/
├── components/
│   ├── chat/
│   ├── charts/
│   ├── metrics/
│   ├── layout/
│   ├── notifications/
│   └── ui/
├── lib/
│   ├── api.ts
│   ├── format.ts
│   └── types.ts
└── package.json