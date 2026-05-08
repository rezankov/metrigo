# Metrigo Design System

## Design direction

Metrigo должен ощущаться как:
- светлый;
- спокойный;
- точный;
- дружелюбный;
- деловой;
- не игрушечный;
- простой для понимания.

Главная эмоция:
“С бизнесом всё понятно. Я вижу главное. AI помогает мне принять решение.”

---

# 1. Visual style

## Keywords

- light
- clean
- calm
- precise
- optimistic
- assistant-like
- business-focused

## Avoid

- тяжёлые BI-интерфейсы;
- тёмные перегруженные панели;
- кислотные цвета;
- слишком много таблиц;
- агрессивные красные предупреждения без причины;
- “игрушечный” AI-стиль.

---

# 2. Color system

## Backgrounds

### App background

Очень светлый фон.

Suggested:
- `#F7F8FA`
- `#F8FAFC`
- `#FAFAF7`

### Card background

Белый или почти белый.

Suggested:
- `#FFFFFF`
- `#FCFCFD`

### Soft accent background

Для AI-блоков, подсказок, insights.

Suggested:
- `#EEF7F4`
- `#EFF6FF`
- `#F5F3FF`

---

# 3. Brand colors

## Primary

Мягкий зелёно-бирюзовый / mint-teal.

Suggested:
- `#2FBF9B`
- `#28BFA3`
- `#32C8A3`

## Secondary

Спокойный синий.

Suggested:
- `#3B82F6`
- `#60A5FA`

## Warm accent

Для положительных бизнес-инсайтов.

Suggested:
- `#F6C85F`
- `#F4B740`

---

# 4. Status colors

## Success

- `#22C55E`
- emoji: 🟢

## Warning

- `#F59E0B`
- emoji: 🟡

## Critical

- `#EF4444`
- emoji: 🔴

## Neutral

- `#64748B`
- emoji: ⚪

Важно:
красный использовать только для действительно критичных состояний.

---

# 5. Typography

## Font direction

Интерфейс должен быть похож на современные mobile apps:
- читабельно;
- спокойно;
- без “корпоративной тяжести”.

Suggested font stack:
- system-ui
- Inter
- SF Pro
- Roboto

## Text hierarchy

### Large KPI

Используется для главных цифр.

- font-size: 28–36px
- font-weight: 650–750

### Section title

- font-size: 18–22px
- font-weight: 600

### Body text

- font-size: 15–17px
- font-weight: 400

### Caption

- font-size: 12–13px
- color: muted

---

# 6. Layout principles

## Mobile-first

Первичный UX — телефон.

## Spacing

Интерфейс должен дышать.

Suggested:
- base spacing: 8px
- card padding: 16px
- section gap: 20–24px
- screen side padding: 16px

## Corners

Мягкие скругления.

Suggested:
- cards: 20–24px
- chips: 999px
- input: 24px

## Shadows

Тени минимальные.

Suggested:
- soft card shadow
- больше использовать border + background, чем heavy shadows.

---

# 7. Core components

## Top bar

Содержит:
- logo;
- seller selector;
- system status;
- notifications;
- settings.

Должен быть compact и не забирать внимание у чата.

---

## Compact metrics strip

Небольшой блок над чатом.

Показывает:
- сегодня;
- 7/14 дней;
- прибыль/выручку;
- DRR;
- риски.

Формат:
- 3–5 мини-KPI;
- горизонтальный scroll на mobile;
- fixed/sticky опционально.

---

## Chat bubble

### User bubble

- справа;
- neutral / primary light;
- короткие запросы.

### AI bubble

- слева;
- white card;
- может включать:
  - текст;
  - KPI;
  - chart;
  - recommendation;
  - action buttons.

---

## Smart action chips

Располагаются между чатом и input.

Примеры:
- Почему просели продажи
- Что заказать
- Реклама
- Остатки
- Финансы
- Найди проблему

Правила:
- короткие;
- контекстные;
- не больше 4–6 одновременно;
- горизонтальный scroll.

---

## KPI Card

Используется в чате и dashboard.

Поля:
- title;
- value;
- delta;
- status;
- explanation.

---

## Inline chart card

График внутри чата.

Должен содержать:
- title;
- period;
- chart;
- short AI explanation;
- actions:
  - Explain;
  - Compare;
  - Open dashboard.

---

## Notification card

Для business events.

Поля:
- severity;
- category;
- title;
- message;
- entity;
- suggested action;
- status.

---

# 8. Dashboard components

Dashboards не должны выглядеть как Metabase.

## Rules

- меньше таблиц;
- больше объяснений;
- каждый график должен иметь смысл;
- рядом с графиком — краткий AI-comment;
- пользователь должен понимать “что делать”.

## Dashboard card structure

- title;
- KPI/chart;
- trend;
- explanation;
- action.

---

# 9. Empty states

Пустые состояния должны быть дружелюбными.

Пример:

“Данных пока нет. Первый сборщик запустится в ближайшее время.”

Или:

“Реклама не найдена. Когда появятся кампании, Metrigo покажет эффективность.”

---

# 10. Loading states

Использовать:
- skeleton cards;
- мягкие shimmer;
- без резких спиннеров на весь экран.

---

# 11. Tone of voice

Metrigo говорит:
- спокойно;
- понятно;
- делово;
- без лишней паники;
- без канцелярита.

## Good

“Продажи вчера выросли на 18%. Основной вклад дал bg-org-8-beige.”

## Bad

“Обнаружено статистически значимое отклонение показателей реализации.”

---

# 12. AI personality

AI в Metrigo — помощник аналитик.

Он:
- объясняет;
- предупреждает;
- предлагает;
- не пугает;
- не обещает невозможного;
- показывает данные;
- честно говорит, если данных не хватает.

---

# 13. Motion

Анимации:
- мягкие;
- короткие;
- функциональные.

Примеры:
- появление AI message;
- раскрытие chart card;
- swipe между chat/dashboard;
- обновление KPI.

---

# 14. Accessibility

Обязательно:
- достаточный contrast;
- readable font sizes;
- не полагаться только на цвет;
- emoji + text для статусов;
- крупные touch targets.

---

# 15. Product feeling

Metrigo должен ощущаться как:
“умный спокойный помощник, который держит бизнес под контролем”.

Не как:
- бухгалтерская программа;
- перегруженный BI;
- игрушечный chatbot;
- сухая админка.