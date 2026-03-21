# Ecom Analytics — РнП

Сервис управленческой аналитики e-commerce бизнеса.

## Быстрый старт

```bash
cd ecom-analytics
docker-compose up --build -d
```

Дождитесь запуска (~1-2 минуты), затем загрузите мок-данные:

```bash
docker-compose exec backend python seed.py
```

Открыть в браузере: http://localhost:3000

## Тестовые аккаунты

| Email | Пароль | Роль |
|-------|--------|------|
| owner@ecom.ru | demo1234 | Собственник |
| finance@ecom.ru | demo1234 | Финансовый менеджер |
| marketer@ecom.ru | demo1234 | Маркетолог |
| mp@ecom.ru | demo1234 | Менеджер МП |
| warehouse@ecom.ru | demo1234 | Склад |

## Сервисы

| Сервис | URL |
|--------|-----|
| Frontend | http://localhost:3000 |
| Backend API | http://localhost:8000 |
| API Docs (Swagger) | http://localhost:8000/api/docs |
| PostgreSQL | localhost:5432 |
| Redis | localhost:6379 |

## Реализованные модули

- **Дашборд** — KPI карточки, график заказов, алерты по остаткам
- **РнП** — таблица по SKU с маржой, оборачиваемостью, TACoS
- **Продажи** — динамика, сводка, таблица заказов
- **SKU** — каталог товаров с остатками и каналами

## Структура

```
ecom-analytics/
├── backend/          # FastAPI + PostgreSQL
│   ├── app/
│   │   ├── api/      # Роуты
│   │   ├── models/   # SQLAlchemy
│   │   ├── schemas/  # Pydantic
│   │   └── services/ # Бизнес-логика
│   └── seed.py       # Мок-данные
└── frontend/         # React + Vite + Tailwind
    └── src/
        ├── pages/
        └── components/
```
