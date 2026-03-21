#!/usr/bin/env bash
# Локальный запуск без Docker (требует: Python 3.11+, Node 18+, PostgreSQL)
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Ecom Analytics — локальный запуск ==="
echo ""

# ── Backend ──────────────────────────────────────────────────────────────────
cd "$PROJECT_DIR/backend"

# Создаём venv если нет
if [ ! -d ".venv" ]; then
  echo "Creating Python venv..."
  python3 -m venv .venv
fi

source .venv/bin/activate
echo "Installing backend dependencies..."
pip install -q -r requirements.txt

# Создаём .env для локального запуска
cat > .env.local << 'EOF'
DATABASE_URL=postgresql://ecom:ecom_pass@localhost:5432/ecom_analytics
REDIS_URL=redis://localhost:6379/0
SECRET_KEY=supersecretkey-change-in-production-32chars!!
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=1440
EOF

# Проверяем PostgreSQL
if ! psql -U ecom -d ecom_analytics -c "SELECT 1" > /dev/null 2>&1; then
  echo ""
  echo "❌ Не могу подключиться к PostgreSQL."
  echo "   Создайте БД командами:"
  echo "   psql -U postgres -c \"CREATE USER ecom WITH PASSWORD 'ecom_pass';\""
  echo "   psql -U postgres -c \"CREATE DATABASE ecom_analytics OWNER ecom;\""
  echo ""
  exit 1
fi

# Запускаем миграции
echo "Running migrations..."
DATABASE_URL=postgresql://ecom:ecom_pass@localhost:5432/ecom_analytics \
  alembic upgrade head 2>/dev/null || python -c "
from app.core.config import settings
settings.__class__.__config__.env_file = '.env.local'
import os; os.environ['DATABASE_URL']='postgresql://ecom:ecom_pass@localhost:5432/ecom_analytics'
from app.core.database import Base, engine
import app.models
Base.metadata.create_all(engine)
print('Tables created')
"

# Seed данные
echo "Seeding mock data..."
DATABASE_URL=postgresql://ecom:ecom_pass@localhost:5432/ecom_analytics python seed.py

# Запускаем backend в фоне
echo "Starting backend on :8000..."
DATABASE_URL=postgresql://ecom:ecom_pass@localhost:5432/ecom_analytics \
  uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload &
BACKEND_PID=$!

# ── Frontend ──────────────────────────────────────────────────────────────────
cd "$PROJECT_DIR/frontend"

echo "Installing frontend dependencies..."
npm install --silent

echo "Starting frontend on :3000..."
VITE_API_URL=http://localhost:8000 npm run dev &
FRONTEND_PID=$!

echo ""
echo "✓ Приложение запущено!"
echo "  Frontend: http://localhost:3000"
echo "  API Docs: http://localhost:8000/api/docs"
echo ""
echo "Нажмите Ctrl+C для остановки."

trap "kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; exit" INT TERM
wait
