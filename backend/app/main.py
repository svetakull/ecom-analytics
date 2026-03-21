import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.endpoints import auth, channels, dashboard, dds, elasticity, finance, integrations, opiu, otsifrovka, rnp, sales, sku, sverka
from app.scheduler import start_scheduler, stop_scheduler

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    start_scheduler()
    yield
    stop_scheduler()


app = FastAPI(title="Ecom Analytics", version="1.0.0", docs_url="/api/docs", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router, prefix="/api/auth", tags=["auth"])
app.include_router(rnp.router, prefix="/api/rnp", tags=["РнП"])
app.include_router(sales.router, prefix="/api/sales", tags=["Продажи"])
app.include_router(sku.router, prefix="/api/sku", tags=["SKU"])
app.include_router(dashboard.router, prefix="/api/dashboard", tags=["Дашборд"])
app.include_router(channels.router, prefix="/api/channels", tags=["Каналы"])
app.include_router(integrations.router, prefix="/api/integrations", tags=["Интеграции"])
app.include_router(finance.router, prefix="/api/finance", tags=["Финансы"])
app.include_router(otsifrovka.router, prefix="/api/otsifrovka", tags=["Оцифровка"])
app.include_router(opiu.router, prefix="/api/opiu", tags=["ОПиУ"])
app.include_router(dds.router, prefix="/api/dds", tags=["ДДС"])
app.include_router(sverka.router, prefix="/api/sverka", tags=["Сверка"])
app.include_router(elasticity.router, prefix="/api/elasticity", tags=["Ценовая аналитика"])


@app.get("/api/health")
def health():
    return {"status": "ok"}
