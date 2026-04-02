from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


# ── КТР / ИРП ──

class KTRHistoryCreate(BaseModel):
    date_from: date
    date_to: date
    value: float


class KTRHistoryUpdate(BaseModel):
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    value: Optional[float] = None


class KTRHistoryOut(BaseModel):
    id: int
    date_from: date
    date_to: date
    value: float
    created_at: datetime

    class Config:
        from_attributes = True


class IRPHistoryCreate(BaseModel):
    date_from: date
    date_to: date
    value: float  # в процентах


class IRPHistoryUpdate(BaseModel):
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    value: Optional[float] = None


class IRPHistoryOut(BaseModel):
    id: int
    date_from: date
    date_to: date
    value: float
    created_at: datetime

    class Config:
        from_attributes = True


# ── Справочник КТР/КРП ──

class KTRReferenceRow(BaseModel):
    localization_min: float
    localization_max: float
    ktr_before: float
    ktr_after: float
    krp_irp: float


# ── Операции логистики ──

class LogisticsOperationOut(BaseModel):
    id: int
    seller_article: str
    nm_id: int
    operation_type: str
    warehouse: str
    supply_number: str
    operation_date: date
    coef_fix_start: Optional[date]
    coef_fix_end: Optional[date]
    warehouse_coef: float
    ktr_value: float
    irp_value: float
    base_first_liter: float
    base_per_liter: float
    volume_card_liters: float
    volume_nomenclature_liters: float
    calculated_wb_volume: float
    retail_price: float
    expected_logistics: float
    actual_logistics: float
    difference: float
    operation_status: str
    dimensions_status: str
    volume_difference: float
    ktr_needs_check: bool
    tariff_missing: bool

    class Config:
        from_attributes = True


class LogisticsArticleSummary(BaseModel):
    seller_article: str
    nm_id: int
    operations_count: int
    total_expected: float
    total_actual: float
    total_difference: float
    volume_card: float
    volume_nomenclature: float
    dimensions_status: str
    overpay_count: int
    saving_count: int
    match_count: int


class LogisticsSummary(BaseModel):
    total_expected: float
    total_actual: float
    total_difference: float
    total_overpay: float
    total_saving: float
    articles_total: int
    articles_overpay: int
    articles_saving: int
    articles_match: int
    current_ktr: Optional[float]
    current_irp: Optional[float]
    warnings_count: int


class LogisticsOperationsResponse(BaseModel):
    operations: list[LogisticsOperationOut]
    total: int
    page: int
    page_size: int


class LogisticsArticleResponse(BaseModel):
    articles: list[LogisticsArticleSummary]
    total: int


# ── Габариты ──

class DimensionsComparisonOut(BaseModel):
    seller_article: str
    nm_id: int
    sku_name: Optional[str] = ""
    volume_card: float
    length_card: float
    width_card: float
    height_card: float
    volume_nomenclature: float
    length_nom: float
    width_nom: float
    height_nom: float
    volume_difference: float
    dimensions_status: str
    card_updated_at: Optional[datetime] = None


class DimensionsResponse(BaseModel):
    items: list[DimensionsComparisonOut]
    total: int


# ── Фильтры ──

class LogisticsFilterOptions(BaseModel):
    warehouses: list[str]
    articles: list[str]
    weeks: list[str]  # "2026-W12" формат
    operation_types: list[str]


# ── Синхронизация ──

class SyncResult(BaseModel):
    processed: int = 0
    updated: int = 0
    warnings: int = 0
    error: Optional[str] = None
