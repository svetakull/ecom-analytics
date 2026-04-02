export type UserRole = 'owner' | 'finance_manager' | 'marketer' | 'mp_manager' | 'warehouse' | 'assistant'

export interface User {
  id: number
  email: string
  name: string
  role: UserRole
  is_active: boolean
}

export interface KPICard {
  title: string
  value: number
  unit: string
  trend_pct: number
  trend_direction: 'up' | 'down' | 'flat'
}

export interface StockAlert {
  sku_id: number
  seller_article: string
  name: string
  channel: string
  stock_qty: number
  turnover_days: number
}

export interface DashboardData {
  orders_today: KPICard
  revenue_today: KPICard
  margin_avg: KPICard
  tacos_avg: KPICard
  stock_alerts: StockAlert[]
  sales_chart: Array<{ date: string; orders_qty: number; orders_rub: number }>
}

export interface RnPSKURow {
  sku_id: number
  seller_article: string
  name: string
  channel: string
  channel_type: string
  orders_qty: number
  orders_rub: number
  sales_forecast_qty: number
  sales_forecast_rub: number
  buyout_rate_pct: number
  price_before_spp: number
  price_after_spp: number
  spp_pct: number
  stock_qty: number
  turnover_days: number
  margin_forecast_pct: number
  gross_margin_per_unit: number
  gross_margin_rub: number
  tacos: number
  ad_spend: number
}

export interface RnPData {
  date: string
  total_orders_qty: number
  total_orders_rub: number
  total_margin_pct: number
  total_tacos: number
  rows: RnPSKURow[]
}

export interface Order {
  id: number
  sku_id: number
  seller_article: string
  sku_name: string
  channel: string
  order_date: string
  qty: number
  price: number
  status: string
}

export interface SalesSummary {
  date_from: string
  date_to: string
  total_orders_qty: number
  total_orders_rub: number
  total_sales_qty: number
  total_sales_rub: number
  total_returns_qty: number
  buyout_rate_pct: number
  avg_order_price: number
}

export interface SalesDynamic {
  date: string
  orders_qty: number
  orders_rub: number
  sales_qty: number
  sales_rub: number
}

export interface SKUOut {
  id: number
  seller_article: string
  name: string
  category: string | null
  brand: string | null
  color: string | null
  is_active: boolean
  channels: string[]
  total_stock: number
  avg_cogs: number
}

export interface Channel {
  id: number
  name: string
  type: string
  is_active: boolean
  commission_pct: number
}

// ── РнП Pivot (новый формат) ──────────────────────────────────────────────

export interface AdTypeMetrics {
  budget: number
  impressions: number
  clicks: number
  orders: number
  ctr: number       // %
  cr: number        // %
  cpc: number       // ₽
  cpm: number       // ₽
  cpo_all: number   // ₽ — стоимость заказа (все заказы)
  cpo_ad: number    // ₽ — стоимость рекламного заказа
  cps: number       // ₽ — стоимость 1 продажи
}

export interface RnPDayMetrics {
  orders_qty: number
  orders_rub: number
  sales_qty: number
  sales_rub: number
  returns_qty: number
  cancellations_qty: number
  forecast_sales_qty: number
  forecast_sales_rub: number
  price_before_spp: number
  price_after_spp: number
  spp_pct: number
  stock_wb: number
  in_way_to_client: number
  in_way_from_client: number
  frozen_capital: number
  buyout_rate_pct: number
  margin_pct: number
  roi_pct: number
  profit_per_unit: number
  profit_total: number
  commission_per_unit: number
  commission_pct: number
  logistics_per_unit: number
  storage_per_unit: number
  cogs_per_unit: number
  tax_usn_per_unit: number
  tax_nds_per_unit: number
  tax_total_per_unit: number
  return_fee_rub: number        // сбор за возврат Lamoda (29 руб/ед × возвраты)
  drr_orders_pct: number
  drr_sales_pct: number
  ad_spend: number
  ad_orders_qty: number
  ad_total: AdTypeMetrics
  ad_search: AdTypeMetrics
  ad_recommend: AdTypeMetrics
  // Воронка карточки (nm-report)
  open_card_count: number        // переходы в карточку (всего)
  ad_clicks_count: number        // переходы рекламные
  organic_clicks_count: number   // переходы органические
  organic_clicks_pct: number     // доля органических переходов, %
  add_to_cart_count: number      // добавили в корзину, шт
  cart_from_card_pct: number     // конверсия переход→корзина, %
  order_from_cart_pct: number    // конверсия корзина→заказ, %
}

export interface RnPPivotSKU {
  sku_id: number
  channel_id: number
  seller_article: string
  name: string
  channel_type: string
  channel_name: string
  wb_article: string
  photo_url: string
  // Итого
  total_orders_qty: number
  total_orders_rub: number
  total_sales_qty: number
  total_returns_qty: number
  avg_price_before_spp: number
  avg_price_after_spp: number
  avg_spp_pct: number
  current_stock: number
  turnover_days: number
  buyout_rate_pct: number
  buyout_rate_is_manual: boolean
  logistics_per_unit_avg: number
  logistics_is_manual: boolean
  commission_pct_avg: number
  commission_is_manual: boolean
  avg_margin_pct: number
  cogs_per_unit: number
  wb_rating: number | null       // рейтинг по отзывам WB (0.00–5.00)
  // По дням: ключ = "YYYY-MM-DD"
  days: Record<string, RnPDayMetrics>
}

export interface RnPPivotData {
  ref_date: string
  days: string[]       // ['2026-03-13', '2026-03-12', ...]
  skus: RnPPivotSKU[]
}

// ── Аналитика РнП (зоны, рекомендации) ────────────────────────────────────

export interface AnalyticsPeriodMetrics {
  orders_qty: number
  orders_rub: number
  buyout_rate_pct: number
  margin_pct: number
  impressions: number
  drr_pct: number
  ad_spend: number
  cart_from_card_pct: number
  order_from_cart_pct: number
}

export interface AnalyticsPeriodDeltas {
  orders_qty: number   // % change
  orders_rub: number   // % change
  buyout_rate_pct: number  // п.п.
  margin_pct: number       // п.п.
  drr_pct: number          // п.п.
  impressions: number      // % change
}

export interface AnalyticsPeriod {
  current: AnalyticsPeriodMetrics
  baseline: AnalyticsPeriodMetrics
  deltas: AnalyticsPeriodDeltas
  agg_type: 'day' | 'avg' | 'sum'
}

export interface MetricZone {
  metric: 'orders' | 'buyout' | 'margin' | 'traffic' | 'drr'
  period: 'yesterday' | 'week' | 'month'
  delta: number
  zone: 'green' | 'yellow' | 'red'
}

export interface Recommendation {
  type: string
  icon: string
  title: string
  description: string
  severity: 'info' | 'warning' | 'critical'
  branch?: string
}

export interface MarginDiagnostic {
  diagnosis: string
  title: string
  description: string
  linked_rec: string | null
}

export interface SKUAnalytics {
  sku_id: number
  channel_id: number
  seller_article: string
  name: string
  channel_type: string
  photo_url: string
  wb_rating: number | null
  current_stock: number
  turnover_days: number
  periods: Record<'yesterday' | 'week' | 'month', AnalyticsPeriod>
  metrics_zones: MetricZone[]
  overall_zone: 'green' | 'yellow' | 'red'
  recommendations: Recommendation[]
  margin_diagnostic: MarginDiagnostic | null
  priority_level: number
}

export interface StoreSummaryPeriod {
  orders_qty: number
  orders_rub: number
  margin_pct: number
  buyout_pct: number
  drr_pct: number
  deltas: {
    orders_qty: number
    orders_rub: number
    margin_pct: number
    drr_pct: number
  }
}

export interface StoreSummary {
  yesterday: StoreSummaryPeriod
  week: StoreSummaryPeriod
  month: StoreSummaryPeriod
  critical_count: number
  warning_count: number
  normal_count: number
  total_count: number
}

export interface AnalyticsData {
  ref_date: string
  store_summary: StoreSummary
  critical_skus: SKUAnalytics[]
  critical_count: number
  warning_skus: SKUAnalytics[]
  warning_count: number
  normal_skus: SKUAnalytics[]
  normal_count: number
}

export interface AnalyticsThreshold {
  key: string
  value: number
  description: string
}

// ── Оцифровка (фактическая P&L-аналитика) ────────────────────────────────

export interface OtsifrovkaRow {
  sku_id: number
  channel_id: number
  seller_article: string
  name: string
  channel_type: string
  channel_name: string
  photo_url: string
  mp_article: string
  // Заказы
  orders_qty: number
  orders_rub: number
  // Продажи (для Ozon = цена покупателя, для WB/Lamoda = цена продавца)
  sales_qty: number
  sales_rub: number
  avg_price: number
  // Реализация и компенсация (Ozon)
  realization_rub: number
  compensation_rub: number
  // Возвраты
  returns_qty: number
  returns_rub: number
  return_rate_pct: number
  // Налоговая база
  tax_base_rub: number
  // Затраты
  commission_rub: number
  logistics_rub: number
  storage_rub: number
  return_fee_rub: number
  fines_rub: number
  acceptance_rub: number
  other_deductions_rub: number
  ad_spend_rub: number
  ad_search_pct: number
  ad_recommend_pct: number
  tax_rub: number
  cogs_rub: number
  cogs_per_unit: number
  // Перечисление и ДРР
  payout_rub: number
  drr_orders_pct: number
  drr_sales_pct: number
  // P&L
  total_costs_rub: number
  profit_rub: number
  margin_pct: number
  // Остаток
  current_stock: number
  turnover_days: number
  // Аналитика
  buyout_rate_pct: number
  revenue_share_pct: number
  abc_revenue: 'A' | 'B' | 'C'
  abc_profit: 'A' | 'B' | 'C'
}

export interface OtsifrovkaSummary {
  orders_qty: number
  orders_rub: number
  sales_qty: number
  sales_rub: number
  realization_rub: number
  compensation_rub: number
  returns_qty: number
  returns_rub: number
  tax_base_rub: number
  commission_rub: number
  logistics_rub: number
  storage_rub: number
  return_fee_rub: number
  fines_rub: number
  ad_spend_rub: number
  ad_search_pct: number
  ad_recommend_pct: number
  tax_rub: number
  cogs_rub: number
  profit_rub: number
  payout_rub: number
  margin_pct: number
}

export interface OtsifrovkaData {
  date_from: string
  date_to: string
  days: number
  summary: OtsifrovkaSummary
  rows: OtsifrovkaRow[]
}

// ── Логистика и габариты ──

export interface LogisticsOperation {
  id: number
  seller_article: string
  nm_id: number
  operation_type: string
  warehouse: string
  supply_number: string
  operation_date: string
  coef_fix_start: string | null
  coef_fix_end: string | null
  warehouse_coef: number
  ktr_value: number
  irp_value: number
  base_first_liter: number
  base_per_liter: number
  volume_card_liters: number
  volume_nomenclature_liters: number
  calculated_wb_volume: number
  retail_price: number
  expected_logistics: number
  actual_logistics: number
  difference: number
  operation_status: string
  dimensions_status: string
  volume_difference: number
  ktr_needs_check: boolean
  tariff_missing: boolean
}

export interface LogisticsOperationsResponse {
  operations: LogisticsOperation[]
  total: number
  page: number
  page_size: number
}

export interface LogisticsArticleSummary {
  seller_article: string
  nm_id: number
  operations_count: number
  total_expected: number
  total_actual: number
  total_difference: number
  volume_card: number
  volume_nomenclature: number
  dimensions_status: string
  overpay_count: number
  saving_count: number
  match_count: number
}

export interface LogisticsSummary {
  total_expected: number
  total_actual: number
  total_difference: number
  total_overpay: number
  total_saving: number
  articles_total: number
  articles_overpay: number
  articles_saving: number
  articles_match: number
  current_ktr: number | null
  current_irp: number | null
  warnings_count: number
}

export interface DimensionsComparison {
  seller_article: string
  nm_id: number
  sku_name: string
  volume_card: number
  length_card: number
  width_card: number
  height_card: number
  volume_nomenclature: number
  length_nom: number
  width_nom: number
  height_nom: number
  volume_difference: number
  dimensions_status: string
  card_updated_at: string | null
}

export interface KTRHistoryRecord {
  id: number
  date_from: string
  date_to: string
  value: number
  created_at: string
}

export interface IRPHistoryRecord {
  id: number
  date_from: string
  date_to: string
  value: number
  created_at: string
}

export interface KTRReferenceRow {
  localization_min: number
  localization_max: number
  ktr_before: number
  ktr_after: number
  krp_irp: number
}

export interface LogisticsFilterOptions {
  warehouses: string[]
  articles: string[]
  weeks: string[]
  operation_types: string[]
}

export interface LogisticsSyncResult {
  processed: number
  updated: number
  warnings: number
  error: string | null
}
