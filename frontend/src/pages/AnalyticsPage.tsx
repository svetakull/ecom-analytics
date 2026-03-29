import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  AlertTriangle,
  AlertCircle,
  CheckCircle,
  ChevronDown,
  ChevronUp,
  TrendingUp,
  TrendingDown,
  Minus,
  Package,
  Search,
  ArrowUpRight,
  ArrowDownRight,
} from 'lucide-react'
import clsx from 'clsx'

import { analyticsApi } from '@/api/endpoints'
import type {
  AnalyticsData,
  SKUAnalytics,
  StoreSummaryPeriod,
  MetricZone,
  Recommendation,
  MarginDiagnostic,
} from '@/types'

// ── Форматирование ───────────────────────────────────────────────────────

const fmtNum = (n: number, d = 0) =>
  n.toLocaleString('ru-RU', { minimumFractionDigits: d, maximumFractionDigits: d })
const fmtRub = (n: number) => `${fmtNum(n, 0)} \u20BD`
const fmtPct = (n: number) => `${n >= 0 ? '+' : ''}${fmtNum(n, 1)}%`
const fmtPP = (n: number) => `${n >= 0 ? '+' : ''}${fmtNum(n, 1)} \u043F.\u043F.`

// ── Метрики: отображение ─────────────────────────────────────────────────

const METRIC_LABELS: Record<string, string> = {
  orders: 'Заказы',
  buyout: '% выкупа',
  margin: 'Маржа',
  traffic: 'Показы / переходы',
  drr: 'ДРР',
}

const PERIOD_LABELS: Record<string, string> = {
  yesterday: 'Вчера',
  week: '7 дней',
  month: 'Месяц',
}

const ZONE_COLORS: Record<string, string> = {
  green: 'bg-green-50 text-green-800',
  yellow: 'bg-yellow-50 text-yellow-800',
  red: 'bg-red-50 text-red-800',
}

const ZONE_BG: Record<string, string> = {
  green: 'bg-green-50',
  yellow: 'bg-yellow-50',
  red: 'bg-red-50',
}

const ZONE_BADGE: Record<string, { bg: string; text: string; label: string }> = {
  green: { bg: 'bg-green-100', text: 'text-green-700', label: 'Норма' },
  yellow: { bg: 'bg-yellow-100', text: 'text-yellow-700', label: 'Внимание' },
  red: { bg: 'bg-red-100', text: 'text-red-700', label: 'Критично' },
}

const REC_COLORS: Record<string, string> = {
  info: 'bg-blue-50 border-blue-200 text-blue-800',
  warning: 'bg-yellow-50 border-yellow-200 text-yellow-800',
  critical: 'bg-red-50 border-red-200 text-red-800',
}

const REC_ICONS: Record<string, React.ReactNode> = {
  TrendingUp: <TrendingUp size={16} />,
  TrendingDown: <TrendingDown size={16} />,
  Package: <Package size={16} />,
  Search: <Search size={16} />,
}

// ── Главный компонент ────────────────────────────────────────────────────

export default function AnalyticsPage() {
  const [channels, setChannels] = useState<string[]>([])
  const [articleFilter, setArticleFilter] = useState('')
  const [showNormal, setShowNormal] = useState(false)

  const { data, isLoading, error } = useQuery<{ data: AnalyticsData }>({
    queryKey: ['rnp-analytics', channels, articleFilter],
    queryFn: () =>
      analyticsApi.dashboard({
        channels: channels.length > 0 ? channels : undefined,
        article: articleFilter || undefined,
      }),
    staleTime: 60_000,
  })

  const analytics = data?.data

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-600" />
      </div>
    )
  }

  if (error || !analytics) {
    return (
      <div className="p-6 text-red-600">
        Ошибка загрузки аналитики: {(error as Error)?.message || 'нет данных'}
      </div>
    )
  }

  return (
    <div className="p-4 space-y-6 max-w-[1400px] mx-auto">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold text-gray-900">Аналитика РнП</h1>
        <div className="flex gap-2 items-center">
          {/* Channel filter */}
          <div className="flex gap-1">
            {['wb', 'ozon'].map((ch) => (
              <button
                key={ch}
                onClick={() =>
                  setChannels((prev) =>
                    prev.includes(ch) ? prev.filter((c) => c !== ch) : [...prev, ch]
                  )
                }
                className={clsx(
                  'px-3 py-1.5 text-xs font-medium rounded-md border transition-colors',
                  channels.includes(ch)
                    ? 'bg-indigo-600 text-white border-indigo-600'
                    : 'bg-white text-gray-600 border-gray-300 hover:bg-gray-50'
                )}
              >
                {ch.toUpperCase()}
              </button>
            ))}
          </div>
          {/* Article search */}
          <input
            type="text"
            placeholder="Поиск артикула..."
            value={articleFilter}
            onChange={(e) => setArticleFilter(e.target.value)}
            className="px-3 py-1.5 text-sm border border-gray-300 rounded-md w-48 focus:ring-1 focus:ring-indigo-500 focus:border-indigo-500"
          />
        </div>
      </div>

      {/* Store Summary */}
      <StoreSummarySection summary={analytics.store_summary} />

      {/* Critical SKUs */}
      <SKUSection
        title="Требуют действий"
        count={analytics.critical_count}
        icon={<AlertCircle size={20} className="text-red-600" />}
        bgClass="bg-red-50"
        borderClass="border-red-200"
        skus={analytics.critical_skus}
        defaultOpen
      />

      {/* Warning SKUs */}
      <SKUSection
        title="Внимание"
        count={analytics.warning_count}
        icon={<AlertTriangle size={20} className="text-yellow-600" />}
        bgClass="bg-yellow-50"
        borderClass="border-yellow-200"
        skus={analytics.warning_skus}
        defaultOpen
      />

      {/* Normal SKUs */}
      <SKUSection
        title="В норме"
        count={analytics.normal_count}
        icon={<CheckCircle size={20} className="text-green-600" />}
        bgClass="bg-green-50"
        borderClass="border-green-200"
        skus={analytics.normal_skus}
        defaultOpen={false}
      />
    </div>
  )
}

// ── Store Summary ────────────────────────────────────────────────────────

function StoreSummarySection({ summary }: { summary: AnalyticsData['store_summary'] }) {
  const periods: Array<{ key: 'yesterday' | 'week' | 'month'; label: string }> = [
    { key: 'yesterday', label: 'Вчера' },
    { key: 'week', label: '7 дней' },
    { key: 'month', label: 'Месяц' },
  ]

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-5">
      <h2 className="text-sm font-semibold text-gray-700 mb-4 flex items-center gap-2">
        <span className="text-lg">📊</span> Сводка магазина
        <span className="text-xs font-normal text-gray-400 ml-2">
          {summary.total_count} SKU: {summary.critical_count} крит. / {summary.warning_count} вним. / {summary.normal_count} норма
        </span>
      </h2>

      <div className="grid grid-cols-3 gap-4">
        {periods.map(({ key, label }) => {
          const p = summary[key] as StoreSummaryPeriod
          if (!p) return null
          return (
            <div key={key} className="bg-gray-50 rounded-lg p-4">
              <div className="text-xs font-medium text-gray-500 mb-3">{label}</div>
              <div className="grid grid-cols-2 gap-3">
                <SummaryMetric label="Заказы" value={fmtRub(p.orders_rub)} delta={p.deltas.orders_rub} fmt="pct" />
                <SummaryMetric label="Маржа" value={`${fmtNum(p.margin_pct, 1)}%`} delta={p.deltas.margin_pct} fmt="pp" />
                <SummaryMetric label="Выкуп" value={`${fmtNum(p.buyout_pct, 1)}%`} delta={0} fmt="pp" />
                <SummaryMetric label="ДРР" value={`${fmtNum(p.drr_pct, 1)}%`} delta={p.deltas.drr_pct} fmt="pp" inverse />
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

function SummaryMetric({
  label,
  value,
  delta,
  fmt,
  inverse = false,
}: {
  label: string
  value: string
  delta: number
  fmt: 'pct' | 'pp'
  inverse?: boolean
}) {
  const isGood = inverse ? delta < 0 : delta > 0
  const isBad = inverse ? delta > 0 : delta < 0
  const colorClass = delta === 0 ? 'text-gray-400' : isGood ? 'text-green-600' : 'text-red-600'
  const formatted = fmt === 'pct' ? fmtPct(delta) : fmtPP(delta)
  const Icon = delta > 0 ? ArrowUpRight : delta < 0 ? ArrowDownRight : Minus

  return (
    <div>
      <div className="text-[11px] text-gray-400">{label}</div>
      <div className="text-sm font-semibold text-gray-900">{value}</div>
      {delta !== 0 && (
        <div className={clsx('flex items-center gap-0.5 text-[11px] font-medium', colorClass)}>
          <Icon size={12} />
          {formatted}
        </div>
      )}
    </div>
  )
}

// ── SKU Section ──────────────────────────────────────────────────────────

function SKUSection({
  title,
  count,
  icon,
  bgClass,
  borderClass,
  skus,
  defaultOpen,
}: {
  title: string
  count: number
  icon: React.ReactNode
  bgClass: string
  borderClass: string
  skus: SKUAnalytics[]
  defaultOpen: boolean
}) {
  const [open, setOpen] = useState(defaultOpen)

  if (count === 0) return null

  return (
    <div className={clsx('rounded-lg border', borderClass)}>
      <button
        onClick={() => setOpen(!open)}
        className={clsx(
          'w-full flex items-center justify-between px-4 py-3 rounded-t-lg',
          bgClass
        )}
      >
        <div className="flex items-center gap-2">
          {icon}
          <span className="font-semibold text-sm text-gray-900">
            {title} ({count})
          </span>
        </div>
        {open ? <ChevronUp size={18} /> : <ChevronDown size={18} />}
      </button>

      {open && (
        <div className="p-3 space-y-3 bg-white rounded-b-lg">
          {skus.map((sku) => (
            <SKUCard key={`${sku.sku_id}-${sku.channel_id}`} sku={sku} />
          ))}
        </div>
      )}
    </div>
  )
}

// ── SKU Card ─────────────────────────────────────────────────────────────

function SKUCard({ sku }: { sku: SKUAnalytics }) {
  const [expanded, setExpanded] = useState(sku.overall_zone === 'red')
  const badge = ZONE_BADGE[sku.overall_zone]

  // Собираем зоны в матрицу: metric → period → zone
  const zoneMap: Record<string, Record<string, MetricZone>> = {}
  for (const z of sku.metrics_zones) {
    if (!zoneMap[z.metric]) zoneMap[z.metric] = {}
    zoneMap[z.metric][z.period] = z
  }

  return (
    <div className="border border-gray-200 rounded-lg overflow-hidden">
      {/* Header */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center gap-3 px-4 py-3 hover:bg-gray-50 transition-colors"
      >
        {/* Photo */}
        {sku.photo_url ? (
          <img src={sku.photo_url} alt="" className="w-10 h-10 rounded object-cover flex-shrink-0" />
        ) : (
          <div className="w-10 h-10 rounded bg-gray-200 flex-shrink-0" />
        )}

        {/* Info */}
        <div className="flex-1 text-left min-w-0">
          <div className="flex items-center gap-2">
            <span className="font-semibold text-sm text-gray-900 truncate">
              {sku.seller_article}
            </span>
            <span className="text-[10px] px-1.5 py-0.5 rounded bg-gray-100 text-gray-500 uppercase">
              {sku.channel_type}
            </span>
            <span className={clsx('text-[10px] px-1.5 py-0.5 rounded font-medium', badge.bg, badge.text)}>
              {badge.label}
            </span>
          </div>
          <div className="text-xs text-gray-500 truncate">{sku.name}</div>
        </div>

        {/* Stock badge */}
        <div className={clsx(
          'text-xs font-medium px-2 py-1 rounded',
          sku.turnover_days < 7 ? 'bg-red-100 text-red-700' :
          sku.turnover_days < 14 ? 'bg-yellow-100 text-yellow-700' :
          'bg-gray-100 text-gray-600'
        )}>
          {sku.turnover_days < 999 ? `${Math.round(sku.turnover_days)}д` : '—'} / {sku.current_stock} шт
        </div>

        {expanded ? <ChevronUp size={16} className="text-gray-400" /> : <ChevronDown size={16} className="text-gray-400" />}
      </button>

      {/* Expanded content */}
      {expanded && (
        <div className="border-t border-gray-100 px-4 py-3 space-y-3">
          {/* Metrics table */}
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-gray-400">
                  <th className="text-left font-medium py-1 pr-3 w-36">Метрика</th>
                  {(['yesterday', 'week', 'month'] as const).map((p) => (
                    <th key={p} className="text-center font-medium py-1 px-2 w-28">
                      {PERIOD_LABELS[p]}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {(['orders', 'buyout', 'margin', 'traffic', 'drr'] as const).map((metric) => (
                  <tr key={metric}>
                    <td className="py-1.5 pr-3 font-medium text-gray-700">{METRIC_LABELS[metric]}</td>
                    {(['yesterday', 'week', 'month'] as const).map((period) => {
                      const z = zoneMap[metric]?.[period]
                      if (!z) return <td key={period} className="text-center py-1.5 px-2">—</td>

                      const isAbsolute = ['buyout', 'margin', 'drr'].includes(metric)
                      const formatted = isAbsolute ? fmtPP(z.delta) : fmtPct(z.delta)

                      return (
                        <td key={period} className="py-1.5 px-2">
                          <div className={clsx(
                            'text-center rounded px-2 py-1 font-medium',
                            ZONE_BG[z.zone],
                            z.zone === 'red' ? 'text-red-700' :
                            z.zone === 'yellow' ? 'text-yellow-700' : 'text-green-700'
                          )}>
                            <DeltaArrow delta={z.delta} metric={metric} />
                            {formatted}
                          </div>
                        </td>
                      )
                    })}
                  </tr>
                ))}
                {/* Turnover — informational */}
                <tr>
                  <td className="py-1.5 pr-3 font-medium text-gray-700">Оборачиваемость</td>
                  <td colSpan={3} className="text-center py-1.5 px-2 text-gray-600">
                    {sku.turnover_days < 999 ? `${Math.round(sku.turnover_days)} дней` : 'нет данных'}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          {/* Recommendations */}
          {sku.recommendations.length > 0 && (
            <div className="space-y-2">
              {sku.recommendations.map((rec, i) => (
                <RecommendationCard key={i} rec={rec} />
              ))}
            </div>
          )}

          {/* Margin diagnostic */}
          {sku.margin_diagnostic && (
            <MarginDiagnosticCard diag={sku.margin_diagnostic} />
          )}
        </div>
      )}
    </div>
  )
}

// ── Delta Arrow ──────────────────────────────────────────────────────────

function DeltaArrow({ delta, metric }: { delta: number; metric: string }) {
  if (Math.abs(delta) < 0.5) return <Minus size={10} className="inline mr-0.5" />
  // DRR: growth is bad, traffic: both directions are bad
  if (delta > 0) return <ArrowUpRight size={10} className="inline mr-0.5" />
  return <ArrowDownRight size={10} className="inline mr-0.5" />
}

// ── Recommendation Card ──────────────────────────────────────────────────

function RecommendationCard({ rec }: { rec: Recommendation }) {
  return (
    <div className={clsx('flex items-start gap-2 px-3 py-2 rounded-md border text-xs', REC_COLORS[rec.severity])}>
      <span className="mt-0.5 flex-shrink-0">{REC_ICONS[rec.icon] || <AlertTriangle size={16} />}</span>
      <div>
        <div className="font-semibold">{rec.title}</div>
        <div className="mt-0.5 opacity-80">{rec.description}</div>
      </div>
    </div>
  )
}

// ── Margin Diagnostic ────────────────────────────────────────────────────

function MarginDiagnosticCard({ diag }: { diag: MarginDiagnostic }) {
  return (
    <div className="bg-orange-50 border border-orange-200 rounded-md px-3 py-2 text-xs text-orange-800">
      <div className="font-semibold flex items-center gap-1">
        <AlertTriangle size={14} />
        Диагностика маржи: {diag.title}
      </div>
      <div className="mt-0.5 opacity-80">{diag.description}</div>
    </div>
  )
}
