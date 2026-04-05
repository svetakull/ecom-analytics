/**
 * РнП — Рука на пульсе
 * Формат: пивот-таблица Метрика × День, аналог WB Аналитики
 */
import { useState, useEffect, useRef, useCallback, useMemo, Fragment } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { format, subDays, startOfDay } from 'date-fns'
import { rnpApi } from '@/api/endpoints'
import type { RnPPivotSKU, RnPDayMetrics, AdTypeMetrics } from '@/types'
import DateRangePicker, { type DateRange } from '@/components/DateRangePicker'
import { MultiSelectDropdown, WBIcon, OzonIcon, ChannelIcon } from '@/components/MultiSelectDropdown'

// ── Форматирование ──────────────────────────────────────────────────────────

const fmtNum = (n: number, decimals = 0) =>
  n === 0 ? '0' : n.toLocaleString('ru-RU', { maximumFractionDigits: decimals })

const fmtPct = (n: number) => Math.round(n) + '%'
const fmtPct2 = (n: number) => n.toFixed(2) + '%'
const fmtRub = (n: number) => fmtNum(n)

const DOW = ['вс', 'пн', 'вт', 'ср', 'чт', 'пт', 'сб']
const MONTHS_SHORT = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']

function formatDayLabel(iso: string) {
  const d = new Date(iso + 'T00:00:00')
  return `${d.getDate()} ${MONTHS_SHORT[d.getMonth()]}. (${DOW[d.getDay()]})`
}

// ── Тренд-стрелки ──────────────────────────────────────────────────────────

function TrendBadge({ current, prev, inverseGood = false }: { current: number; prev: number | null; inverseGood?: boolean }) {
  if (prev === null) return null
  const diff = current - prev
  if (Math.abs(diff) < 0.0001) return null
  const up = diff > 0
  const good = inverseGood ? !up : up
  return <span className={`ml-0.5 text-[10px] ${good ? 'text-green-500' : 'text-red-500'}`}>{up ? '↑' : '↓'}</span>
}

// ── Sparkline ──────────────────────────────────────────────────────────────

function MiniSparkline({ values }: { values: number[] }) {
  const max = Math.max(...values)
  const W = 44, H = 16, n = values.length
  if (max === 0) {
    return (
      <svg width={W} height={H} className="opacity-25">
        {values.map((_, i) => <rect key={i} x={i*(W/n)+1} y={H-2} width={W/n-2} height={2} fill="#9CA3AF" rx={1}/>)}
      </svg>
    )
  }
  return (
    <svg width={W} height={H}>
      {values.map((v, i) => {
        const h = Math.max(2, Math.round((v / max) * (H - 2)))
        return <rect key={i} x={i*(W/n)+1} y={H-h} width={W/n-2} height={h} fill={i===n-1?'#6366F1':'#10B981'} rx={1}/>
      })}
    </svg>
  )
}

// ── Инлайн-редактирование ──────────────────────────────────────────────────

interface EditableCellProps {
  value: number
  isManual: boolean
  suffix?: string
  onSave: (val: number | null) => void
  formatDisplay?: (v: number) => string
  valueClassName?: string
}

function EditableCell({ value, isManual, suffix = '', onSave, formatDisplay, valueClassName }: EditableCellProps) {
  const [editing, setEditing] = useState(false)
  const [inputVal, setInputVal] = useState('')
  const inputRef = useRef<HTMLInputElement>(null)

  const startEdit = () => {
    setInputVal(value > 0 ? value.toFixed(1) : '')
    setEditing(true)
    setTimeout(() => inputRef.current?.select(), 0)
  }

  const commit = () => {
    const parsed = parseFloat(inputVal.replace(',', '.'))
    if (inputVal.trim() === '' || isNaN(parsed)) {
      onSave(null) // сбросить на расчётное
    } else {
      onSave(parsed)
    }
    setEditing(false)
  }

  const display = formatDisplay ? formatDisplay(value) : `${fmtNum(value, 1)}${suffix}`

  if (editing) {
    return (
      <div className="flex items-center gap-1">
        <input
          ref={inputRef}
          type="text"
          value={inputVal}
          onChange={(e) => setInputVal(e.target.value)}
          onBlur={commit}
          onKeyDown={(e) => { if (e.key === 'Enter') commit(); if (e.key === 'Escape') setEditing(false) }}
          className="w-16 px-1 py-0.5 text-xs border border-indigo-400 rounded focus:outline-none focus:ring-1 focus:ring-indigo-400 tabular-nums"
          autoFocus
        />
        {suffix && <span className="text-[10px] text-gray-400">{suffix}</span>}
        <button onClick={() => { onSave(null); setEditing(false) }} title="Сбросить" className="text-[10px] text-gray-400 hover:text-red-400">✕</button>
      </div>
    )
  }

  return (
    <button
      onClick={startEdit}
      className={`group flex items-center gap-1 px-1 py-0.5 rounded hover:bg-indigo-50 transition-colors text-xs tabular-nums ${isManual ? 'text-indigo-700 font-semibold' : (valueClassName ?? 'text-gray-800')}`}
      title={isManual ? 'Значение задано вручную. Нажмите для редактирования.' : 'Нажмите для ввода вручную'}
    >
      <span>{value === 0 && !isManual ? '—' : display}</span>
      {isManual
        ? <span className="text-indigo-400 text-[9px]">✎</span>
        : <span className="text-gray-300 group-hover:text-indigo-400 text-[9px] opacity-0 group-hover:opacity-100 transition-opacity">✎</span>
      }
    </button>
  )
}

// ── Метрики ────────────────────────────────────────────────────────────────

interface MetricDef {
  key: keyof RnPDayMetrics
  label: string
  format: (v: number) => string
  inverseGood?: boolean
  aggType: 'sum' | 'avg' | 'last'
  editable?: 'buyout' | 'logistics' | 'commission'
  colorScale?: boolean         // цветовая шкала: красный (мин) → зелёный (макс)
  colorScaleInverse?: boolean  // цветовая шкала: зелёный (мин) → красный (макс)
}

function scaleColor(val: number, min: number, max: number, inverse = false): string {
  if (max <= min) return ''
  const t = Math.max(0, Math.min(1, (val - min) / (max - min)))
  const h = Math.round((inverse ? 1 - t : t) * 120)   // 0°=красный, 120°=зелёный
  return `hsl(${h}, 72%, 88%)`
}

const MAIN_METRICS: MetricDef[] = [
  { key: 'margin_pct',          label: 'Прогноз. маржинальность, %',      format: fmtPct,                 aggType: 'avg' },
  { key: 'roi_pct',             label: 'ROI, %',                           format: fmtPct,                 aggType: 'avg' },
  { key: 'profit_per_unit',     label: 'Прогноз. прибыль на 1 ед., ₽',    format: fmtRub,                 aggType: 'avg' },
  { key: 'profit_total',        label: 'Прогноз. прибыль, ₽',             format: fmtRub,                 aggType: 'sum', colorScale: true },
  { key: 'orders_qty',          label: 'Факт. заказы, шт',                format: fmtNum,                 aggType: 'sum', colorScale: true },
  { key: 'orders_rub',          label: 'Факт. заказы, ₽',                 format: fmtRub,                 aggType: 'sum' },
  { key: 'buyout_rate_pct',     label: 'Процент выкупа, %',               format: fmtPct,                 aggType: 'avg', editable: 'buyout' },
  { key: 'forecast_sales_qty',  label: 'Прогноз. продажи, шт',            format: (v) => fmtNum(v, 1),    aggType: 'sum' },
  { key: 'forecast_sales_rub',  label: 'Прогноз. продажи до СПП, ₽',     format: fmtRub,                 aggType: 'sum' },
  { key: 'price_before_spp',    label: 'Цена до СПП, ₽',                  format: fmtRub,                 aggType: 'avg' },
  { key: 'price_after_spp',     label: 'Цена после СПП, ₽',               format: fmtRub,                 aggType: 'avg' },
  { key: 'spp_pct',             label: 'Скидка МП (СПП), %',              format: fmtPct, inverseGood:true, aggType: 'avg' },
  { key: 'stock_wb',            label: 'Остатки (на складах), шт',        format: fmtNum,                 aggType: 'last' },
  { key: 'in_way_to_client',    label: 'В пути до получателей, шт',       format: fmtNum,                 aggType: 'last' },
  { key: 'in_way_from_client',  label: 'В пути возвраты на склад, шт',    format: fmtNum, inverseGood:true, aggType: 'last' },
  { key: 'frozen_capital',      label: 'Заморожен. капитал, ₽',           format: fmtRub, inverseGood:true, aggType: 'last' },
  { key: 'drr_orders_pct',      label: 'ДРР по заказам, %',               format: fmtPct, inverseGood:true, aggType: 'avg' },
  { key: 'drr_sales_pct',       label: 'ДРР по продажам, %',              format: fmtPct, inverseGood:true, aggType: 'avg' },
]

const COSTS_METRICS: MetricDef[] = [
  { key: 'commission_pct',      label: 'Комиссия + экв., %',              format: fmtPct2, inverseGood:true, aggType: 'avg', editable: 'commission' },
  { key: 'commission_per_unit', label: 'Комиссия на 1 ед., ₽',           format: fmtRub, inverseGood:true, aggType: 'avg' },
  { key: 'logistics_per_unit',  label: 'Логистика на 1 ед., ₽',          format: fmtRub, inverseGood:true, aggType: 'avg', editable: 'logistics' },
  { key: 'storage_per_unit',    label: 'Хранение, ₽',                     format: fmtRub, inverseGood:true, aggType: 'sum' },
  { key: 'cogs_per_unit',       label: 'Себестоимость на 1 ед., ₽',      format: fmtRub, inverseGood:true, aggType: 'avg' },
  { key: 'tax_total_per_unit',  label: 'Налоги на 1 ед., ₽',              format: fmtRub, inverseGood:true, aggType: 'avg' },
]

function calcTotal(sku: RnPPivotSKU, days: string[], m: MetricDef): number {
  const vals = days.map((d) => (sku.days[d]?.[m.key] ?? 0) as number)
  if (m.aggType === 'sum') return vals.reduce((a, b) => a + b, 0)
  if (m.aggType === 'last') return vals[0] ?? 0
  const nz = vals.filter((v) => v > 0)
  return nz.length ? nz.reduce((a, b) => a + b, 0) / nz.length : 0
}

// ── Таблица метрик ─────────────────────────────────────────────────────────

interface MetricsTableProps {
  metrics: MetricDef[]
  sku: RnPPivotSKU
  days: string[]
  onSaveBuyout: (val: number | null) => void
  onSaveLogistics: (val: number | null) => void
  onSaveCommission: (val: number | null) => void
}

function MetricsTable({ metrics, sku, days, onSaveBuyout, onSaveLogistics, onSaveCommission }: MetricsTableProps) {
  return (
    <>
      {metrics.map((m, ri) => {
        const total = calcTotal(sku, days, m)
        const dayVals = days.map((d) => (sku.days[d]?.[m.key] ?? 0) as number)
        const sparkVals = [...dayVals].reverse()

        const hasScale = m.colorScale || m.colorScaleInverse
        const csMin = hasScale ? Math.min(...dayVals) : 0
        const csMax = hasScale ? Math.max(...dayVals) : 0

        const isBuyout = m.editable === 'buyout'
        const isLogistics = m.editable === 'logistics'
        const isCommission = m.editable === 'commission'
        const isEditable = isBuyout || isLogistics || isCommission
        const isManualTotal = isBuyout ? sku.buyout_rate_is_manual : isLogistics ? sku.logistics_is_manual : isCommission ? sku.commission_is_manual : false

        const totalCell = isEditable ? (
          <EditableCell
            value={isBuyout ? sku.buyout_rate_pct : isLogistics ? sku.logistics_per_unit_avg : sku.commission_pct_avg}
            isManual={isManualTotal}
            suffix={isBuyout ? '%' : isLogistics ? ' ₽' : '%'}
            formatDisplay={isBuyout ? (v) => v.toFixed(2) + '%' : isLogistics ? (v) => fmtNum(v, 1) + ' ₽' : (v) => v.toFixed(2) + '%'}
            onSave={isBuyout ? onSaveBuyout : isLogistics ? onSaveLogistics : onSaveCommission}
          />
        ) : (
          <span className="font-bold text-gray-800 tabular-nums">{m.format(total)}</span>
        )

        return (
          <tr key={m.key} className={`border-t border-gray-50 hover:bg-indigo-50/20 transition-colors ${ri % 2 === 0 ? '' : 'bg-gray-50/30'}`}>
            <td className="px-4 py-1.5 text-gray-700 font-medium sticky left-0 bg-white border-r border-gray-100 z-10">
              <div className="flex items-center gap-1">
                {m.label}
                {isManualTotal && (
                  <span className="text-[9px] px-1 py-0.5 bg-indigo-100 text-indigo-600 rounded font-normal">ручн.</span>
                )}
              </div>
            </td>
            <td className="px-3 py-1.5 text-right">
              {totalCell}
            </td>
            <td className="px-2 py-1.5">
              <div className="flex justify-center">
                <MiniSparkline values={sparkVals} />
              </div>
            </td>
            {dayVals.map((val, idx) => {
              const prev = idx < dayVals.length - 1 ? dayVals[idx + 1] : null
              const bg = hasScale ? scaleColor(val, csMin, csMax, !!m.colorScaleInverse) : ''
              return (
                <td
                  key={days[idx]}
                  className="px-3 py-1.5 text-right tabular-nums text-gray-800"
                  style={bg ? { backgroundColor: bg } : undefined}
                >
                  {m.format(val)}
                  <TrendBadge current={val} prev={prev} inverseGood={m.inverseGood} />
                </td>
              )
            })}
          </tr>
        )
      })}
    </>
  )
}

// ── Реклама: блок рекламных кампаний ────────────────────────────────────────

interface AdMetricDef {
  key: keyof AdTypeMetrics
  label: string
  format: (v: number) => string
  aggType: 'sum' | 'avg'
  inverseGood?: boolean
}

// Метрики с разбивкой Поиск / Полки
const AD_METRICS_6: AdMetricDef[] = [
  { key: 'budget',      label: 'Бюджет РК, ₽',      format: fmtRub,  aggType: 'sum', inverseGood: true },
  { key: 'impressions', label: 'Показы, шт',          format: fmtNum,  aggType: 'sum' },
  { key: 'clicks',      label: 'Клики, шт',           format: fmtNum,  aggType: 'sum' },
  { key: 'orders',      label: 'Рекл. заказы, шт',   format: fmtNum,  aggType: 'sum' },
  { key: 'ctr',         label: 'CTR, %',              format: fmtPct2, aggType: 'avg' },
  { key: 'cpc',         label: 'CPC, ₽',              format: fmtRub,  aggType: 'avg', inverseGood: true },
  { key: 'cpm',         label: 'CPM, ₽',              format: fmtRub,  aggType: 'avg', inverseGood: true },
  { key: 'cpo_ad',    label: 'СРО (рекл. заказы), ₽', format: fmtRub,  aggType: 'avg', inverseGood: true },
]

// Дополнительные метрики в итоговой строке (скрыты по умолчанию, без разбивки)
const AD_METRICS_EXTRA: AdMetricDef[] = [
  { key: 'cpo_all',  label: 'СРО (все заказы), ₽',    format: fmtRub, aggType: 'avg', inverseGood: true },
  { key: 'cr',       label: 'CR (рекл.), %',            format: fmtPct, aggType: 'avg' },
]

const EMPTY_AD: AdTypeMetrics = {
  budget: 0, impressions: 0, clicks: 0, orders: 0,
  ctr: 0, cr: 0, cpc: 0, cpm: 0, cpo_all: 0, cpo_ad: 0, cps: 0,
}

function getAdMetric(day: RnPDayMetrics, block: 'total' | 'search' | 'recommend'): AdTypeMetrics {
  return day[block === 'total' ? 'ad_total' : block === 'search' ? 'ad_search' : 'ad_recommend'] ?? EMPTY_AD
}

function aggMetric(vals: number[], aggType: 'sum' | 'avg'): number {
  if (aggType === 'sum') return vals.reduce((a, b) => a + b, 0)
  const nz = vals.filter(v => v > 0)
  return nz.length ? nz.reduce((a, b) => a + b, 0) / nz.length : 0
}

// ── Строка одной метрики с раскрытием Поиск / Полки ────────────────────────

interface AdMetricGroupProps {
  def: AdMetricDef
  sku: RnPPivotSKU
  days: string[]
  indent?: boolean  // вложенность для доп-метрик итоговой строки
}

function AdMetricGroup({ def, sku, days, indent = false }: AdMetricGroupProps) {
  const [open, setOpen] = useState(false)

  const totalVals     = days.map(d => getAdMetric(sku.days[d] ?? {} as RnPDayMetrics, 'total')[def.key] ?? 0)
  const searchVals    = days.map(d => getAdMetric(sku.days[d] ?? {} as RnPDayMetrics, 'search')[def.key] ?? 0)
  const recommendVals = days.map(d => getAdMetric(sku.days[d] ?? {} as RnPDayMetrics, 'recommend')[def.key] ?? 0)

  const totalAgg     = aggMetric(totalVals, def.aggType)
  const searchAgg    = aggMetric(searchVals, def.aggType)
  const recommendAgg = aggMetric(recommendVals, def.aggType)

  const SUB_ROWS = [
    { label: 'Поиск', vals: searchVals, agg: searchAgg },
    { label: 'Полки', vals: recommendVals, agg: recommendAgg },
  ]

  return (
    <>
      {/* ── Строка итоговой метрики ── */}
      <tr
        className="border-t border-gray-100 cursor-pointer hover:bg-orange-50/30 transition-colors"
        onClick={() => setOpen(v => !v)}
      >
        <td className="px-4 py-1.5 text-gray-700 font-medium sticky left-0 bg-white border-r border-gray-100 z-10">
          <div className={`flex items-center gap-1.5 ${indent ? 'pl-4' : ''}`}>
            <span className={`text-[10px] transition-transform inline-block ${open ? 'rotate-90' : ''}`}>▶</span>
            {def.label}
          </div>
        </td>
        <td className="px-3 py-1.5 text-right font-bold text-gray-800 tabular-nums">{def.format(totalAgg)}</td>
        <td className="px-2 py-1.5">
          <div className="flex justify-center"><MiniSparkline values={[...totalVals].reverse()} /></div>
        </td>
        {totalVals.map((val, idx) => {
          const prev = idx < totalVals.length - 1 ? totalVals[idx + 1] : null
          return (
            <td key={days[idx]} className="px-3 py-1.5 text-right tabular-nums text-gray-800">
              {def.format(val)}
              <TrendBadge current={val} prev={prev} inverseGood={def.inverseGood} />
            </td>
          )
        })}
      </tr>

      {/* ── Раскрытые строки: Поиск / Полки ── */}
      {open && SUB_ROWS.map(({ label, vals, agg }) => (
        <tr key={label} className="border-t border-gray-50 bg-orange-50/20">
          <td className="pl-9 pr-4 py-1 sticky left-0 bg-orange-50/20 border-r border-gray-100 z-10">
            <span className="text-[10px] font-semibold text-orange-500 uppercase tracking-wide">{label}</span>
          </td>
          <td className="px-3 py-1 text-right text-xs tabular-nums text-gray-700 font-medium">{def.format(agg)}</td>
          <td className="px-2 py-1">
            <div className="flex justify-center"><MiniSparkline values={[...vals].reverse()} /></div>
          </td>
          {vals.map((val, idx) => {
            const prev = idx < vals.length - 1 ? vals[idx + 1] : null
            return (
              <td key={days[idx]} className="px-3 py-1 text-right tabular-nums text-xs text-gray-700">
                {def.format(val)}
                <TrendBadge current={val} prev={prev} inverseGood={def.inverseGood} />
              </td>
            )
          })}
        </tr>
      ))}
    </>
  )
}


function AdSection({ sku, days }: { sku: RnPPivotSKU; days: string[] }) {
  const [open, setOpen] = useState(false)
  const [budgetOpen, setBudgetOpen] = useState(false)

  const hasAds = days.some(d => (sku.days[d]?.ad_spend ?? 0) > 0)

  // budget показан в строке-заголовке с разбивкой — не дублируем в детальных метриках
  const detailMetrics6 = AD_METRICS_6.filter(d => d.key !== 'budget')

  const budgetTotalVals  = days.map(d => sku.days[d]?.ad_total?.budget ?? 0)
  const budgetSearchVals = days.map(d => sku.days[d]?.ad_search?.budget ?? 0)
  const budgetRecomVals  = days.map(d => sku.days[d]?.ad_recommend?.budget ?? 0)
  const bMin = Math.min(...budgetTotalVals)
  const bMax = Math.max(...budgetTotalVals)

  const BUDGET_SUB = [
    { label: 'Поиск', vals: budgetSearchVals },
    { label: 'Полки', vals: budgetRecomVals },
  ]

  return (
    <tbody>
      {/* ── Строка-заголовок (всегда видна) ── */}
      <tr
        className="border-t border-gray-100 cursor-pointer hover:bg-orange-50/30 transition-colors"
        onClick={() => setOpen(v => !v)}
      >
        <td className="px-4 py-2 sticky left-0 bg-white z-10 border-r border-gray-100">
          <div className="flex items-center gap-2 font-semibold text-gray-900">
            <span className={`transition-transform ${open ? 'rotate-90' : ''} inline-block`}>▶</span>
            <span className="text-orange-600">📢</span>
            Бюджет РК, ₽
            {!hasAds && <span className="ml-2 text-[10px] text-gray-400 font-normal">нет данных</span>}
          </div>
        </td>
        <td />
        <td />
        {budgetTotalVals.map((val, idx) => {
          const prev = idx < budgetTotalVals.length - 1 ? budgetTotalVals[idx + 1] : null
          const bg = hasAds && val > 0 ? scaleColor(val, bMin, bMax, true) : ''
          const searchPct = val > 0 ? Math.round(budgetSearchVals[idx] / val * 100) : 0
          const recomPct  = val > 0 ? Math.round(budgetRecomVals[idx]  / val * 100) : 0
          return (
            <td key={days[idx]} className="px-3 py-2 text-right tabular-nums font-semibold text-orange-700"
              style={bg ? { backgroundColor: bg } : undefined}>
              {hasAds && val > 0 ? (
                <div className="flex flex-col items-end gap-0">
                  <span className="flex items-center gap-1">{fmtRub(val)}<TrendBadge current={val} prev={prev} inverseGood /></span>
                  <span className="text-[10px] font-normal text-gray-400 leading-tight whitespace-nowrap">
                    П {searchPct}% / Р {recomPct}%
                  </span>
                </div>
              ) : <span className="text-gray-300">—</span>}
            </td>
          )
        })}
      </tr>

      {/* ── Подстроки Бюджет: Поиск / Полки (видны когда section открыт или budgetOpen) ── */}
      {(open || budgetOpen) && hasAds && BUDGET_SUB.map(({ label, vals }) => (
        <tr key={label} className="border-t border-gray-50 bg-orange-50/20">
          <td className="pl-9 pr-4 py-1 sticky left-0 bg-orange-50/20 border-r border-gray-100 z-10"
            onClick={e => { e.stopPropagation(); setBudgetOpen(v => !v) }}>
            <span className="text-[10px] font-semibold text-orange-500 uppercase tracking-wide">{label}</span>
          </td>
          <td className="px-3 py-1 text-right text-xs tabular-nums text-gray-700 font-medium">
            {fmtRub(vals.reduce((a, b) => a + b, 0))}
          </td>
          <td className="px-2 py-1">
            <div className="flex justify-center"><MiniSparkline values={[...vals].reverse()} /></div>
          </td>
          {vals.map((val, idx) => {
            const prev = idx < vals.length - 1 ? vals[idx + 1] : null
            return (
              <td key={days[idx]} className="px-3 py-1 text-right tabular-nums text-xs text-gray-700">
                {val > 0 ? <>{fmtRub(val)}<TrendBadge current={val} prev={prev} inverseGood /></> : <span className="text-gray-300">—</span>}
              </td>
            )
          })}
        </tr>
      ))}

      {open && (
        <>
          {/* Метрики с разбивкой Поиск / Полки (без дубля budget) */}
          {detailMetrics6.map(def => (
            <AdMetricGroup key={def.key} def={def} sku={sku} days={days} />
          ))}

          {/* Плоские метрики: CPO, CR */}
          {AD_METRICS_EXTRA.map((def) => {
            const vals = days.map(d => getAdMetric(sku.days[d] ?? {} as RnPDayMetrics, 'total')[def.key] ?? 0)
            const agg  = aggMetric(vals, def.aggType)
            return (
              <tr key={def.key} className="border-t border-gray-100 hover:bg-orange-50/20 transition-colors">
                <td className="px-4 py-1.5 text-gray-700 font-medium sticky left-0 bg-white border-r border-gray-100 z-10">
                  {def.label}
                </td>
                <td className="px-3 py-1.5 text-right font-bold text-gray-800 tabular-nums">{def.format(agg)}</td>
                <td className="px-2 py-1.5">
                  <div className="flex justify-center"><MiniSparkline values={[...vals].reverse()} /></div>
                </td>
                {vals.map((val, idx) => {
                  const prev = idx < vals.length - 1 ? vals[idx + 1] : null
                  return (
                    <td key={days[idx]} className="px-3 py-1.5 text-right tabular-nums text-gray-800">
                      {def.format(val)}
                      <TrendBadge current={val} prev={prev} inverseGood={def.inverseGood} />
                    </td>
                  )
                })}
              </tr>
            )
          })}
        </>
      )}
    </tbody>
  )
}

// ── Переходы: воронка карточки ─────────────────────────────────────────────

interface FunnelMetricDef {
  key: keyof RnPDayMetrics
  label: string
  format: (v: number) => string
  aggType: 'sum' | 'avg'
  inverseGood?: boolean
}

const FUNNEL_METRICS: FunnelMetricDef[] = [
  { key: 'open_card_count',      label: 'Переходы, шт',                        format: fmtNum,  aggType: 'sum' },
  { key: 'ad_clicks_count',      label: 'Переходы рекламные, шт',              format: fmtNum,  aggType: 'sum' },
  { key: 'organic_clicks_count', label: 'Переходы орган., шт',                  format: fmtNum,  aggType: 'sum' },
  { key: 'organic_clicks_pct',   label: 'Доля орган. переходов, %',            format: fmtPct,  aggType: 'avg' },
  { key: 'add_to_cart_count',    label: 'Добавили в корзину, шт',              format: fmtNum,  aggType: 'sum' },
  { key: 'cart_from_card_pct',   label: 'Добавление в корзину, %',             format: fmtPct,  aggType: 'avg' },
  { key: 'order_from_cart_pct',  label: 'Добавление в заказ, %',               format: fmtPct,  aggType: 'avg' },
]

function FunnelSection({ sku, days }: { sku: RnPPivotSKU; days: string[] }) {
  const [open, setOpen] = useState(false)

  const hasData = days.some(d => (sku.days[d]?.open_card_count ?? 0) > 0)
  const rating = sku.wb_rating

  // open_card_count уже показан в строке-заголовке — не дублируем
  const detailMetrics = FUNNEL_METRICS.filter(m => m.key !== 'open_card_count')

  return (
    <tbody>
      {/* ── Строка-заголовок (всегда видна) ── */}
      <tr
        className="border-t border-gray-100 cursor-pointer hover:bg-teal-50/30 transition-colors"
        onClick={() => setOpen(v => !v)}
      >
        <td className="px-4 py-2 sticky left-0 bg-white z-10 border-r border-gray-100">
          <div className="flex items-center gap-2 font-semibold text-gray-500">
            <span className={`transition-transform ${open ? 'rotate-90' : ''} inline-block`}>▶</span>
            <span className="text-teal-600">👁</span>
            Переходы, шт
            {!hasData && <span className="ml-2 text-[10px] text-gray-400 font-normal">нет данных</span>}
          </div>
        </td>
        <td className="px-3 py-2 text-right">
          {rating !== null && rating !== undefined && (
            <span className="text-[10px] text-yellow-600 font-medium">★ {rating.toFixed(2)}</span>
          )}
        </td>
        <td />
        {(() => {
          const cardVals = days.map(d => sku.days[d]?.open_card_count ?? 0)
          const cMin = Math.min(...cardVals)
          const cMax = Math.max(...cardVals)
          return days.map((d, idx) => {
            const val = cardVals[idx]
            const prev = idx < cardVals.length - 1 ? cardVals[idx + 1] : null
            const bg = hasData && val > 0 ? scaleColor(val, cMin, cMax) : ''
            return (
              <td key={d} className="px-3 py-2 text-right tabular-nums font-semibold text-gray-900"
                style={bg ? { backgroundColor: bg } : undefined}>
                {hasData && val > 0 ? <>{fmtNum(val)}<TrendBadge current={val} prev={prev} /></> : <span className="text-gray-300">—</span>}
              </td>
            )
          })
        })()}
      </tr>

      {open && (
        <>
          {/* Строки деталей (без дубля open_card_count) */}
          {detailMetrics.map((m, ri) => {
            const vals = days.map(d => (sku.days[d]?.[m.key] ?? 0) as number)
            const sparkVals = [...vals].reverse()
            const agg = m.aggType === 'sum'
              ? vals.reduce((a, b) => a + b, 0)
              : (() => { const nz = vals.filter(v => v > 0); return nz.length ? nz.reduce((a, b) => a + b, 0) / nz.length : 0 })()
            return (
              <tr key={m.key} className={`border-t border-gray-50 hover:bg-teal-50/20 transition-colors ${ri % 2 === 0 ? '' : 'bg-gray-50/30'}`}>
                <td className="px-4 py-1.5 text-gray-700 font-medium sticky left-0 bg-white border-r border-gray-100 z-10">
                  {m.label}
                </td>
                <td className="px-3 py-1.5 text-right font-bold text-gray-800 tabular-nums">{m.format(agg)}</td>
                <td className="px-2 py-1.5">
                  <div className="flex justify-center"><MiniSparkline values={sparkVals} /></div>
                </td>
                {vals.map((val, idx) => {
                  const prev = idx < vals.length - 1 ? vals[idx + 1] : null
                  return (
                    <td key={days[idx]} className="px-3 py-1.5 text-right tabular-nums text-gray-800">
                      {m.format(val)}
                      <TrendBadge current={val} prev={prev} inverseGood={m.inverseGood} />
                    </td>
                  )
                })}
              </tr>
            )
          })}

          {/* Рейтинг по отзывам */}
          {rating !== null && rating !== undefined && (
            <tr className="border-t border-gray-50 bg-yellow-50/20">
              <td className="px-4 py-1.5 text-gray-700 font-medium sticky left-0 bg-yellow-50/20 border-r border-gray-100 z-10">
                Рейтинг по отзывам
              </td>
              <td className="px-3 py-1.5 text-right font-bold text-yellow-700 tabular-nums">
                ★ {rating.toFixed(2)}
              </td>
              <td className="px-2 py-1.5" />
              {days.map(d => (
                <td key={d} className="px-3 py-1.5 text-right tabular-nums text-yellow-600">
                  {rating.toFixed(2)}
                </td>
              ))}
            </tr>
          )}
        </>
      )}
    </tbody>
  )
}

// ── Карточка SKU ──────────────────────────────────────────────────────────

function nextBasketUrl(url: string): string | null {
  // basket-{N}.wbbasket.ru → пробуем basket-(N+1)
  const m = url.match(/basket-(\d+)\.wbbasket\.ru/)
  if (!m) return null
  const next = String(parseInt(m[1]) + 1).padStart(2, '0')
  return url.replace(/basket-\d+\.wbbasket\.ru/, `basket-${next}.wbbasket.ru`)
}

function SkuPhoto({ url, article }: { url: string; article: string }) {
  const [src, setSrc] = useState(url)
  const [failed, setFailed] = useState(false)

  useEffect(() => { setSrc(url); setFailed(false) }, [url])

  if (!src || failed) {
    return (
      <div className="w-12 h-12 rounded-lg bg-gray-100 flex items-center justify-center text-gray-400 text-[9px] font-mono text-center shrink-0 overflow-hidden">
        {article.slice(0, 4)}
      </div>
    )
  }
  return (
    <img
      src={src}
      alt={article}
      onError={() => {
        const fallback = nextBasketUrl(src)
        if (fallback && fallback !== src) {
          setSrc(fallback)
        } else {
          setFailed(true)
        }
      }}
      className="w-12 h-12 rounded-lg object-cover shrink-0 border border-gray-100"
    />
  )
}

// Fixed column widths for table-layout:fixed (both sticky header and body must match)
const COL_W = { label: 200, total: 76, bar: 40, day: 90 }

function SkuPivotCard({ sku, days, onUpdate, externalCollapsed }: { sku: RnPPivotSKU; days: string[]; onUpdate: () => void; externalCollapsed?: boolean }) {
  const [collapsed, setCollapsed] = useState(false)

  useEffect(() => {
    if (externalCollapsed !== undefined) setCollapsed(externalCollapsed)
  }, [externalCollapsed])
  const [costsOpen, setCostsOpen] = useState(false)
  const [saving, setSaving] = useState(false)

  // Sticky header scroll-sync refs
  const headerRef = useRef<HTMLDivElement>(null)
  const bodyRef = useRef<HTMLDivElement>(null)
  const handleBodyScroll = useCallback(() => {
    if (headerRef.current && bodyRef.current) {
      headerRef.current.scrollLeft = bodyRef.current.scrollLeft
    }
  }, [])

  const colTotalWidth = COL_W.label + COL_W.total + COL_W.bar + COL_W.day * days.length

  const stockDanger = sku.turnover_days < 7
  const stockWarn = sku.turnover_days < 15

  const saveOverride = useCallback(async (payload: { buyout_rate_pct?: number | null; logistics_rub?: number | null; commission_pct?: number | null }) => {
    setSaving(true)
    try {
      await rnpApi.updateSkuOverrides({ sku_id: sku.sku_id, channel_id: sku.channel_id, ...payload })
      onUpdate()
    } catch (e) {
      console.error('Failed to save override', e)
    } finally {
      setSaving(false)
    }
  }, [sku.sku_id, sku.channel_id, onUpdate])

  const onSaveBuyout = (val: number | null) => saveOverride({ buyout_rate_pct: val })
  const onSaveLogistics = (val: number | null) => saveOverride({ logistics_rub: val })
  const onSaveCommission = (val: number | null) => saveOverride({ commission_pct: val })

  return (
    <div className={`bg-white rounded-xl border shadow-sm overflow-clip transition-opacity ${saving ? 'opacity-70' : ''} ${sku.buyout_rate_is_manual || sku.logistics_is_manual || sku.commission_is_manual ? 'border-indigo-200' : 'border-gray-200'}`}>
      {/* ── SKU Header ── */}
      <div className="flex items-center gap-3 px-4 py-3 border-b border-gray-200 bg-gradient-to-r from-gray-50 to-white">
        {/* Фото товара */}
        <SkuPhoto url={sku.photo_url} article={sku.seller_article} />

        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className={`font-bold text-sm font-mono px-2 py-0.5 rounded ${sku.channel_type === 'wb' ? 'text-purple-800 bg-purple-50' : 'text-blue-800 bg-blue-50'}`}>
              {sku.seller_article}
            </span>
            {sku.wb_article && (
              <a
                href={
                  sku.channel_type === 'ozon'
                    ? `https://www.ozon.ru/product/${sku.wb_article}/`
                    : `https://www.wildberries.ru/catalog/${sku.wb_article}/detail.aspx`
                }
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs text-gray-400 font-mono hover:text-indigo-500 hover:underline transition-colors"
                onClick={e => e.stopPropagation()}
              >
                {sku.wb_article}
              </a>
            )}
          </div>
        </div>

        {/* Мини-KPI */}
        <div className="hidden lg:flex items-center gap-5 text-sm">
          <div className="text-right">
            <div className="text-[10px] text-gray-400 uppercase tracking-wide">Заказы</div>
            <div className="font-bold text-gray-800">{fmtNum(sku.total_orders_qty)} шт</div>
            <div className="text-[10px] text-gray-500">{fmtRub(sku.total_orders_rub)} ₽</div>
          </div>
          <div className="text-right">
            <div className="text-[10px] text-gray-400 uppercase tracking-wide">Маржа</div>
            <div className={`font-bold ${sku.avg_margin_pct >= 20 ? 'text-green-600' : sku.avg_margin_pct >= 10 ? 'text-yellow-600' : 'text-red-500'}`}>
              {sku.avg_margin_pct.toFixed(1)}%
            </div>
          </div>
          {(() => {
            const totalProfit = Object.values(sku.days).reduce((s, d) => s + (d.profit_total ?? 0), 0)
            return (
              <div className="text-right">
                <div className="text-[10px] text-gray-400 uppercase tracking-wide">Прогноз. прибыль</div>
                <div className={`font-bold ${totalProfit >= 0 ? 'text-green-600' : 'text-red-500'}`}>
                  {fmtRub(totalProfit)} ₽
                </div>
              </div>
            )
          })()}
          {(() => {
            const totalAdSpend = Object.values(sku.days).reduce((s, d) => s + (d.ad_spend ?? 0), 0)
            return (
              <div className="text-right">
                <div className="text-[10px] text-gray-400 uppercase tracking-wide">Бюджет РК</div>
                <div className="font-bold text-gray-800">{fmtRub(totalAdSpend)} ₽</div>
              </div>
            )
          })()}
          <div className="text-right">
            <div className="text-[10px] text-gray-400 uppercase tracking-wide">Остаток</div>
            <div className={`font-bold ${stockDanger ? 'text-red-500' : stockWarn ? 'text-yellow-600' : 'text-gray-800'}`}>
              {fmtNum(sku.current_stock)} шт
            </div>
            <div className={`text-[10px] ${stockDanger ? 'text-red-400' : stockWarn ? 'text-yellow-500' : 'text-gray-400'}`}>
              {sku.turnover_days === 999 ? '∞' : sku.turnover_days.toFixed(0)} дн.
            </div>
          </div>
          {/* Выкуп — inline edit прямо в шапке */}
          <div className="text-right">
            <div className="text-[10px] text-gray-400 uppercase tracking-wide">
              Выкуп
              {sku.buyout_rate_is_manual && <span className="ml-1 text-indigo-400">✎</span>}
            </div>
            <EditableCell
              value={sku.buyout_rate_pct}
              isManual={sku.buyout_rate_is_manual}
              suffix="%"
              formatDisplay={(v) => v.toFixed(1) + '%'}
              onSave={onSaveBuyout}
              valueClassName={sku.buyout_rate_pct >= 45 ? 'text-green-600 font-bold' : sku.buyout_rate_pct >= 30 ? 'text-yellow-600 font-bold' : 'text-red-500 font-bold'}
            />
          </div>
          {/* Логистика — inline edit в шапке */}
          <div className="text-right">
            <div className="text-[10px] text-gray-400 uppercase tracking-wide">
              Логистика
              {sku.logistics_is_manual && <span className="ml-1 text-indigo-400">✎</span>}
            </div>
            <EditableCell
              value={sku.logistics_per_unit_avg}
              isManual={sku.logistics_is_manual}
              suffix=" ₽"
              formatDisplay={(v) => fmtNum(v, 1) + ' ₽'}
              onSave={onSaveLogistics}
            />
          </div>
          {/* Комиссия + эквайринг — inline edit в шапке */}
          <div className="text-right">
            <div className="text-[10px] text-gray-400 uppercase tracking-wide">
              Комиссия
              {sku.commission_is_manual && <span className="ml-1 text-indigo-400">✎</span>}
            </div>
            <EditableCell
              value={sku.commission_pct_avg}
              isManual={sku.commission_is_manual}
              suffix="%"
              formatDisplay={(v) => v.toFixed(2) + '%'}
              onSave={onSaveCommission}
            />
          </div>
        </div>

        <button
          onClick={() => setCollapsed((v) => !v)}
          className="ml-2 w-7 h-7 flex items-center justify-center text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-lg transition-colors text-xs"
        >
          {collapsed ? '▶' : '▼'}
        </button>
      </div>

      {!collapsed && (
        <div>
          {/* ── Липкая шапка дат — вне overflow-x контейнера, sticky к viewport ── */}
          <div
            ref={headerRef}
            className="sticky top-0 z-20 overflow-hidden border-b border-gray-200"
          >
            <table
              className="text-xs border-collapse"
              style={{ tableLayout: 'fixed', width: colTotalWidth }}
            >
              <colgroup>
                <col style={{ width: COL_W.label }} />
                <col style={{ width: COL_W.total }} />
                <col style={{ width: COL_W.bar }} />
                {days.map(d => <col key={d} style={{ width: COL_W.day }} />)}
              </colgroup>
              <thead>
                <tr className="bg-gray-100 shadow-[0_1px_0_0_#e5e7eb,0_2px_4px_0_rgba(0,0,0,0.06)]">
                  <th className="sticky left-0 z-30 text-left px-4 py-2 font-medium text-gray-500 bg-gray-100 border-r border-gray-200">
                    Показатель
                  </th>
                  <th className="text-right px-3 py-2 font-semibold text-gray-600 bg-gray-100">Итого</th>
                  <th className="text-center px-2 py-2 font-medium text-gray-400 bg-gray-100">▓▓</th>
                  {days.map(d => (
                    <th key={d} className="text-right px-3 py-2 font-medium text-gray-600 whitespace-nowrap bg-gray-100">
                      {formatDayLabel(d)}
                    </th>
                  ))}
                </tr>
              </thead>
            </table>
          </div>

          {/* ── Прокручиваемое тело таблицы ── */}
          <div ref={bodyRef} className="overflow-x-auto" onScroll={handleBodyScroll}>
            <table
              className="text-xs border-collapse"
              style={{ tableLayout: 'fixed', width: colTotalWidth }}
            >
              <colgroup>
                <col style={{ width: COL_W.label }} />
                <col style={{ width: COL_W.total }} />
                <col style={{ width: COL_W.bar }} />
                {days.map(d => <col key={d} style={{ width: COL_W.day }} />)}
              </colgroup>

              {/* ── Юнит-экономика ── */}
              <tbody>
                <tr className="bg-blue-50/70 border-b border-blue-100">
                  <td colSpan={3 + days.length} className="px-4 py-1.5">
                    <div className="flex items-center gap-1.5">
                      <span className="text-blue-500 text-xs">⊞</span>
                      <span className="text-xs font-semibold text-blue-700">Юнит-экономика</span>
                    </div>
                  </td>
                </tr>
                <MetricsTable metrics={MAIN_METRICS} sku={sku} days={days} onSaveBuyout={onSaveBuyout} onSaveLogistics={onSaveLogistics} onSaveCommission={onSaveCommission} />
              </tbody>

              {/* ── Переходы ── */}
              <FunnelSection sku={sku} days={days} />

              {/* ── Реклама РК ── */}
              <AdSection sku={sku} days={days} />

              {/* ── Структура затрат ── */}
              <tbody>
                <tr
                  className="border-t border-gray-100 cursor-pointer hover:bg-gray-50 transition-colors"
                  onClick={() => setCostsOpen((v) => !v)}
                >
                  <td colSpan={3 + days.length} className="px-4 py-2">
                    <div className="flex items-center gap-2 font-semibold text-gray-500">
                      <span className={`transition-transform ${costsOpen ? 'rotate-90' : ''} inline-block`}>▶</span>
                      Структура затрат
                    </div>
                  </td>
                </tr>
                {costsOpen && (
                  <MetricsTable metrics={COSTS_METRICS} sku={sku} days={days} onSaveBuyout={onSaveBuyout} onSaveLogistics={onSaveLogistics} onSaveCommission={onSaveCommission} />
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}

// ── Агрегация SKU по маркетплейсу ───────────────────────────────────────────

function emptyAd(): AdTypeMetrics {
  return {
    budget: 0, impressions: 0, clicks: 0, orders: 0,
    ctr: 0, cr: 0, cpc: 0, cpm: 0, cpo_all: 0, cpo_ad: 0, cps: 0,
  }
}

function emptyDay(): RnPDayMetrics {
  return {
    orders_qty: 0, orders_rub: 0, sales_qty: 0, sales_rub: 0,
    returns_qty: 0, cancellations_qty: 0,
    forecast_sales_qty: 0, forecast_sales_rub: 0,
    price_before_spp: 0, price_after_spp: 0, spp_pct: 0,
    stock_wb: 0, in_way_to_client: 0, in_way_from_client: 0, frozen_capital: 0,
    buyout_rate_pct: 0, margin_pct: 0, roi_pct: 0,
    profit_per_unit: 0, profit_total: 0,
    commission_per_unit: 0, commission_pct: 0,
    logistics_per_unit: 0, storage_per_unit: 0, cogs_per_unit: 0,
    tax_usn_per_unit: 0, tax_nds_per_unit: 0, tax_total_per_unit: 0,
    return_fee_rub: 0,
    drr_orders_pct: 0, drr_sales_pct: 0,
    ad_spend: 0, ad_orders_qty: 0,
    ad_total: emptyAd(), ad_search: emptyAd(), ad_recommend: emptyAd(),
    open_card_count: 0, ad_clicks_count: 0, organic_clicks_count: 0, organic_clicks_pct: 0,
    add_to_cart_count: 0, cart_from_card_pct: 0, order_from_cart_pct: 0,
  }
}

function aggregateAd(group: AdTypeMetrics[]): AdTypeMetrics {
  const budget = group.reduce((s, a) => s + a.budget, 0)
  const impressions = group.reduce((s, a) => s + a.impressions, 0)
  const clicks = group.reduce((s, a) => s + a.clicks, 0)
  const orders = group.reduce((s, a) => s + a.orders, 0)
  return {
    budget, impressions, clicks, orders,
    ctr: impressions > 0 ? (clicks / impressions) * 100 : 0,
    cr: clicks > 0 ? (orders / clicks) * 100 : 0,
    cpc: clicks > 0 ? budget / clicks : 0,
    cpm: impressions > 0 ? (budget / impressions) * 1000 : 0,
    cpo_all: orders > 0 ? budget / orders : 0,
    cpo_ad: orders > 0 ? budget / orders : 0,
    cps: orders > 0 ? budget / orders : 0,
  }
}

function aggregateChannelDay(skus: RnPPivotSKU[], ds: string): RnPDayMetrics {
  const day = emptyDay()
  const parts = skus.map(s => s.days[ds]).filter(Boolean) as RnPDayMetrics[]
  if (parts.length === 0) return day

  // Суммы
  day.orders_qty = parts.reduce((s, d) => s + d.orders_qty, 0)
  day.orders_rub = parts.reduce((s, d) => s + d.orders_rub, 0)
  day.sales_qty = parts.reduce((s, d) => s + d.sales_qty, 0)
  day.sales_rub = parts.reduce((s, d) => s + d.sales_rub, 0)
  day.returns_qty = parts.reduce((s, d) => s + d.returns_qty, 0)
  day.cancellations_qty = parts.reduce((s, d) => s + d.cancellations_qty, 0)
  day.forecast_sales_qty = parts.reduce((s, d) => s + d.forecast_sales_qty, 0)
  day.forecast_sales_rub = parts.reduce((s, d) => s + d.forecast_sales_rub, 0)
  day.stock_wb = parts.reduce((s, d) => s + d.stock_wb, 0)
  day.in_way_to_client = parts.reduce((s, d) => s + d.in_way_to_client, 0)
  day.in_way_from_client = parts.reduce((s, d) => s + d.in_way_from_client, 0)
  day.frozen_capital = parts.reduce((s, d) => s + d.frozen_capital, 0)
  day.profit_total = parts.reduce((s, d) => s + d.profit_total, 0)
  day.ad_spend = parts.reduce((s, d) => s + d.ad_spend, 0)
  day.ad_orders_qty = parts.reduce((s, d) => s + d.ad_orders_qty, 0)
  day.return_fee_rub = parts.reduce((s, d) => s + d.return_fee_rub, 0)
  day.open_card_count = parts.reduce((s, d) => s + d.open_card_count, 0)
  day.ad_clicks_count = parts.reduce((s, d) => s + d.ad_clicks_count, 0)
  day.organic_clicks_count = parts.reduce((s, d) => s + d.organic_clicks_count, 0)
  day.add_to_cart_count = parts.reduce((s, d) => s + d.add_to_cart_count, 0)
  day.storage_per_unit = parts.reduce((s, d) => s + d.storage_per_unit, 0)

  // Реклама по типам
  day.ad_total = aggregateAd(parts.map(p => p.ad_total))
  day.ad_search = aggregateAd(parts.map(p => p.ad_search))
  day.ad_recommend = aggregateAd(parts.map(p => p.ad_recommend))

  // Взвешенные средние (по orders_qty или orders_rub) для цен/процентов
  const totalOrdersQty = day.orders_qty
  const totalOrdersRub = day.orders_rub
  const weightedByOrders = (key: keyof RnPDayMetrics) => {
    if (totalOrdersQty === 0) {
      const nz = parts.filter(p => (p[key] as number) > 0)
      return nz.length ? nz.reduce((s, p) => s + (p[key] as number), 0) / nz.length : 0
    }
    return parts.reduce((s, p) => s + (p[key] as number) * p.orders_qty, 0) / totalOrdersQty
  }
  day.price_before_spp = weightedByOrders('price_before_spp')
  day.price_after_spp = weightedByOrders('price_after_spp')
  day.spp_pct = weightedByOrders('spp_pct')
  day.buyout_rate_pct = weightedByOrders('buyout_rate_pct')
  day.commission_per_unit = weightedByOrders('commission_per_unit')
  day.logistics_per_unit = weightedByOrders('logistics_per_unit')
  day.cogs_per_unit = weightedByOrders('cogs_per_unit')
  day.tax_usn_per_unit = weightedByOrders('tax_usn_per_unit')
  day.tax_nds_per_unit = weightedByOrders('tax_nds_per_unit')
  day.tax_total_per_unit = weightedByOrders('tax_total_per_unit')
  day.profit_per_unit = weightedByOrders('profit_per_unit')

  // Пересчитываемые показатели
  day.commission_pct = totalOrdersRub > 0
    ? (parts.reduce((s, p) => s + p.commission_pct * p.orders_rub, 0) / totalOrdersRub)
    : 0
  day.margin_pct = day.forecast_sales_rub > 0
    ? (day.profit_total / day.forecast_sales_rub) * 100
    : 0
  day.roi_pct = (day.cogs_per_unit > 0 && day.profit_per_unit !== 0)
    ? (day.profit_per_unit / day.cogs_per_unit) * 100
    : 0
  day.drr_orders_pct = totalOrdersRub > 0 ? (day.ad_spend / totalOrdersRub) * 100 : 0
  day.drr_sales_pct = day.forecast_sales_rub > 0 ? (day.ad_spend / day.forecast_sales_rub) * 100 : 0

  // Воронка
  day.organic_clicks_pct = day.open_card_count > 0
    ? (day.organic_clicks_count / day.open_card_count) * 100 : 0
  day.cart_from_card_pct = day.open_card_count > 0
    ? (day.add_to_cart_count / day.open_card_count) * 100 : 0
  day.order_from_cart_pct = day.add_to_cart_count > 0
    ? (day.orders_qty / day.add_to_cart_count) * 100 : 0

  return day
}

const CHANNEL_META: Record<string, { label: string; color: string }> = {
  wb: { label: 'Wildberries', color: 'from-purple-50 to-white' },
  ozon: { label: 'Ozon', color: 'from-blue-50 to-white' },
  lamoda: { label: 'Lamoda', color: 'from-pink-50 to-white' },
}

function buildChannelAggregate(skus: RnPPivotSKU[], channelType: string, days: string[]): RnPPivotSKU {
  const daysMap: Record<string, RnPDayMetrics> = {}
  for (const ds of days) daysMap[ds] = aggregateChannelDay(skus, ds)

  const totalOrdersQty = skus.reduce((s, r) => s + r.total_orders_qty, 0)
  const totalOrdersRub = skus.reduce((s, r) => s + r.total_orders_rub, 0)
  const totalSalesQty = skus.reduce((s, r) => s + r.total_sales_qty, 0)
  const totalReturnsQty = skus.reduce((s, r) => s + r.total_returns_qty, 0)
  const totalStock = skus.reduce((s, r) => s + r.current_stock, 0)

  // Взвешенные средние по orders_rub
  const w = (key: keyof RnPPivotSKU) =>
    totalOrdersRub > 0
      ? skus.reduce((s, r) => s + (r[key] as number) * r.total_orders_rub, 0) / totalOrdersRub
      : (skus.length ? skus.reduce((s, r) => s + (r[key] as number), 0) / skus.length : 0)

  const meta = CHANNEL_META[channelType] ?? { label: channelType.toUpperCase(), color: 'from-gray-50 to-white' }

  return {
    sku_id: -1,
    channel_id: -1,
    seller_article: `ИТОГО ${meta.label}`,
    name: `Сводная по ${meta.label}`,
    channel_type: channelType,
    channel_name: meta.label,
    wb_article: '',
    photo_url: '',
    total_orders_qty: totalOrdersQty,
    total_orders_rub: totalOrdersRub,
    total_sales_qty: totalSalesQty,
    total_returns_qty: totalReturnsQty,
    avg_price_before_spp: w('avg_price_before_spp'),
    avg_price_after_spp: w('avg_price_after_spp'),
    avg_spp_pct: w('avg_spp_pct'),
    current_stock: totalStock,
    turnover_days: (() => {
      const avgDaily = days.length > 0 ? totalOrdersQty / days.length : 0
      return avgDaily > 0 ? Math.round((totalStock / avgDaily) * 10) / 10 : 999
    })(),
    buyout_rate_pct: w('buyout_rate_pct'),
    buyout_rate_is_manual: false,
    logistics_per_unit_avg: w('logistics_per_unit_avg'),
    logistics_is_manual: false,
    commission_pct_avg: w('commission_pct_avg'),
    commission_is_manual: false,
    avg_margin_pct: w('avg_margin_pct'),
    cogs_per_unit: w('cogs_per_unit'),
    wb_rating: null,
    days: daysMap,
  }
}

// ── Сводная карточка по каналу ──────────────────────────────────────────────

function ChannelAggregateCard({ agg, days, externalCollapsed }: {
  agg: RnPPivotSKU
  days: string[]
  externalCollapsed?: boolean
}) {
  const [collapsed, setCollapsed] = useState(true)
  const [costsOpen, setCostsOpen] = useState(false)
  useEffect(() => {
    if (externalCollapsed !== undefined) setCollapsed(externalCollapsed)
  }, [externalCollapsed])

  const headerRef = useRef<HTMLDivElement>(null)
  const bodyRef = useRef<HTMLDivElement>(null)
  const handleBodyScroll = useCallback(() => {
    if (headerRef.current && bodyRef.current) {
      headerRef.current.scrollLeft = bodyRef.current.scrollLeft
    }
  }, [])

  const colTotalWidth = COL_W.label + COL_W.total + COL_W.bar + COL_W.day * days.length
  const noop = () => {}
  const meta = CHANNEL_META[agg.channel_type] ?? { label: agg.channel_name, color: 'from-gray-50 to-white' }

  const totalProfit = Object.values(agg.days).reduce((s, d) => s + (d.profit_total ?? 0), 0)
  const totalAdSpend = Object.values(agg.days).reduce((s, d) => s + (d.ad_spend ?? 0), 0)

  return (
    <div className="bg-white rounded-xl border-2 border-indigo-200 shadow-sm overflow-clip">
      <div className={`flex items-center gap-3 px-4 py-3 border-b border-gray-200 bg-gradient-to-r ${meta.color}`}>
        <div className="w-12 h-12 rounded-lg bg-white border border-gray-200 flex items-center justify-center shrink-0">
          <ChannelIcon type={agg.channel_type as 'wb' | 'ozon'} size={28} />
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="font-bold text-sm px-2 py-0.5 rounded bg-indigo-100 text-indigo-800">
              ИТОГО · {meta.label}
            </span>
          </div>
        </div>
        <div className="hidden lg:flex items-center gap-5 text-sm">
          <div className="text-right">
            <div className="text-[10px] text-gray-400 uppercase tracking-wide">Заказы</div>
            <div className="font-bold text-gray-800">{fmtNum(agg.total_orders_qty)} шт</div>
            <div className="text-[10px] text-gray-500">{fmtRub(agg.total_orders_rub)} ₽</div>
          </div>
          <div className="text-right">
            <div className="text-[10px] text-gray-400 uppercase tracking-wide">Маржа</div>
            <div className={`font-bold ${agg.avg_margin_pct >= 20 ? 'text-green-600' : agg.avg_margin_pct >= 10 ? 'text-yellow-600' : 'text-red-500'}`}>
              {agg.avg_margin_pct.toFixed(1)}%
            </div>
          </div>
          <div className="text-right">
            <div className="text-[10px] text-gray-400 uppercase tracking-wide">Прогноз. прибыль</div>
            <div className={`font-bold ${totalProfit >= 0 ? 'text-green-600' : 'text-red-500'}`}>
              {fmtRub(totalProfit)} ₽
            </div>
          </div>
          <div className="text-right">
            <div className="text-[10px] text-gray-400 uppercase tracking-wide">Бюджет РК</div>
            <div className="font-bold text-gray-800">{fmtRub(totalAdSpend)} ₽</div>
          </div>
          <div className="text-right">
            <div className="text-[10px] text-gray-400 uppercase tracking-wide">Остаток</div>
            <div className="font-bold text-gray-800">{fmtNum(agg.current_stock)} шт</div>
            <div className="text-[10px] text-gray-400">
              {agg.turnover_days === 999 ? '∞' : agg.turnover_days.toFixed(0)} дн.
            </div>
          </div>
          <div className="text-right">
            <div className="text-[10px] text-gray-400 uppercase tracking-wide">Выкуп</div>
            <div className={`font-bold ${agg.buyout_rate_pct >= 45 ? 'text-green-600' : agg.buyout_rate_pct >= 30 ? 'text-yellow-600' : 'text-red-500'}`}>
              {agg.buyout_rate_pct.toFixed(1)}%
            </div>
          </div>
          <div className="text-right">
            <div className="text-[10px] text-gray-400 uppercase tracking-wide">Логистика</div>
            <div className="font-bold text-gray-800">{fmtNum(agg.logistics_per_unit_avg, 1)} ₽</div>
          </div>
          <div className="text-right">
            <div className="text-[10px] text-gray-400 uppercase tracking-wide">Комиссия</div>
            <div className="font-bold text-gray-800">{agg.commission_pct_avg.toFixed(2)}%</div>
          </div>
        </div>
        <button
          onClick={() => setCollapsed(v => !v)}
          className="ml-2 w-7 h-7 flex items-center justify-center text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-lg transition-colors text-xs"
        >
          {collapsed ? '▶' : '▼'}
        </button>
      </div>

      {!collapsed && (
        <div>
          <div ref={headerRef} className="sticky top-0 z-20 overflow-hidden border-b border-gray-200">
            <table className="text-xs border-collapse" style={{ tableLayout: 'fixed', width: colTotalWidth }}>
              <colgroup>
                <col style={{ width: COL_W.label }} />
                <col style={{ width: COL_W.total }} />
                <col style={{ width: COL_W.bar }} />
                {days.map(d => <col key={d} style={{ width: COL_W.day }} />)}
              </colgroup>
              <thead>
                <tr className="bg-gray-100 shadow-[0_1px_0_0_#e5e7eb,0_2px_4px_0_rgba(0,0,0,0.06)]">
                  <th className="sticky left-0 z-30 text-left px-4 py-2 font-medium text-gray-500 bg-gray-100 border-r border-gray-200">Показатель</th>
                  <th className="text-right px-3 py-2 font-semibold text-gray-600 bg-gray-100">Итого</th>
                  <th className="text-center px-2 py-2 font-medium text-gray-400 bg-gray-100">▓▓</th>
                  {days.map(d => (
                    <th key={d} className="text-right px-3 py-2 font-medium text-gray-600 whitespace-nowrap bg-gray-100">
                      {formatDayLabel(d)}
                    </th>
                  ))}
                </tr>
              </thead>
            </table>
          </div>

          <div ref={bodyRef} className="overflow-x-auto" onScroll={handleBodyScroll}>
            <table className="text-xs border-collapse" style={{ tableLayout: 'fixed', width: colTotalWidth }}>
              <colgroup>
                <col style={{ width: COL_W.label }} />
                <col style={{ width: COL_W.total }} />
                <col style={{ width: COL_W.bar }} />
                {days.map(d => <col key={d} style={{ width: COL_W.day }} />)}
              </colgroup>
              <tbody>
                <tr className="bg-indigo-50/70 border-b border-indigo-100">
                  <td colSpan={3 + days.length} className="px-4 py-1.5">
                    <div className="flex items-center gap-1.5">
                      <span className="text-indigo-500 text-xs">⊞</span>
                      <span className="text-xs font-semibold text-indigo-700">Юнит-экономика (сводная)</span>
                    </div>
                  </td>
                </tr>
                <MetricsTable metrics={MAIN_METRICS} sku={agg} days={days} onSaveBuyout={noop} onSaveLogistics={noop} onSaveCommission={noop} />
              </tbody>

              {/* ── Переходы (сводная) ── */}
              <FunnelSection sku={agg} days={days} />

              {/* ── Реклама (сводная) ── */}
              <AdSection sku={agg} days={days} />

              <tbody>
                <tr className="border-t border-gray-100 cursor-pointer hover:bg-gray-50 transition-colors" onClick={() => setCostsOpen(v => !v)}>
                  <td colSpan={3 + days.length} className="px-4 py-2">
                    <div className="flex items-center gap-2 font-semibold text-gray-500">
                      <span className={`transition-transform ${costsOpen ? 'rotate-90' : ''} inline-block`}>▶</span>
                      Структура затрат (сводная)
                    </div>
                  </td>
                </tr>
                {costsOpen && (
                  <MetricsTable metrics={COSTS_METRICS} sku={agg} days={days} onSaveBuyout={noop} onSaveLogistics={noop} onSaveCommission={noop} />
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}

// ── Главная страница ────────────────────────────────────────────────────────

function defaultRange(): DateRange {
  const yesterday = subDays(startOfDay(new Date()), 1)
  return { from: subDays(yesterday, 6), to: yesterday }
}

export default function RnPPage() {
  const [dateRange, setDateRange] = useState<DateRange>(defaultRange)
  const [selectedChannels, setSelectedChannels] = useState<string[]>([])
  const [selectedArticles, setSelectedArticles] = useState<string[]>([])
  const [sortBy, setSortBy] = useState<'orders' | 'margin' | 'stock'>('orders')
  const [allCollapsed, setAllCollapsed] = useState(false)
  const queryClient = useQueryClient()

  const dateFrom = format(dateRange.from, 'yyyy-MM-dd')
  const dateTo = format(dateRange.to, 'yyyy-MM-dd')

  const { data, isLoading, error } = useQuery({
    queryKey: ['rnp-pivot', dateFrom, dateTo, selectedChannels],
    queryFn: () =>
      rnpApi
        .pivot({
          date_from: dateFrom,
          date_to: dateTo,
          ...(selectedChannels.length > 0 ? { channels: selectedChannels } : {}),
        })
        .then((r) => r.data),
    refetchInterval: 3_600_000,
    staleTime: 5 * 60_000,
  })

  const handleUpdate = useCallback(() => {
    queryClient.invalidateQueries({ queryKey: ['rnp-pivot', dateFrom, dateTo, selectedChannels] })
  }, [queryClient, dateFrom, dateTo, selectedChannels])

  const articleOptions = useMemo(() => {
    if (!data?.skus) return []
    return data.skus.map((s) => ({
      value: `${s.sku_id}-${s.channel_type}`,
      label: s.seller_article,
      icon: <ChannelIcon type={s.channel_type} size={18} />,
    }))
  }, [data])

  const filtered = (data?.skus ?? [])
    .filter((s) => {
      if (selectedArticles.length > 0 && !selectedArticles.includes(`${s.sku_id}-${s.channel_type}`)) return false
      return true
    })
    .sort((a, b) => {
      if (sortBy === 'margin') return b.avg_margin_pct - a.avg_margin_pct
      if (sortBy === 'stock') return a.turnover_days - b.turnover_days
      return b.total_orders_qty - a.total_orders_qty
    })

  const totalOrders = filtered.reduce((s, r) => s + r.total_orders_qty, 0)
  const totalRevenue = filtered.reduce((s, r) => s + r.total_orders_rub, 0)
  const totalReturns = filtered.reduce((s, r) => s + r.total_returns_qty, 0)
  const totalSales = filtered.reduce((s, r) => s + r.total_sales_qty, 0)
  // Взвешенная средняя маржа по выручке: sum(margin * выручка) / sum(выручка)
  const avgMargin = totalRevenue
    ? filtered.reduce((s, r) => s + r.avg_margin_pct * r.total_orders_rub, 0) / totalRevenue
    : 0
  const totalStock = filtered.reduce((s, r) => s + r.current_stock, 0)
  const skuCount = data?.skus.length ?? 0
  // Сводный % выкупа по всем артикулам
  const avgBuyout = filtered.length ? filtered.reduce((s, r) => s + r.buyout_rate_pct, 0) / filtered.length : 0
  // Суммарная прогноз. прибыль и бюджет РК по всем дням всех артикулов
  const totalProfit = filtered.reduce((sum, sku) =>
    sum + Object.values(sku.days).reduce((s, d) => s + (d.profit_total ?? 0), 0), 0)
  const totalAdSpend = filtered.reduce((sum, sku) =>
    sum + Object.values(sku.days).reduce((s, d) => s + (d.ad_spend ?? 0), 0), 0)

  return (
    <div className="space-y-4">
      {/* ── Заголовок ──────────────────────────────── */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-xl font-bold text-gray-800">РнП — Рука на пульсе</h1>
          {data && <p className="text-xs text-gray-400 mt-0.5">Последние данные: {data.ref_date}</p>}
        </div>
        <div className="flex items-center gap-2 flex-wrap">
          {/* Календарь */}
          <DateRangePicker value={dateRange} onChange={setDateRange} />

          {/* Marketplace multi-select */}
          <MultiSelectDropdown
            options={[
              { value: 'wb', label: 'Wildberries', icon: <WBIcon /> },
              { value: 'ozon', label: 'Ozon', icon: <OzonIcon /> },
            ]}
            selected={selectedChannels}
            onChange={setSelectedChannels}
            placeholder="Маркетплейсы"
            selectedLabel="Маркетплейсы"
            searchPlaceholder="Поиск..."
          />

          {/* Article multi-select */}
          <MultiSelectDropdown
            options={articleOptions}
            selected={selectedArticles}
            onChange={setSelectedArticles}
            placeholder="Артикулы"
            selectedLabel="Артикулы"
            searchPlaceholder="Поиск по артикулу"
          />

          {/* Сортировка */}
          <select value={sortBy} onChange={(e) => setSortBy(e.target.value as typeof sortBy)}
            className="border border-gray-200 rounded-lg px-3 py-1.5 text-xs text-gray-600 bg-white focus:outline-none focus:ring-2 focus:ring-indigo-400 shadow-sm">
            <option value="orders">По заказам ↓</option>
            <option value="margin">По марже ↓</option>
            <option value="stock">По остатку ↑</option>
          </select>
          {/* Свернуть/развернуть все */}
          <button onClick={() => setAllCollapsed(v => !v)}
            className="border border-gray-200 rounded-lg px-3 py-1.5 text-xs text-gray-600 bg-white hover:bg-gray-50 shadow-sm transition-colors whitespace-nowrap">
            {allCollapsed ? '▶ Развернуть все' : '▼ Свернуть все'}
          </button>
        </div>
      </div>

      {/* ── Подсказка про ручной ввод ─────────────── */}
      <div className="flex items-center gap-2 text-xs text-gray-400 bg-gray-50 border border-gray-100 rounded-lg px-3 py-2">
        <span className="text-indigo-400">✎</span>
        <span>Нажмите на значение <strong>Выкупа</strong> или <strong>Логистики</strong> для ручного ввода. Пустое поле или <kbd className="px-1 bg-white border rounded text-[10px]">✕</kbd> — сброс на расчётное.</span>
      </div>

      {/* ── Сводная панель по всем артикулам ─────────── */}
      {data && (
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
          <div className="px-4 py-2.5 bg-gradient-to-r from-indigo-50 to-white border-b border-gray-100 flex items-center gap-2">
            <span className="text-indigo-500 text-sm">⊞</span>
            <span className="text-sm font-semibold text-gray-700">Сводные показатели</span>
            <span className="text-xs text-gray-400 ml-1">
              {(d => d.slice(8,10)+'.'+d.slice(5,7))(data.days[data.days.length - 1] ?? '')} — {(d => d.slice(8,10)+'.'+d.slice(5,7))(data.days[0] ?? '')} · {data.days.length} дн. · {skuCount} арт.
            </span>
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-9 divide-x divide-gray-100">
            {[
              { label: 'Заказы', value: fmtNum(totalOrders) + ' шт', sub: fmtRub(totalRevenue) + ' ₽', color: 'text-gray-800' },
              { label: 'Продажи', value: fmtNum(totalSales) + ' шт', sub: '', color: 'text-gray-800' },
              { label: 'Возвраты', value: fmtNum(totalReturns) + ' шт', sub: totalOrders > 0 ? ((totalReturns / totalOrders) * 100).toFixed(1) + '%' : '', color: totalReturns > 0 ? 'text-red-500' : 'text-gray-800' },
              { label: 'Ср. маржа', value: avgMargin.toFixed(1) + '%', sub: '', color: avgMargin >= 20 ? 'text-green-600' : avgMargin >= 10 ? 'text-yellow-600' : 'text-red-500' },
              { label: 'Ср. выкуп', value: avgBuyout.toFixed(1) + '%', sub: '', color: avgBuyout >= 45 ? 'text-green-600' : avgBuyout >= 30 ? 'text-yellow-600' : 'text-red-500' },
              { label: 'Прогноз. прибыль', value: fmtRub(totalProfit) + ' ₽', sub: '', color: totalProfit >= 0 ? 'text-green-600' : 'text-red-500' },
              { label: 'Бюджет РК', value: fmtRub(totalAdSpend) + ' ₽', sub: '', color: 'text-gray-800' },
              { label: 'Остаток', value: fmtNum(totalStock) + ' шт', sub: 'все склады', color: 'text-gray-800' },
              { label: 'Отфильтровано', value: filtered.length + ' / ' + skuCount, sub: 'артикулов', color: 'text-gray-800' },
            ].map((c) => (
              <div key={c.label} className="px-4 py-3">
                <div className="text-[10px] text-gray-400 uppercase tracking-wide">{c.label}</div>
                <div className={`text-base font-bold tabular-nums mt-0.5 ${c.color}`}>{c.value}</div>
                {c.sub && <div className="text-[10px] text-gray-400 mt-0.5">{c.sub}</div>}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Загрузка / ошибка ────────────────────── */}
      {isLoading && (
        <div className="flex items-center justify-center h-48 text-gray-400 text-sm">
          <svg className="animate-spin w-5 h-5 mr-2" viewBox="0 0 24 24" fill="none">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z"/>
          </svg>
          Загрузка...
        </div>
      )}

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-xl px-4 py-3 text-red-600 text-sm">
          Ошибка: {String(error)}
        </div>
      )}

      {!isLoading && !error && filtered.length === 0 && (
        <div className="bg-white rounded-xl border border-dashed border-gray-300 px-4 py-12 text-center text-gray-400 text-sm">
          Нет данных. Выполните синхронизацию с WB в разделе <strong>Интеграции</strong>.
        </div>
      )}

      {/* ── Сводные блоки по маркетплейсам ─────────── */}
      {data && filtered.length > 0 && (() => {
        const byChannel = new Map<string, RnPPivotSKU[]>()
        for (const s of filtered) {
          if (!byChannel.has(s.channel_type)) byChannel.set(s.channel_type, [])
          byChannel.get(s.channel_type)!.push(s)
        }
        // Показываем сводку только если каналов больше 1 ИЛИ выбран фильтр
        if (byChannel.size === 0) return null
        return (
          <div className="space-y-3">
            {Array.from(byChannel.entries()).map(([ch, skus]) => (
              <ChannelAggregateCard
                key={`agg-${ch}`}
                agg={buildChannelAggregate(skus, ch, data.days)}
                days={data.days}
                externalCollapsed={allCollapsed}
              />
            ))}
          </div>
        )
      })()}

      {/* ── Список SKU ───────────────────────────── */}
      <div className="space-y-4">
        {filtered.map((sku) => (
          <SkuPivotCard key={`${sku.sku_id}-${sku.channel_type}`} sku={sku} days={data?.days ?? []} onUpdate={handleUpdate} externalCollapsed={allCollapsed} />
        ))}
      </div>
    </div>
  )
}
