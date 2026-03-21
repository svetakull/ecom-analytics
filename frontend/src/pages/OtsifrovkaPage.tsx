import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { format, subDays, startOfDay } from 'date-fns'
import { otsifrovkaApi } from '@/api/endpoints'
import type { OtsifrovkaRow } from '@/types'
import DateRangePicker, { type DateRange } from '@/components/DateRangePicker'
import { MultiSelectDropdown, WBIcon, OzonIcon, LamodaIcon, ChannelIcon } from '@/components/MultiSelectDropdown'
import clsx from 'clsx'
import {
  TrendingUp,
  TrendingDown,
  ChevronUp,
  ChevronDown,
  ChevronsUpDown,
} from 'lucide-react'

// ── Helpers ────────────────────────────────────────────────────────────────

const fmt = (n: number, decimals = 0) =>
  n.toLocaleString('ru-RU', { minimumFractionDigits: decimals, maximumFractionDigits: decimals })

const fmtRub = (n: number) => `${fmt(n)} ₽`

const ABC_COLORS: Record<string, string> = {
  A: 'bg-emerald-100 text-emerald-800',
  B: 'bg-amber-100 text-amber-800',
  C: 'bg-red-100 text-red-800',
}

const CHANNEL_COLORS: Record<string, string> = {
  wb: 'bg-purple-100 text-purple-700',
  ozon: 'bg-blue-100 text-blue-700',
  lamoda: 'bg-gray-900 text-white',
}

type SortKey = keyof OtsifrovkaRow
type SortDir = 'asc' | 'desc'

// ── Summary card ──────────────────────────────────────────────────────────

function SumCard({
  label,
  value,
  sub,
  color = 'default',
}: {
  label: string
  value: string
  sub?: string
  color?: 'default' | 'green' | 'red' | 'blue'
}) {
  const colorMap = {
    default: 'text-gray-900',
    green: 'text-emerald-600',
    red: 'text-red-600',
    blue: 'text-blue-700',
  }
  return (
    <div className="bg-white rounded-xl border border-gray-200 px-4 py-3 min-w-[140px]">
      <div className="text-xs text-gray-500 mb-1 truncate">{label}</div>
      <div className={clsx('text-base font-bold', colorMap[color])}>{value}</div>
      {sub && <div className="text-xs text-gray-400 mt-0.5">{sub}</div>}
    </div>
  )
}

// ── Sort icon ──────────────────────────────────────────────────────────────

function SortIcon({ active, dir }: { active: boolean; dir: SortDir }) {
  if (!active) return <ChevronsUpDown size={12} className="text-gray-300 ml-0.5" />
  return dir === 'asc' ? (
    <ChevronUp size={12} className="text-blue-500 ml-0.5" />
  ) : (
    <ChevronDown size={12} className="text-blue-500 ml-0.5" />
  )
}

// ── Th ────────────────────────────────────────────────────────────────────

function Th({
  children,
  sortKey,
  sortState,
  onSort,
  className = '',
  title,
}: {
  children: React.ReactNode
  sortKey: SortKey
  sortState: { key: SortKey; dir: SortDir }
  onSort: (k: SortKey) => void
  className?: string
  title?: string
}) {
  const active = sortState.key === sortKey
  return (
    <th
      title={title}
      className={clsx(
        'px-2 py-2 text-[11px] font-medium text-gray-500 uppercase tracking-wide cursor-pointer select-none whitespace-nowrap',
        'hover:bg-gray-100 transition-colors',
        active && 'bg-gray-50 text-blue-700',
        className
      )}
      onClick={() => onSort(sortKey)}
    >
      <span className="flex items-center gap-0.5">
        {children}
        <SortIcon active={active} dir={sortState.dir} />
      </span>
    </th>
  )
}

// ── Main page ─────────────────────────────────────────────────────────────

function defaultRange(): DateRange {
  const yesterday = subDays(startOfDay(new Date()), 1)
  return { from: subDays(yesterday, 29), to: yesterday }
}

export default function OtsifrovkaPage() {
  const [dateRange, setDateRange] = useState<DateRange>(defaultRange)
  const [selectedChannels, setSelectedChannels] = useState<string[]>([])
  const [selectedArticles, setSelectedArticles] = useState<string[]>([])
  const [search, setSearch] = useState('')
  const [sort, setSort] = useState<{ key: SortKey; dir: SortDir }>({
    key: 'sales_rub',
    dir: 'desc',
  })

  const dateFrom = format(dateRange.from, 'yyyy-MM-dd')
  const dateTo = format(dateRange.to, 'yyyy-MM-dd')

  const { data, isLoading, isError } = useQuery({
    queryKey: ['otsifrovka', dateFrom, dateTo, selectedChannels],
    queryFn: () =>
      otsifrovkaApi
        .get({
          date_from: dateFrom,
          date_to: dateTo,
          ...(selectedChannels.length > 0 ? { channels: selectedChannels } : {}),
        })
        .then((r) => r.data),
    staleTime: 2 * 60 * 1000,
  })

  const handleSort = (key: SortKey) => {
    setSort((s) => ({
      key,
      dir: s.key === key ? (s.dir === 'desc' ? 'asc' : 'desc') : 'desc',
    }))
  }

  const articleOptions = useMemo(() => {
    if (!data?.rows) return []
    return data.rows.map((r) => ({
      value: `${r.sku_id}-${r.channel_id}`,
      label: r.seller_article,
      icon: <ChannelIcon type={r.channel_type} size={18} />,
    }))
  }, [data])

  const rows = useMemo(() => {
    if (!data?.rows) return []
    let list = [...data.rows]
    if (selectedArticles.length > 0) {
      list = list.filter((r) => selectedArticles.includes(`${r.sku_id}-${r.channel_id}`))
    }
    if (search.trim()) {
      const q = search.toLowerCase()
      list = list.filter(
        (r) =>
          r.seller_article.toLowerCase().includes(q) ||
          r.name.toLowerCase().includes(q) ||
          r.channel_name.toLowerCase().includes(q)
      )
    }
    list.sort((a, b) => {
      const av = a[sort.key] as number | string
      const bv = b[sort.key] as number | string
      if (typeof av === 'string' && typeof bv === 'string') {
        return sort.dir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av)
      }
      return sort.dir === 'asc' ? (av as number) - (bv as number) : (bv as number) - (av as number)
    })
    return list
  }, [data, selectedArticles, search, sort])

  const summary = data?.summary

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between gap-4 mb-4 flex-wrap">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Оцифровка — факт P&L</h1>
          {data && (
            <div className="text-xs text-gray-400 mt-0.5">
              {data.date_from} — {data.date_to} · {data.rows.length} SKU × канал
            </div>
          )}
        </div>

        <div className="flex items-center gap-2 flex-wrap">
          <DateRangePicker value={dateRange} onChange={setDateRange} />

          {/* Marketplace multi-select */}
          <MultiSelectDropdown
            options={[
              { value: 'wb', label: 'Wildberries', icon: <WBIcon /> },
              { value: 'ozon', label: 'Ozon', icon: <OzonIcon /> },
              { value: 'lamoda', label: 'Lamoda', icon: <LamodaIcon /> },
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

          {/* Text search */}
          <input
            type="text"
            placeholder="Поиск..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="border border-gray-200 rounded-lg px-3 py-1.5 text-sm w-44 focus:outline-none focus:ring-2 focus:ring-blue-500/30"
          />
        </div>
      </div>

      {/* Active filters */}
      {(selectedChannels.length > 0 || selectedArticles.length > 0 || search.trim()) && (
        <div className="flex items-center gap-2 mb-3 flex-wrap text-xs">
          <span className="text-gray-400">Фильтры:</span>
          {selectedChannels.map((ch) => (
            <span key={ch} className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-gray-50 text-blue-700 border border-blue-200">
              {ch === 'wb' ? 'Wildberries' : ch === 'ozon' ? 'Ozon' : ch === 'lamoda' ? 'Lamoda' : ch}
              <button onClick={() => setSelectedChannels(selectedChannels.filter((c) => c !== ch))} className="hover:text-blue-900">×</button>
            </span>
          ))}
          {selectedArticles.map((key) => {
            const row = data?.rows.find((r) => `${r.sku_id}-${r.channel_id}` === key)
            return (
              <span key={key} className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-gray-50 text-purple-700 border border-purple-200">
                {row?.seller_article || key}
                <button onClick={() => setSelectedArticles(selectedArticles.filter((a) => a !== key))} className="hover:text-purple-900">×</button>
              </span>
            )
          })}
          {search.trim() && (
            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-gray-100 text-gray-700 border border-gray-200">
              «{search}»
              <button onClick={() => setSearch('')} className="hover:text-gray-900">×</button>
            </span>
          )}
          <button
            onClick={() => { setSelectedChannels([]); setSelectedArticles([]); setSearch('') }}
            className="text-gray-400 hover:text-red-500 ml-1"
          >
            Сбросить всё
          </button>
        </div>
      )}

      {/* Summary cards */}
      {summary && (
        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-3 mb-4">
          <SumCard label="Заказы" value={fmt(summary.orders_qty) + ' шт'} sub={fmtRub(summary.orders_rub)} />
          <SumCard label="Продажи" value={fmt(summary.sales_qty) + ' шт'} sub={fmtRub(summary.sales_rub)} color="blue" />
          <SumCard label="Возвраты" value={fmt(summary.returns_qty) + ' шт'} sub={fmtRub(summary.returns_rub || 0)} />
          <SumCard label="К перечислению" value={fmtRub(summary.payout_rub)} color="blue" />
          <SumCard label="Прибыль" value={fmtRub(summary.profit_rub)} color={summary.profit_rub >= 0 ? 'green' : 'red'} sub={`Маржа ${fmt(summary.margin_pct, 1)}%`} />
          <SumCard label="Реклама" value={fmtRub(summary.ad_spend_rub)} />
          <SumCard label="Себестоимость" value={fmtRub(summary.cogs_rub)} />
          <SumCard label="Комиссия" value={fmtRub(summary.commission_rub)} />
          <SumCard label="Логистика" value={fmtRub(summary.logistics_rub)} />
          <SumCard label="Налоги (6%)" value={fmtRub(summary.tax_rub)} />
        </div>
      )}

      {/* Table */}
      <div className="rounded-xl border border-gray-200 bg-white">
        {isLoading && (
          <div className="flex items-center justify-center h-40 text-gray-400">Загрузка...</div>
        )}
        {isError && (
          <div className="flex items-center justify-center h-40 text-red-500">
            Ошибка загрузки данных
          </div>
        )}
        {!isLoading && !isError && rows.length === 0 && (
          <div className="flex items-center justify-center h-40 text-gray-400">
            Нет данных за период
          </div>
        )}
        {rows.length > 0 && (
          <table className="min-w-full text-sm" style={{ borderCollapse: 'separate', borderSpacing: 0 }}>
            <thead className="sticky top-0 z-20 border-b border-gray-200" style={{ background: '#f9fafb', boxShadow: '0 2px 4px rgba(0,0,0,0.08)' }}>
              <tr>
                {/* Sticky */}
                <th className="sticky left-0 z-30 bg-gray-50 px-2 py-2 text-left text-[11px] font-medium text-gray-500 uppercase w-10">Фото</th>
                <th className="sticky left-10 z-30 bg-gray-50 px-2 py-2 text-left text-[11px] font-medium text-gray-500 uppercase min-w-[150px] shadow-[4px_0_6px_-2px_rgba(0,0,0,0.1)]">Артикул / Канал</th>

                <th className="px-1 py-1 text-center text-[10px] text-gray-400 bg-gray-50 border-l border-gray-200" colSpan={3}>P&L</th>
                <th className="px-1 py-1 text-center text-[10px] text-gray-400 bg-gray-50 border-l border-gray-200" colSpan={2}>Продажи</th>
                <th className="px-1 py-1 text-center text-[10px] text-gray-400 bg-gray-50 border-l border-gray-200" colSpan={4}>Аналитика</th>
                <th className="px-1 py-1 text-center text-[10px] text-gray-400 bg-gray-50 border-l border-gray-200" colSpan={2}>Заказы</th>
                <th className="px-1 py-1 text-center text-[10px] text-gray-400 bg-gray-50 border-l border-gray-200">Продажи шт</th>
                <th className="px-1 py-1 text-center text-[10px] text-gray-400 bg-gray-50 border-l border-gray-200">Возвраты шт</th>
                <th className="px-1 py-1 text-center text-[10px] text-gray-400 bg-gray-50 border-l border-gray-200" colSpan={10}>Затраты</th>
                <th className="px-1 py-1 text-center text-[10px] text-gray-400 bg-gray-50 border-l border-gray-200" colSpan={3}>Доп.</th>
                <th className="px-1 py-1 text-center text-[10px] text-gray-400 bg-gray-50 border-l border-gray-200" colSpan={2}>Остаток</th>
              </tr>
              <tr>
                <th className="sticky left-0 z-30 bg-gray-50 px-2 py-1"></th>
                <th className="sticky left-10 z-30 bg-gray-50 px-2 py-1 shadow-[4px_0_6px_-2px_rgba(0,0,0,0.1)]"></th>

                {/* P&L — сразу после артикула */}
                <Th sortKey="profit_rub" sortState={sort} onSort={handleSort} title="Прибыль" className="border-l border-gray-200">Прибыль</Th>
                <Th sortKey="margin_pct" sortState={sort} onSort={handleSort} title="Маржинальность">Маржа%</Th>
                <Th sortKey="revenue_share_pct" sortState={sort} onSort={handleSort} title="Доля в общей выручке">Доля%</Th>

                {/* Продажи */}
                <Th sortKey="sales_rub" sortState={sort} onSort={handleSort} title="Продажи, ₽" className="border-l border-gray-200">Продажи ₽</Th>
                <Th sortKey="avg_price" sortState={sort} onSort={handleSort} title="Средняя цена продажи">Ср. цена</Th>

                {/* Аналитика */}
                <Th sortKey="abc_revenue" sortState={sort} onSort={handleSort} title="ABC по выручке" className="border-l border-gray-200">ABC В</Th>
                <Th sortKey="abc_profit" sortState={sort} onSort={handleSort} title="ABC по прибыли">ABC П</Th>
                <Th sortKey="buyout_rate_pct" sortState={sort} onSort={handleSort} title="% выкупа">Выкуп%</Th>
                <Th sortKey="drr_sales_pct" sortState={sort} onSort={handleSort} title="ДРР по продажам">ДРР%</Th>

                {/* Заказы */}
                <Th sortKey="orders_qty" sortState={sort} onSort={handleSort} title="Заказы, шт" className="border-l border-gray-200">Зак. шт</Th>
                <Th sortKey="orders_rub" sortState={sort} onSort={handleSort} title="Заказы, ₽">Зак. ₽</Th>

                {/* Продажи шт */}
                <Th sortKey="sales_qty" sortState={sort} onSort={handleSort} title="Всего продаж, шт" className="border-l border-gray-200">Прод. шт</Th>

                {/* Возвраты */}
                <Th sortKey="returns_qty" sortState={sort} onSort={handleSort} title="Отказы + возвраты" className="border-l border-gray-200">Возвр. шт</Th>

                {/* Затраты */}
                <Th sortKey="ad_spend_rub" sortState={sort} onSort={handleSort} title="Расходы на рекламу" className="border-l border-gray-200">Реклама</Th>
                <Th sortKey="logistics_rub" sortState={sort} onSort={handleSort} title="Стоимость логистики">Логист.</Th>
                <Th sortKey="commission_rub" sortState={sort} onSort={handleSort} title="Комиссия МП">Комис.</Th>
                <Th sortKey="fines_rub" sortState={sort} onSort={handleSort} title="Штрафы">Штрафы</Th>
                <Th sortKey="storage_rub" sortState={sort} onSort={handleSort} title="Хранение">Хран.</Th>
                <Th sortKey="acceptance_rub" sortState={sort} onSort={handleSort} title="Платная приёмка">Приёмка</Th>
                <Th sortKey="cogs_rub" sortState={sort} onSort={handleSort} title="Себестоимость продаж">Себест.</Th>
                <Th sortKey="tax_rub" sortState={sort} onSort={handleSort} title="Налоги">Налоги</Th>
                <Th sortKey="other_deductions_rub" sortState={sort} onSort={handleSort} title="Прочие удержания">Прочие</Th>
                <Th sortKey="payout_rub" sortState={sort} onSort={handleSort} title="К перечислению">К переч.</Th>

                {/* Доп */}
                <Th sortKey="realization_rub" sortState={sort} onSort={handleSort} title="Реализация (до СПП)" className="border-l border-gray-200">Реализ.</Th>
                <Th sortKey="compensation_rub" sortState={sort} onSort={handleSort} title="Компенсация (соинвест)">Компенс.</Th>
                <Th sortKey="return_rate_pct" sortState={sort} onSort={handleSort} title="Возвраты, ₽">Возвр. ₽</Th>

                {/* Остаток */}
                <Th sortKey="current_stock" sortState={sort} onSort={handleSort} title="Остатки, шт" className="border-l border-gray-200">Остаток</Th>
                <Th sortKey="turnover_days" sortState={sort} onSort={handleSort} title="Оборачиваемость по продажам">Обор. дн</Th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r, i) => (
                <tr
                  key={`${r.sku_id}-${r.channel_id}`}
                  className={clsx(
                    'border-b border-gray-100 hover:bg-gray-50/30 transition-colors',
                    i % 2 === 0 ? 'bg-white' : 'bg-[#f8f9fa]'
                  )}
                >
                  {/* Фото — sticky, явный фон чтобы не просвечивало при скролле */}
                  <td className={clsx(
                    'sticky left-0 z-10 px-2 py-1.5',
                    i % 2 === 0 ? 'bg-white' : 'bg-[#f8f9fa]'
                  )}>
                    {r.photo_url ? (
                      <img
                        src={r.photo_url}
                        alt=""
                        className="w-8 h-8 min-w-[32px] min-h-[32px] object-cover rounded shrink-0"
                        onError={(e) => {
                          ;(e.target as HTMLImageElement).style.display = 'none'
                        }}
                      />
                    ) : (
                      <div className="w-8 h-8 bg-gray-100 rounded flex items-center justify-center text-gray-300 text-xs">
                        —
                      </div>
                    )}
                  </td>

                  {/* Артикул / Канал — sticky */}
                  <td className={clsx(
                    'sticky left-10 z-10 px-2 py-1.5 max-w-[180px] shadow-[4px_0_6px_-2px_rgba(0,0,0,0.1)]',
                    i % 2 === 0 ? 'bg-white' : 'bg-[#f8f9fa]'
                  )}>
                    {r.mp_article ? (
                      <a
                        href={
                          r.channel_type === 'ozon'
                            ? `https://www.ozon.ru/product/${r.mp_article}/`
                            : r.channel_type === 'wb'
                            ? `https://www.wildberries.ru/catalog/${r.mp_article}/detail.aspx`
                            : undefined
                        }
                        target="_blank"
                        rel="noopener noreferrer"
                        className="font-medium text-gray-900 text-xs truncate hover:text-blue-600 hover:underline"
                        title={r.mp_article}
                      >
                        {r.seller_article}
                      </a>
                    ) : (
                      <div className="font-medium text-gray-900 text-xs truncate">{r.seller_article}</div>
                    )}
                    <div className="text-gray-500 text-[11px] truncate">{r.name}</div>
                    <span
                      className={clsx(
                        'inline-block text-[10px] px-1.5 py-0.5 rounded-full font-medium mt-0.5',
                        CHANNEL_COLORS[r.channel_type] || 'bg-gray-100 text-gray-600'
                      )}
                    >
                      {r.channel_name}
                    </span>
                  </td>

                  {/* P&L — сразу после артикула */}
                  <td className={clsx(
                    'px-2 py-1.5 text-right text-xs font-bold tabular-nums border-l border-gray-200',
                    r.profit_rub >= 0 ? 'text-emerald-600' : 'text-red-600'
                  )}>
                    <span className="flex items-center justify-end gap-0.5">
                      {r.profit_rub >= 0 ? <TrendingUp size={11} /> : <TrendingDown size={11} />}
                      {fmt(r.profit_rub)}
                    </span>
                  </td>
                  <td className={clsx(
                    'px-2 py-1.5 text-right text-xs font-bold tabular-nums border-l border-gray-100',
                    r.margin_pct >= 20 ? 'text-emerald-600' : r.margin_pct >= 0 ? 'text-amber-600' : 'text-red-600'
                  )}>
                    {fmt(r.margin_pct, 1)}%
                  </td>
                  <Td>{fmt(r.revenue_share_pct, 1)}%</Td>

                  {/* Продажи */}
                  <Td bold divider>{fmt(r.sales_rub)}</Td>
                  <Td>{fmt(r.avg_price)}</Td>

                  {/* Аналитика */}
                  <td className="px-2 py-1.5 text-center border-l border-gray-200">
                    <span className={clsx('inline-block text-xs font-bold px-2 py-0.5 rounded', ABC_COLORS[r.abc_revenue])}>{r.abc_revenue}</span>
                  </td>
                  <td className="px-2 py-1.5 text-center border-l border-gray-100">
                    <span className={clsx('inline-block text-xs font-bold px-2 py-0.5 rounded', ABC_COLORS[r.abc_profit])}>{r.abc_profit}</span>
                  </td>
                  <Td>{fmt(r.buyout_rate_pct, 1)}%</Td>
                  <Td>{fmt(r.drr_sales_pct, 1)}%</Td>

                  {/* Заказы */}
                  <Td divider>{fmt(r.orders_qty)}</Td>
                  <Td>{fmt(r.orders_rub)}</Td>

                  {/* Продажи шт */}
                  <Td divider>{fmt(r.sales_qty)}</Td>

                  {/* Возвраты шт */}
                  <Td warn={r.return_rate_pct > 30} divider>{fmt(r.returns_qty)}</Td>

                  {/* Затраты */}
                  <Td divider>{fmt(r.ad_spend_rub)}</Td>
                  <Td>{fmt(r.logistics_rub)}</Td>
                  <Td>{fmt(r.commission_rub)}</Td>
                  <Td warn={r.fines_rub > 0}>{fmt(r.fines_rub)}</Td>
                  <Td>{fmt(r.storage_rub)}</Td>
                  <Td>{fmt(r.acceptance_rub)}</Td>
                  <Td>{fmt(r.cogs_rub)}</Td>
                  <Td>{fmt(r.tax_rub)}</Td>
                  <Td>{fmt(r.other_deductions_rub)}</Td>
                  <Td bold>{fmt(r.payout_rub)}</Td>

                  {/* Доп */}
                  <Td divider>{fmt(r.realization_rub)}</Td>
                  <Td>{fmt(r.compensation_rub)}</Td>
                  <Td>{fmt(r.returns_rub)}</Td>

                  {/* Остаток */}
                  <Td divider>{fmt(r.current_stock)}</Td>
                  <td className={clsx(
                    'px-2 py-1.5 text-right text-xs tabular-nums border-l border-gray-100',
                    r.turnover_days < 14 ? 'text-red-600 font-medium' :
                    r.turnover_days < 30 ? 'text-amber-600' : 'text-gray-700'
                  )}>
                    {r.turnover_days >= 999 ? '∞' : fmt(r.turnover_days, 1)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}

// ── Td helper ─────────────────────────────────────────────────────────────

function Td({
  children,
  bold = false,
  warn = false,
  divider = false,
}: {
  children: React.ReactNode
  bold?: boolean
  warn?: boolean
  divider?: boolean
}) {
  return (
    <td
      className={clsx(
        'px-2 py-1.5 text-right text-xs tabular-nums border-l whitespace-nowrap',
        bold ? 'font-semibold text-gray-900' : 'text-gray-700',
        warn && 'text-orange-600 font-medium',
        divider ? 'border-gray-300' : 'border-gray-100'
      )}
    >
      {children}
    </td>
  )
}
