import { useState, useMemo } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { logisticsApi } from '@/api/endpoints'
import type {
  LogisticsOperation, LogisticsArticleSummary, LogisticsSummary,
  DimensionsComparison, KTRHistoryRecord, IRPHistoryRecord, KTRReferenceRow,
  LogisticsFilterOptions,
} from '@/types'
import clsx from 'clsx'
import {
  Download, Upload, RefreshCw, AlertTriangle, ChevronDown, ChevronUp,
  Plus, Trash2, Edit3, Check, X, Info,
} from 'lucide-react'

const fmtNum = (n: number, d = 2) => n.toLocaleString('ru-RU', { minimumFractionDigits: d, maximumFractionDigits: d })
const fmtRub = (n: number) => `${fmtNum(n)} \u20BD`
const fmtDateRu = (iso: string) => {
  if (!iso) return ''
  const [y, m, d] = iso.split('-')
  return `${d}.${m}.${y}`
}

/** Последнее воскресенье (включая сегодня, если сегодня вс) */
function getLastSunday(): Date {
  const d = new Date()
  const day = d.getDay() // 0=вс
  d.setDate(d.getDate() - (day === 0 ? 0 : day))
  d.setHours(0, 0, 0, 0)
  return d
}

function toISO(d: Date) {
  return d.toISOString().slice(0, 10)
}

type PeriodKey = '7d' | '1m' | '3m'
const PERIOD_LABELS: Record<PeriodKey, string> = { '7d': '7 дней', '1m': 'Месяц', '3m': 'Квартал' }

function calcPeriod(period: PeriodKey): { from: string; to: string } {
  const lastSun = getLastSunday()
  const to = toISO(lastSun)
  const from = new Date(lastSun)
  if (period === '7d') from.setDate(from.getDate() - 6)
  else if (period === '1m') from.setMonth(from.getMonth() - 1, from.getDate() + 1)
  else from.setMonth(from.getMonth() - 3, from.getDate() + 1)
  return { from: toISO(from), to }
}

type Tab = 'reports' | 'dimensions'
type DetailLevel = 'summary' | 'article' | 'operation'

export default function LogisticsPage() {
  const qc = useQueryClient()

  // ── Period State ──
  const defaultPeriod = calcPeriod('7d')
  const [periodKey, setPeriodKey] = useState<PeriodKey>('7d')
  const [dateFrom, setDateFrom] = useState(defaultPeriod.from)
  const [dateTo, setDateTo] = useState(defaultPeriod.to)
  const [showCalendar, setShowCalendar] = useState(false)

  const selectPeriod = (key: PeriodKey) => {
    const p = calcPeriod(key)
    setPeriodKey(key)
    setDateFrom(p.from)
    setDateTo(p.to)
    setPage(1)
  }

  const applyCustomDate = (isoDate: string) => {
    setDateTo(isoDate)
    // dateFrom остаётся, пересчитаем по текущему периоду
    const to = new Date(isoDate)
    const from = new Date(to)
    if (periodKey === '7d') from.setDate(from.getDate() - 6)
    else if (periodKey === '1m') from.setMonth(from.getMonth() - 1, from.getDate() + 1)
    else from.setMonth(from.getMonth() - 3, from.getDate() + 1)
    setDateFrom(toISO(from))
    setShowCalendar(false)
    setPage(1)
  }

  // ── Other State ──
  const [tab, setTab] = useState<Tab>('reports')
  const [detailLevel, setDetailLevel] = useState<DetailLevel>('article')
  const [selectedArticles, setSelectedArticles] = useState<string[]>([])
  const [statusFilter, setStatusFilter] = useState('')
  const [opTypeFilter, setOpTypeFilter] = useState('')
  const [warehouseFilter, setWarehouseFilter] = useState('')
  const [calcMethod, setCalcMethod] = useState<'card' | 'nomenclature'>('card')
  const [showKTRPanel, setShowKTRPanel] = useState(false)
  const [page, setPage] = useState(1)

  // ── Queries ──
  const filterParams = {
    date_from: dateFrom || undefined,
    date_to: dateTo || undefined,
    articles: selectedArticles.length ? selectedArticles : undefined,
    status: statusFilter || undefined,
    operation_type: opTypeFilter || undefined,
    warehouse: warehouseFilter || undefined,
  }

  const { data: filtersData } = useQuery({
    queryKey: ['logistics-filters'],
    queryFn: () => logisticsApi.filters().then(r => r.data),
  })

  const { data: summaryData, isLoading: summaryLoading } = useQuery({
    queryKey: ['logistics-summary', dateFrom, dateTo, selectedArticles],
    queryFn: () => logisticsApi.summary({
      date_from: dateFrom || undefined,
      date_to: dateTo || undefined,
      articles: selectedArticles.length ? selectedArticles : undefined,
    }).then(r => r.data),
  })

  const { data: opsData, isLoading: opsLoading } = useQuery({
    queryKey: ['logistics-operations', filterParams, page],
    queryFn: () => logisticsApi.operations({ ...filterParams, page, page_size: 50 }).then(r => r.data),
    enabled: detailLevel === 'operation' && tab === 'reports',
  })

  const { data: articleData, isLoading: articleLoading } = useQuery({
    queryKey: ['logistics-articles', dateFrom, dateTo, selectedArticles, statusFilter],
    queryFn: () => logisticsApi.byArticle({
      date_from: dateFrom || undefined,
      date_to: dateTo || undefined,
      articles: selectedArticles.length ? selectedArticles : undefined,
      status: statusFilter || undefined,
    }).then(r => r.data),
    enabled: detailLevel === 'article' && tab === 'reports',
  })

  const { data: dimsData } = useQuery({
    queryKey: ['logistics-dimensions', selectedArticles],
    queryFn: () => logisticsApi.dimensions({
      articles: selectedArticles.length ? selectedArticles : undefined,
    }).then(r => r.data),
    enabled: tab === 'dimensions',
  })

  const { data: ktrList } = useQuery({
    queryKey: ['ktr-list'],
    queryFn: () => logisticsApi.ktrList().then(r => r.data),
  })

  const { data: irpList } = useQuery({
    queryKey: ['irp-list'],
    queryFn: () => logisticsApi.irpList().then(r => r.data),
  })

  const { data: ktrRef } = useQuery({
    queryKey: ['ktr-reference'],
    queryFn: () => logisticsApi.ktrReference().then(r => r.data),
    enabled: showKTRPanel,
  })

  // ── Sync feedback ──
  const [syncMsg, setSyncMsg] = useState<{ type: 'ok' | 'err'; text: string } | null>(null)

  // ── Mutations ──
  const syncMut = useMutation({
    mutationFn: () => logisticsApi.sync(dateFrom, dateTo, calcMethod),
    onSuccess: (resp) => {
      const d = resp.data
      qc.invalidateQueries({ predicate: (q) => String(q.queryKey[0]).startsWith('logistics') })
      qc.invalidateQueries({ queryKey: ['ktr-list'] })
      qc.invalidateQueries({ queryKey: ['irp-list'] })
      if (d.error) {
        setSyncMsg({ type: 'err', text: d.error })
      } else {
        setSyncMsg({ type: 'ok', text: `Обработано ${d.processed} операций` + (d.warnings ? `, ${d.warnings} предупр.` : '') })
      }
      setTimeout(() => setSyncMsg(null), 8000)
    },
    onError: (err: any) => {
      const msg = err?.response?.data?.detail || err?.message || 'Ошибка синхронизации'
      setSyncMsg({ type: 'err', text: msg })
      setTimeout(() => setSyncMsg(null), 8000)
    },
  })

  const handleExport = async (format: 'xlsx' | 'csv') => {
    try {
      const resp = await logisticsApi.exportData(format, filterParams)
      const blob = new Blob([resp.data])
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `logistics_report.${format}`
      a.click()
      URL.revokeObjectURL(url)
    } catch (err: any) {
      setSyncMsg({ type: 'err', text: 'Ошибка экспорта: ' + (err?.message || 'неизвестная') })
      setTimeout(() => setSyncMsg(null), 5000)
    }
  }

  const summary: LogisticsSummary = summaryData || {
    total_expected: 0, total_actual: 0, total_difference: 0,
    total_overpay: 0, total_saving: 0,
    articles_total: 0, articles_overpay: 0, articles_saving: 0, articles_match: 0,
    current_ktr: null, current_irp: null, warnings_count: 0,
  }

  return (
    <div className="p-5 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Габариты и логистика WB</h1>
          <p className="text-sm text-gray-500 mt-0.5">Сверка габаритов и логистических расходов</p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={() => setShowKTRPanel(!showKTRPanel)}
            className="flex items-center gap-1.5 px-3 py-2 text-sm border border-gray-200 rounded-lg hover:bg-gray-50"
          >
            <Edit3 size={14} />
            КТР / ИРП
          </button>
          <button
            onClick={() => handleExport('xlsx')}
            className="flex items-center gap-1.5 px-3 py-2 text-sm border border-gray-200 rounded-lg hover:bg-gray-50"
          >
            <Download size={14} />
            Excel
          </button>
          <button
            onClick={() => handleExport('csv')}
            className="flex items-center gap-1.5 px-3 py-2 text-sm border border-gray-200 rounded-lg hover:bg-gray-50"
          >
            <Download size={14} />
            CSV
          </button>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 p-1 bg-gray-100 rounded-lg w-fit">
        {([['reports', 'Еженедельные отчёты'], ['dimensions', 'Габариты']] as const).map(([key, label]) => (
          <button
            key={key}
            onClick={() => setTab(key)}
            className={clsx(
              'px-4 py-1.5 text-sm rounded-md transition-colors',
              tab === key ? 'bg-white text-gray-900 shadow-sm font-medium' : 'text-gray-600 hover:text-gray-900'
            )}
          >
            {label}
          </button>
        ))}
      </div>

      {/* Filters */}
      <div className="flex flex-wrap gap-3 items-end bg-white rounded-xl border border-gray-200 p-4">
        {/* Period Selector */}
        <div>
          <label className="block text-xs text-gray-500 mb-1">Период</label>
          <div className="flex items-center gap-1">
            <div className="flex gap-0.5 p-0.5 bg-gray-100 rounded-lg">
              {(['7d', '1m', '3m'] as PeriodKey[]).map(k => (
                <button key={k} onClick={() => selectPeriod(k)}
                  className={clsx('px-3 py-1.5 text-sm rounded-md transition-colors',
                    periodKey === k ? 'bg-white shadow-sm font-medium text-gray-900' : 'text-gray-500 hover:text-gray-700')}>
                  {PERIOD_LABELS[k]}
                </button>
              ))}
            </div>
            <div className="relative ml-1">
              <button
                onClick={() => setShowCalendar(!showCalendar)}
                className="flex items-center gap-1.5 px-3 py-1.5 text-sm border border-gray-200 rounded-lg hover:bg-gray-50"
              >
                <span className="text-gray-600">{fmtDateRu(dateFrom)}</span>
                <span className="text-gray-400">—</span>
                <span className="text-gray-900 font-medium">{fmtDateRu(dateTo)}</span>
                <ChevronDown size={14} className="text-gray-400" />
              </button>
              {showCalendar && (
                <div className="absolute top-full mt-1 left-0 z-20 bg-white border border-gray-200 rounded-xl shadow-lg p-3">
                  <div className="text-xs text-gray-500 mb-2">Отчёт по дату (воскресенье):</div>
                  <input
                    type="date"
                    value={dateTo}
                    onChange={e => applyCustomDate(e.target.value)}
                    className="border border-gray-200 rounded-lg px-3 py-1.5 text-sm w-full"
                  />
                  <div className="text-[10px] text-gray-400 mt-1.5">
                    Период {PERIOD_LABELS[periodKey]} до выбранной даты
                  </div>
                  <button onClick={() => setShowCalendar(false)}
                    className="mt-2 w-full text-xs text-gray-500 hover:text-gray-700">Закрыть</button>
                </div>
              )}
            </div>
          </div>
        </div>

        {filtersData && (
          <>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Статус</label>
              <select value={statusFilter} onChange={e => { setStatusFilter(e.target.value); setPage(1) }}
                className="border border-gray-200 rounded-lg px-3 py-1.5 text-sm">
                <option value="">Все</option>
                <option value="Переплата">Переплата</option>
                <option value="Экономия">Экономия</option>
                <option value="Соответствует">Соответствует</option>
              </select>
            </div>

            {tab === 'reports' && (
              <>
                <div>
                  <label className="block text-xs text-gray-500 mb-1">Тип операции</label>
                  <select value={opTypeFilter} onChange={e => { setOpTypeFilter(e.target.value); setPage(1) }}
                    className="border border-gray-200 rounded-lg px-3 py-1.5 text-sm">
                    <option value="">Все</option>
                    {filtersData.operation_types.map(t => <option key={t} value={t}>{t}</option>)}
                  </select>
                </div>
                <div>
                  <label className="block text-xs text-gray-500 mb-1">Склад</label>
                  <select value={warehouseFilter} onChange={e => { setWarehouseFilter(e.target.value); setPage(1) }}
                    className="border border-gray-200 rounded-lg px-3 py-1.5 text-sm">
                    <option value="">Все</option>
                    {filtersData.warehouses.map(w => <option key={w} value={w}>{w}</option>)}
                  </select>
                </div>
              </>
            )}
          </>
        )}

        <div>
          <label className="block text-xs text-gray-500 mb-1">Метод расчёта</label>
          <div className="flex gap-0.5 p-0.5 bg-gray-100 rounded-lg">
            {([['card', 'По карточке'], ['nomenclature', 'По номенклатуре']] as const).map(([k, l]) => (
              <button key={k} onClick={() => setCalcMethod(k)}
                className={clsx('px-3 py-1 text-xs rounded-md', calcMethod === k ? 'bg-white shadow-sm font-medium' : 'text-gray-600')}>
                {l}
              </button>
            ))}
          </div>
        </div>

        <button
          onClick={() => syncMut.mutate()}
          disabled={syncMut.isPending || !dateFrom || !dateTo}
          className={clsx(
            'flex items-center gap-1.5 px-4 py-1.5 text-sm rounded-lg',
            syncMut.isPending ? 'bg-gray-100 text-gray-400' : 'bg-blue-600 text-white hover:bg-blue-700'
          )}
        >
          <RefreshCw size={14} className={syncMut.isPending ? 'animate-spin' : ''} />
          {syncMut.isPending ? 'Загрузка...' : 'Загрузить данные'}
        </button>

        {syncMsg && (
          <div className={clsx('flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-lg',
            syncMsg.type === 'ok' ? 'bg-green-50 text-green-700 border border-green-200' : 'bg-red-50 text-red-700 border border-red-200')}>
            {syncMsg.type === 'ok' ? <Check size={14} /> : <AlertTriangle size={14} />}
            {syncMsg.text}
          </div>
        )}
      </div>

      {/* Summary Cards */}
      {tab === 'reports' && (
        <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3">
          <SummaryCard label="Ожидаемая" value={fmtRub(summary.total_expected)} />
          <SummaryCard label="Фактическая" value={fmtRub(summary.total_actual)} />
          <SummaryCard label="Разница" value={fmtRub(summary.total_difference)}
            color={summary.total_difference > 0 ? 'red' : summary.total_difference < 0 ? 'green' : undefined} />
          <SummaryCard label="Переплата" value={fmtRub(summary.total_overpay)} sub={`${summary.articles_overpay} арт.`} color="red" />
          <SummaryCard label="Экономия" value={fmtRub(summary.total_saving)} sub={`${summary.articles_saving} арт.`} color="green" />
          <SummaryCard label="КТР / ИРП"
            value={`${summary.current_ktr ?? '—'} / ${summary.current_irp != null ? summary.current_irp + '%' : '—'}`}
            sub={summary.warnings_count > 0 ? `${summary.warnings_count} предупр.` : ''} />
        </div>
      )}

      {/* Detail Level Toggle */}
      {tab === 'reports' && (
        <div className="flex gap-1 p-1 bg-gray-100 rounded-lg w-fit">
          {([['summary', 'Сводка'], ['article', 'По артикулу'], ['operation', 'По операции']] as const).map(([k, l]) => (
            <button key={k} onClick={() => { setDetailLevel(k); setPage(1) }}
              className={clsx('px-3 py-1 text-sm rounded-md', detailLevel === k ? 'bg-white shadow-sm font-medium' : 'text-gray-600')}>
              {l}
            </button>
          ))}
        </div>
      )}

      {/* Content */}
      {tab === 'reports' && detailLevel === 'summary' && <SummaryView summary={summary} />}
      {tab === 'reports' && detailLevel === 'article' && (
        <ArticleTable data={articleData?.articles || []} loading={articleLoading} />
      )}
      {tab === 'reports' && detailLevel === 'operation' && (
        <OperationsTable
          data={opsData?.operations || []} total={opsData?.total || 0}
          page={page} pageSize={50} onPageChange={setPage} loading={opsLoading}
        />
      )}
      {tab === 'dimensions' && <DimensionsTable data={dimsData?.items || []} />}

      {/* KTR/IRP Panel */}
      {showKTRPanel && (
        <KTRIRPPanel
          ktrList={ktrList || []} irpList={irpList || []} ktrRef={ktrRef || []}
          onClose={() => setShowKTRPanel(false)}
        />
      )}
    </div>
  )
}

// ── Summary Card ──
function SummaryCard({ label, value, sub, color }: {
  label: string; value: string; sub?: string; color?: 'red' | 'green'
}) {
  return (
    <div className="bg-white rounded-xl border border-gray-200 p-4">
      <div className="text-xs text-gray-500 mb-1">{label}</div>
      <div className={clsx('text-lg font-semibold', color === 'red' && 'text-red-600', color === 'green' && 'text-green-600')}>
        {value}
      </div>
      {sub && <div className="text-xs text-gray-400 mt-0.5">{sub}</div>}
    </div>
  )
}

// ── Summary View ──
function SummaryView({ summary }: { summary: LogisticsSummary }) {
  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6 space-y-4">
      <h3 className="font-semibold text-gray-900">Итоги по периоду</h3>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
        <div>
          <span className="text-gray-500">Всего артикулов:</span>
          <span className="ml-2 font-medium">{summary.articles_total}</span>
        </div>
        <div>
          <span className="text-gray-500">Переплата:</span>
          <span className="ml-2 font-medium text-red-600">{summary.articles_overpay} арт.</span>
        </div>
        <div>
          <span className="text-gray-500">Экономия:</span>
          <span className="ml-2 font-medium text-green-600">{summary.articles_saving} арт.</span>
        </div>
        <div>
          <span className="text-gray-500">Соответствует:</span>
          <span className="ml-2 font-medium">{summary.articles_match} арт.</span>
        </div>
      </div>
      {summary.warnings_count > 0 && (
        <div className="flex items-center gap-2 p-3 bg-amber-50 border border-amber-200 rounded-lg text-sm text-amber-700">
          <AlertTriangle size={16} />
          {summary.warnings_count} операций с предупреждениями (КТР или тарифы)
        </div>
      )}
    </div>
  )
}

// ── Status Badge ──
function StatusBadge({ status }: { status: string }) {
  const map: Record<string, string> = {
    'Переплата': 'bg-red-100 text-red-700',
    'Экономия': 'bg-green-100 text-green-700',
    'Соответствует': 'bg-gray-100 text-gray-600',
    'Занижение': 'bg-red-100 text-red-700',
    'Превышение': 'bg-green-100 text-green-700',
    'Не заполнены': 'bg-yellow-100 text-yellow-700',
  }
  return (
    <span className={clsx('px-2 py-0.5 rounded-full text-xs font-medium', map[status] || 'bg-gray-100 text-gray-600')}>
      {status}
    </span>
  )
}

// ── Article Table ──
function ArticleTable({ data, loading }: { data: LogisticsArticleSummary[]; loading: boolean }) {
  const [sortKey, setSortKey] = useState<string>('total_difference')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')

  const sorted = useMemo(() => {
    return [...data].sort((a, b) => {
      const va = (a as any)[sortKey] ?? 0
      const vb = (b as any)[sortKey] ?? 0
      return sortDir === 'asc' ? va - vb : vb - va
    })
  }, [data, sortKey, sortDir])

  const toggleSort = (key: string) => {
    if (sortKey === key) setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    else { setSortKey(key); setSortDir('desc') }
  }

  const SortIcon = ({ col }: { col: string }) => sortKey === col
    ? (sortDir === 'asc' ? <ChevronUp size={12} /> : <ChevronDown size={12} />)
    : null

  if (loading) return <div className="text-center py-10 text-gray-400">Загрузка...</div>
  if (!data.length) return <div className="text-center py-10 text-gray-400">Нет данных. Загрузите отчёт.</div>

  return (
    <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 border-b border-gray-200 text-xs uppercase text-gray-500">
              <th className="px-3 py-2 text-left">Артикул</th>
              <th className="px-3 py-2 text-left">nmId</th>
              <th className="px-3 py-2 text-right cursor-pointer" onClick={() => toggleSort('operations_count')}>
                <span className="inline-flex items-center gap-1">Операций <SortIcon col="operations_count" /></span>
              </th>
              <th className="px-3 py-2 text-right cursor-pointer" onClick={() => toggleSort('total_expected')}>
                <span className="inline-flex items-center gap-1">Ожидаемая <SortIcon col="total_expected" /></span>
              </th>
              <th className="px-3 py-2 text-right cursor-pointer" onClick={() => toggleSort('total_actual')}>
                <span className="inline-flex items-center gap-1">Фактическая <SortIcon col="total_actual" /></span>
              </th>
              <th className="px-3 py-2 text-right cursor-pointer" onClick={() => toggleSort('total_difference')}>
                <span className="inline-flex items-center gap-1">Разница <SortIcon col="total_difference" /></span>
              </th>
              <th className="px-3 py-2 text-right">Объём карт. (л)</th>
              <th className="px-3 py-2 text-right">Объём ном. (л)</th>
              <th className="px-3 py-2 text-center">Статус габ.</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map(art => (
              <tr key={`${art.seller_article}-${art.nm_id}`} className="border-b border-gray-100 hover:bg-gray-50">
                <td className="px-3 py-2 font-medium">{art.seller_article}</td>
                <td className="px-3 py-2 text-gray-500">{art.nm_id}</td>
                <td className="px-3 py-2 text-right">{art.operations_count}</td>
                <td className="px-3 py-2 text-right">{fmtRub(art.total_expected)}</td>
                <td className="px-3 py-2 text-right">{fmtRub(art.total_actual)}</td>
                <td className={clsx('px-3 py-2 text-right font-medium',
                  art.total_difference > 0 ? 'text-red-600' : art.total_difference < 0 ? 'text-green-600' : '')}>
                  {fmtRub(art.total_difference)}
                </td>
                <td className="px-3 py-2 text-right">{fmtNum(art.volume_card, 3)}</td>
                <td className="px-3 py-2 text-right">{fmtNum(art.volume_nomenclature, 3)}</td>
                <td className="px-3 py-2 text-center"><StatusBadge status={art.dimensions_status} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Operations Table ──
function OperationsTable({ data, total, page, pageSize, onPageChange, loading }: {
  data: LogisticsOperation[]; total: number; page: number; pageSize: number;
  onPageChange: (p: number) => void; loading: boolean;
}) {
  if (loading) return <div className="text-center py-10 text-gray-400">Загрузка...</div>
  if (!data.length) return <div className="text-center py-10 text-gray-400">Нет данных. Загрузите отчёт.</div>

  const totalPages = Math.ceil(total / pageSize)

  return (
    <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="bg-gray-50 border-b border-gray-200 text-[10px] uppercase text-gray-500">
              <th className="px-2 py-2 text-left">Артикул</th>
              <th className="px-2 py-2">nmId</th>
              <th className="px-2 py-2 text-left">Тип</th>
              <th className="px-2 py-2 text-left">Склад</th>
              <th className="px-2 py-2">Дата</th>
              <th className="px-2 py-2">Коэф.</th>
              <th className="px-2 py-2">КТР</th>
              <th className="px-2 py-2">ИРП%</th>
              <th className="px-2 py-2">V карт.</th>
              <th className="px-2 py-2">V ном.</th>
              <th className="px-2 py-2">V WB</th>
              <th className="px-2 py-2">Цена</th>
              <th className="px-2 py-2">Ожид.</th>
              <th className="px-2 py-2">Факт.</th>
              <th className="px-2 py-2">Разн.</th>
              <th className="px-2 py-2">Статус</th>
              <th className="px-2 py-2">Габ.</th>
            </tr>
          </thead>
          <tbody>
            {data.map(op => (
              <tr key={op.id} className={clsx('border-b border-gray-100 hover:bg-gray-50',
                op.tariff_missing && 'bg-red-50', op.ktr_needs_check && !op.tariff_missing && 'bg-yellow-50')}>
                <td className="px-2 py-1.5 font-medium">{op.seller_article}</td>
                <td className="px-2 py-1.5 text-center text-gray-500">{op.nm_id}</td>
                <td className="px-2 py-1.5">{op.operation_type}</td>
                <td className="px-2 py-1.5">{op.warehouse}</td>
                <td className="px-2 py-1.5 text-center">{op.operation_date}</td>
                <td className="px-2 py-1.5 text-center">{fmtNum(op.warehouse_coef, 3)}</td>
                <td className="px-2 py-1.5 text-center">{fmtNum(op.ktr_value)}</td>
                <td className="px-2 py-1.5 text-center">{fmtNum(op.irp_value)}%</td>
                <td className="px-2 py-1.5 text-center">{fmtNum(op.volume_card_liters, 3)}</td>
                <td className="px-2 py-1.5 text-center">{fmtNum(op.volume_nomenclature_liters, 3)}</td>
                <td className="px-2 py-1.5 text-center">{fmtNum(op.calculated_wb_volume, 3)}</td>
                <td className="px-2 py-1.5 text-right">{fmtNum(op.retail_price, 0)}</td>
                <td className="px-2 py-1.5 text-right">{fmtRub(op.expected_logistics)}</td>
                <td className="px-2 py-1.5 text-right">{fmtRub(op.actual_logistics)}</td>
                <td className={clsx('px-2 py-1.5 text-right font-medium',
                  op.difference > 0 ? 'text-red-600' : op.difference < 0 ? 'text-green-600' : '')}>
                  {fmtRub(op.difference)}
                </td>
                <td className="px-2 py-1.5 text-center"><StatusBadge status={op.operation_status} /></td>
                <td className="px-2 py-1.5 text-center"><StatusBadge status={op.dimensions_status} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {totalPages > 1 && (
        <div className="flex items-center justify-between px-4 py-3 border-t border-gray-200">
          <span className="text-xs text-gray-500">
            {((page - 1) * pageSize) + 1}–{Math.min(page * pageSize, total)} из {total}
          </span>
          <div className="flex gap-1">
            <button onClick={() => onPageChange(page - 1)} disabled={page <= 1}
              className="px-3 py-1 text-xs border rounded hover:bg-gray-50 disabled:opacity-30">Назад</button>
            <button onClick={() => onPageChange(page + 1)} disabled={page >= totalPages}
              className="px-3 py-1 text-xs border rounded hover:bg-gray-50 disabled:opacity-30">Вперёд</button>
          </div>
        </div>
      )}
    </div>
  )
}

// ── Dimensions Table ──
function DimensionsTable({ data }: { data: DimensionsComparison[] }) {
  if (!data.length) return <div className="text-center py-10 text-gray-400">Нет данных о габаритах. Загрузите отчёт номенклатур и синхронизируйте карточки.</div>

  return (
    <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 border-b border-gray-200 text-xs uppercase text-gray-500">
              <th className="px-3 py-2 text-left">Артикул</th>
              <th className="px-3 py-2">nmId</th>
              <th className="px-3 py-2 text-left">Название</th>
              <th className="px-3 py-2 text-right" colSpan={2}>Карточка (Д×Ш×В / V)</th>
              <th className="px-3 py-2 text-right" colSpan={2}>Номенклатура (Д×Ш×В / V)</th>
              <th className="px-3 py-2 text-right">Разница V</th>
              <th className="px-3 py-2 text-center">Статус</th>
            </tr>
          </thead>
          <tbody>
            {data.map(d => (
              <tr key={d.nm_id} className="border-b border-gray-100 hover:bg-gray-50">
                <td className="px-3 py-2 font-medium">{d.seller_article}</td>
                <td className="px-3 py-2 text-center text-gray-500">{d.nm_id}</td>
                <td className="px-3 py-2 text-gray-600 truncate max-w-[200px]">{d.sku_name}</td>
                <td className="px-3 py-2 text-right text-gray-500">
                  {d.length_card}×{d.width_card}×{d.height_card}
                </td>
                <td className="px-3 py-2 text-right font-medium">{fmtNum(d.volume_card, 3)} л</td>
                <td className="px-3 py-2 text-right text-gray-500">
                  {d.length_nom}×{d.width_nom}×{d.height_nom}
                </td>
                <td className="px-3 py-2 text-right font-medium">{fmtNum(d.volume_nomenclature, 3)} л</td>
                <td className={clsx('px-3 py-2 text-right font-medium',
                  d.volume_difference > 0.05 ? 'text-red-600' : d.volume_difference < -0.05 ? 'text-green-600' : '')}>
                  {d.volume_difference > 0 ? '+' : ''}{fmtNum(d.volume_difference, 3)} л
                </td>
                <td className="px-3 py-2 text-center"><StatusBadge status={d.dimensions_status} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── KTR/IRP Management Panel ──
function KTRIRPPanel({ ktrList, irpList, ktrRef, onClose }: {
  ktrList: KTRHistoryRecord[]; irpList: IRPHistoryRecord[];
  ktrRef: KTRReferenceRow[]; onClose: () => void;
}) {
  const qc = useQueryClient()
  const [activeTab, setActiveTab] = useState<'ktr' | 'irp' | 'reference'>('ktr')
  const [newFrom, setNewFrom] = useState('')
  const [newTo, setNewTo] = useState('')
  const [newValue, setNewValue] = useState('')

  const createKTR = useMutation({
    mutationFn: () => logisticsApi.ktrCreate({ date_from: newFrom, date_to: newTo, value: parseFloat(newValue) }),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['ktr-list'] }); setNewFrom(''); setNewTo(''); setNewValue('') },
  })
  const deleteKTR = useMutation({
    mutationFn: (id: number) => logisticsApi.ktrDelete(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['ktr-list'] }),
  })
  const createIRP = useMutation({
    mutationFn: () => logisticsApi.irpCreate({ date_from: newFrom, date_to: newTo, value: parseFloat(newValue) }),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['irp-list'] }); setNewFrom(''); setNewTo(''); setNewValue('') },
  })
  const deleteIRP = useMutation({
    mutationFn: (id: number) => logisticsApi.irpDelete(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['irp-list'] }),
  })

  return (
    <div className="fixed inset-0 bg-black/30 z-50 flex justify-end" onClick={e => { if (e.target === e.currentTarget) onClose() }}>
      <div className="w-[600px] bg-white h-full shadow-xl overflow-y-auto p-6 space-y-4">
        <div className="flex items-center justify-between">
          <h2 className="text-lg font-bold">Управление КТР / ИРП</h2>
          <button onClick={onClose} className="p-1 hover:bg-gray-100 rounded"><X size={20} /></button>
        </div>

        {/* Tabs */}
        <div className="flex gap-1 p-1 bg-gray-100 rounded-lg">
          {([['ktr', 'КТР'], ['irp', 'ИРП'], ['reference', 'Справочник']] as const).map(([k, l]) => (
            <button key={k} onClick={() => setActiveTab(k)}
              className={clsx('flex-1 px-3 py-1.5 text-sm rounded-md',
                activeTab === k ? 'bg-white shadow-sm font-medium' : 'text-gray-600')}>
              {l}
            </button>
          ))}
        </div>

        {/* Add form */}
        {activeTab !== 'reference' && (
          <div className="flex gap-2 items-end">
            <div>
              <label className="block text-xs text-gray-500 mb-1">С</label>
              <input type="date" value={newFrom} onChange={e => setNewFrom(e.target.value)}
                className="border border-gray-200 rounded px-2 py-1 text-sm w-32" />
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">По</label>
              <input type="date" value={newTo} onChange={e => setNewTo(e.target.value)}
                className="border border-gray-200 rounded px-2 py-1 text-sm w-32" />
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">{activeTab === 'ktr' ? 'КТР' : 'ИРП %'}</label>
              <input type="number" step="0.01" value={newValue} onChange={e => setNewValue(e.target.value)}
                className="border border-gray-200 rounded px-2 py-1 text-sm w-20" />
            </div>
            <button
              onClick={() => activeTab === 'ktr' ? createKTR.mutate() : createIRP.mutate()}
              disabled={!newFrom || !newTo || !newValue}
              className="flex items-center gap-1 px-3 py-1 text-sm bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-30"
            >
              <Plus size={14} /> Добавить
            </button>
          </div>
        )}

        {/* KTR List */}
        {activeTab === 'ktr' && (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-xs text-gray-500">
                <th className="py-2 text-left">Период</th>
                <th className="py-2 text-right">КТР</th>
                <th className="py-2 w-10"></th>
              </tr>
            </thead>
            <tbody>
              {ktrList.map(r => (
                <tr key={r.id} className="border-b border-gray-100">
                  <td className="py-2">{r.date_from} — {r.date_to}</td>
                  <td className="py-2 text-right font-medium">{r.value}</td>
                  <td className="py-2">
                    <button onClick={() => deleteKTR.mutate(r.id)} className="p-1 hover:bg-red-50 rounded text-red-500">
                      <Trash2 size={14} />
                    </button>
                  </td>
                </tr>
              ))}
              {!ktrList.length && <tr><td colSpan={3} className="py-4 text-center text-gray-400">Нет записей</td></tr>}
            </tbody>
          </table>
        )}

        {/* IRP List */}
        {activeTab === 'irp' && (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-xs text-gray-500">
                <th className="py-2 text-left">Период</th>
                <th className="py-2 text-right">ИРП %</th>
                <th className="py-2 w-10"></th>
              </tr>
            </thead>
            <tbody>
              {irpList.map(r => (
                <tr key={r.id} className="border-b border-gray-100">
                  <td className="py-2">{r.date_from} — {r.date_to}</td>
                  <td className="py-2 text-right font-medium">{r.value}%</td>
                  <td className="py-2">
                    <button onClick={() => deleteIRP.mutate(r.id)} className="p-1 hover:bg-red-50 rounded text-red-500">
                      <Trash2 size={14} />
                    </button>
                  </td>
                </tr>
              ))}
              {!irpList.length && <tr><td colSpan={3} className="py-4 text-center text-gray-400">Нет записей</td></tr>}
            </tbody>
          </table>
        )}

        {/* Reference Table */}
        {activeTab === 'reference' && ktrRef.length > 0 && (
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b text-[10px] uppercase text-gray-500">
                <th className="py-2 text-left">Локализация %</th>
                <th className="py-2 text-right">КТР до 23.03</th>
                <th className="py-2 text-right">КТР с 23.03</th>
                <th className="py-2 text-right">КРП (ИРП) %</th>
              </tr>
            </thead>
            <tbody>
              {ktrRef.map((r, i) => (
                <tr key={i} className="border-b border-gray-100">
                  <td className="py-1.5">{fmtNum(r.localization_min, 2)} – {fmtNum(r.localization_max, 2)}</td>
                  <td className="py-1.5 text-right">{r.ktr_before}</td>
                  <td className="py-1.5 text-right">{r.ktr_after}</td>
                  <td className="py-1.5 text-right">{r.krp_irp}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
