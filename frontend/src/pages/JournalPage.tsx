import { useState, useMemo, useRef, useCallback, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { format, subDays, startOfDay, startOfYear } from 'date-fns'
import DateRangePicker, { type DateRange } from '@/components/DateRangePicker'
import JournalEntryModal from '@/components/JournalEntryModal'
import StatementUploadModal from '@/components/StatementUploadModal'
import ReceiptsUploadModal from '@/components/ReceiptsUploadModal'
import { api } from '@/api/client'
import { Plus, Paperclip, Camera, Pencil, Trash2, RefreshCw } from 'lucide-react'
import clsx from 'clsx'

const fmt = (n: number) =>
  n.toLocaleString('ru-RU', { minimumFractionDigits: 0, maximumFractionDigits: 0 })

const fmtDate = (s: string) => {
  const d = new Date(s + 'T00:00:00')
  return d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: 'numeric' })
}

type EntryType = 'expense' | 'income' | 'transfer'

interface JournalEntry {
  id: number
  entry_type: EntryType
  amount: number
  nds_amount: number
  is_recurring: boolean
  recurrence_rule: string | null
  recurrence_day: number | null
  scheduled_date: string | null
  backfill_from: string | null
  account_name: string
  category: string
  counterparty: string
  description: string
  is_distributed: boolean
  is_official: boolean
  created_at: string
}

interface JournalData {
  items: JournalEntry[]
  total: number
}

const TYPE_LABELS: Record<EntryType, string> = {
  expense: 'Расход',
  income: 'Доход',
  transfer: 'Перевод',
}

const TYPE_COLORS: Record<EntryType, string> = {
  expense: 'bg-red-100 text-red-700',
  income: 'bg-emerald-100 text-emerald-700',
  transfer: 'bg-blue-100 text-blue-700',
}

const RECURRENCE_LABELS: Record<string, string> = {
  monthly: 'Ежемесячно',
  weekly: 'Еженедельно',
}

function defaultRange(): DateRange {
  const now = new Date()
  return { from: startOfYear(now), to: subDays(startOfDay(now), 1) }
}

// ── Конфиг колонок журнала ────────────────────────────────────────────────
interface ColumnConfig {
  key: string
  label: string
  width: number
  align: 'left' | 'right' | 'center'
}

const DEFAULT_COLUMNS: ColumnConfig[] = [
  { key: 'date',         label: 'Дата',       width: 110, align: 'left' },
  { key: 'entry_type',   label: 'Тип',        width: 90,  align: 'left' },
  { key: 'amount',       label: 'Сумма',      width: 120, align: 'right' },
  { key: 'nds_amount',   label: 'НДС',        width: 90,  align: 'right' },
  { key: 'account_name', label: 'Счёт',       width: 130, align: 'left' },
  { key: 'category',     label: 'Статья',     width: 150, align: 'left' },
  { key: 'counterparty', label: 'Контрагент', width: 150, align: 'left' },
  { key: 'description',  label: 'Описание',   width: 220, align: 'left' },
  { key: 'recurring',    label: 'Повтор',     width: 90,  align: 'center' },
  { key: 'actions',      label: 'Действия',   width: 90,  align: 'center' },
]

const COLUMNS_STORAGE_KEY = 'ecom-analytics:journal-columns'

function loadColumns(): ColumnConfig[] {
  try {
    const stored = localStorage.getItem(COLUMNS_STORAGE_KEY)
    if (stored) {
      const parsed = JSON.parse(stored) as ColumnConfig[]
      // Merge: keep stored order/widths, add new columns at end
      const storedKeys = new Set(parsed.map(c => c.key))
      const merged = [...parsed]
      for (const c of DEFAULT_COLUMNS) {
        if (!storedKeys.has(c.key)) merged.push(c)
      }
      // Remove obsolete
      return merged.filter(c => DEFAULT_COLUMNS.some(d => d.key === c.key))
    }
  } catch {}
  return [...DEFAULT_COLUMNS]
}

function saveColumns(cols: ColumnConfig[]): void {
  try {
    localStorage.setItem(COLUMNS_STORAGE_KEY, JSON.stringify(cols))
  } catch {}
}

export default function JournalPage() {
  const [dateRange, setDateRange] = useState<DateRange>(defaultRange)
  const [filterType, setFilterType] = useState<string>('all')
  const [filterAccount, setFilterAccount] = useState<string>('')
  const [filterCategory, setFilterCategory] = useState<string>('')
  const [modalOpen, setModalOpen] = useState(false)
  const [uploadOpen, setUploadOpen] = useState(false)
  const [receiptsOpen, setReceiptsOpen] = useState(false)
  const [editEntry, setEditEntry] = useState<JournalEntry | null>(null)
  const [columns, setColumns] = useState<ColumnConfig[]>(() => loadColumns())
  const dragColKeyRef = useRef<string | null>(null)
  const resizeStateRef = useRef<{ key: string; startX: number; startW: number } | null>(null)
  const queryClient = useQueryClient()

  // Resize handlers
  useEffect(() => {
    const onMouseMove = (e: MouseEvent) => {
      const s = resizeStateRef.current
      if (!s) return
      const diff = e.clientX - s.startX
      const newWidth = Math.max(60, s.startW + diff)
      setColumns(prev => prev.map(c => c.key === s.key ? { ...c, width: newWidth } : c))
    }
    const onMouseUp = () => {
      if (resizeStateRef.current) {
        resizeStateRef.current = null
        setColumns(curr => { saveColumns(curr); return curr })
        document.body.style.cursor = ''
        document.body.style.userSelect = ''
      }
    }
    window.addEventListener('mousemove', onMouseMove)
    window.addEventListener('mouseup', onMouseUp)
    return () => {
      window.removeEventListener('mousemove', onMouseMove)
      window.removeEventListener('mouseup', onMouseUp)
    }
  }, [])

  const startResize = useCallback((key: string, e: React.MouseEvent) => {
    e.preventDefault()
    e.stopPropagation()
    const col = columns.find(c => c.key === key)
    if (!col) return
    resizeStateRef.current = { key, startX: e.clientX, startW: col.width }
    document.body.style.cursor = 'col-resize'
    document.body.style.userSelect = 'none'
  }, [columns])

  const handleDragStart = (e: React.DragEvent, key: string) => {
    dragColKeyRef.current = key
    e.dataTransfer.effectAllowed = 'move'
  }

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault()
    e.dataTransfer.dropEffect = 'move'
  }

  const handleDrop = (e: React.DragEvent, targetKey: string) => {
    e.preventDefault()
    const sourceKey = dragColKeyRef.current
    if (!sourceKey || sourceKey === targetKey) return
    setColumns(prev => {
      const srcIdx = prev.findIndex(c => c.key === sourceKey)
      const dstIdx = prev.findIndex(c => c.key === targetKey)
      if (srcIdx < 0 || dstIdx < 0) return prev
      const next = [...prev]
      const [moved] = next.splice(srcIdx, 1)
      next.splice(dstIdx, 0, moved)
      saveColumns(next)
      return next
    })
    dragColKeyRef.current = null
  }

  const resetColumns = () => {
    setColumns([...DEFAULT_COLUMNS])
    saveColumns([...DEFAULT_COLUMNS])
  }

  const dateFrom = format(dateRange.from, 'yyyy-MM-dd')
  const dateTo = format(dateRange.to, 'yyyy-MM-dd')

  const { data, isLoading, isError } = useQuery<JournalData>({
    queryKey: ['journal', dateFrom, dateTo, filterType, filterAccount, filterCategory],
    queryFn: () =>
      api
        .get('/journal', {
          params: {
            date_from: dateFrom,
            date_to: dateTo,
            ...(filterType !== 'all' ? { entry_type: filterType } : {}),
            ...(filterAccount ? { account: filterAccount } : {}),
            ...(filterCategory ? { category: filterCategory } : {}),
          },
        })
        .then((r) => {
          const d = r.data
          // Support both { items: [...] } and plain array responses
          if (Array.isArray(d)) return { items: d, total: d.length }
          return d
        }),
    staleTime: 30_000,
  })

  const { data: accounts = [] } = useQuery<{ id: number; name: string }[]>({
    queryKey: ['journal-accounts'],
    queryFn: () => api.get('/journal/accounts').then((r) => r.data),
  })

  const { data: categories = [] } = useQuery<{ key: string; name: string }[]>({
    queryKey: ['journal-categories'],
    queryFn: () => api.get('/journal/categories').then((r) => r.data),
  })

  const deleteMutation = useMutation({
    mutationFn: (id: number) => api.delete(`/journal/${id}`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['journal'] }),
  })

  const entries = Array.isArray(data) ? data : (data?.items || [])

  // Summary calculations
  const summary = useMemo(() => {
    let totalExpense = 0
    let totalIncome = 0
    for (const e of entries) {
      if (e.entry_type === 'expense') totalExpense += e.amount
      else if (e.entry_type === 'income') totalIncome += e.amount
    }
    return { totalExpense, totalIncome, balance: totalIncome - totalExpense }
  }, [entries])

  const handleEdit = (entry: JournalEntry) => {
    setEditEntry(entry)
    setModalOpen(true)
  }

  const categoryName = (key: string) => categories.find(c => c.key === key)?.name || key

  const renderCell = (entry: JournalEntry, col: ColumnConfig) => {
    switch (col.key) {
      case 'date':
        return entry.scheduled_date
          ? fmtDate(entry.scheduled_date)
          : entry.created_at ? fmtDate(entry.created_at.slice(0, 10)) : '—'
      case 'entry_type':
        return (
          <span className={clsx('inline-block px-2 py-0.5 rounded-full text-xs font-medium', TYPE_COLORS[entry.entry_type as EntryType])}>
            {TYPE_LABELS[entry.entry_type as EntryType]}
          </span>
        )
      case 'amount':
        return (
          <span className={clsx(
            'tabular-nums font-medium whitespace-nowrap',
            entry.entry_type === 'expense' ? 'text-red-600' : entry.entry_type === 'income' ? 'text-emerald-600' : 'text-blue-600'
          )}>
            {entry.entry_type === 'expense' ? '−' : '+'}{fmt(entry.amount)} ₽
          </span>
        )
      case 'nds_amount':
        return entry.nds_amount ? `${fmt(entry.nds_amount)} ₽` : '—'
      case 'account_name':
        return entry.account_name
      case 'category':
        return categoryName(entry.category)
      case 'counterparty':
        return entry.counterparty || '—'
      case 'description':
        return entry.description || '—'
      case 'recurring':
        return entry.is_recurring ? (
          <span className="inline-flex items-center gap-1 text-xs text-violet-600">
            <RefreshCw size={12} />
            {RECURRENCE_LABELS[entry.recurrence_rule || ''] || '—'}
          </span>
        ) : <span className="text-gray-300">—</span>
      case 'actions':
        return (
          <div className="flex items-center justify-center gap-1">
            <button onClick={() => handleEdit(entry)} className="p-1 text-gray-400 hover:text-blue-600 rounded" title="Редактировать">
              <Pencil size={14} />
            </button>
            <button onClick={() => handleDelete(entry.id)} className="p-1 text-gray-400 hover:text-red-600 rounded" title="Удалить">
              <Trash2 size={14} />
            </button>
          </div>
        )
      default:
        return '—'
    }
  }

  const handleDelete = (id: number) => {
    if (window.confirm('Удалить операцию?')) {
      deleteMutation.mutate(id)
    }
  }

  const handleCloseModal = () => {
    setModalOpen(false)
    setEditEntry(null)
  }

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between gap-4 mb-4 flex-wrap">
        <h1 className="text-xl font-bold text-gray-900">Журнал операций</h1>
        <div className="flex items-center gap-2">
          <button
            onClick={() => {
              setEditEntry(null)
              setModalOpen(true)
            }}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors"
          >
            <Plus size={16} />
            Добавить операцию
          </button>
          <button
            onClick={() => setUploadOpen(true)}
            className="flex items-center gap-1.5 px-3 py-1.5 border border-gray-200 bg-white text-sm text-gray-700 rounded-lg hover:bg-gray-50 transition-colors"
          >
            <Paperclip size={15} />
            Загрузить выписку
          </button>
          <button
            onClick={() => setReceiptsOpen(true)}
            className="flex items-center gap-1.5 px-3 py-1.5 border border-gray-200 bg-white text-sm text-gray-700 rounded-lg hover:bg-gray-50 transition-colors"
          >
            <Camera size={15} />
            Загрузить чеки
          </button>
        </div>
      </div>

      {/* Filters */}
      <div className="flex items-center gap-3 mb-4 flex-wrap">
        <DateRangePicker value={dateRange} onChange={setDateRange} />
        <select
          value={filterType}
          onChange={(e) => setFilterType(e.target.value)}
          className="border border-gray-200 rounded-lg px-3 py-1.5 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500/30"
        >
          <option value="all">Все типы</option>
          <option value="expense">Расход</option>
          <option value="income">Доход</option>
          <option value="transfer">Перевод</option>
        </select>
        <select
          value={filterAccount}
          onChange={(e) => setFilterAccount(e.target.value)}
          className="border border-gray-200 rounded-lg px-3 py-1.5 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500/30"
        >
          <option value="">Все счета</option>
          {accounts.map((a) => (
            <option key={a.id || a.name} value={a.name}>
              {a.name}
            </option>
          ))}
        </select>
        <select
          value={filterCategory}
          onChange={(e) => setFilterCategory(e.target.value)}
          className="border border-gray-200 rounded-lg px-3 py-1.5 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500/30"
        >
          <option value="">Все статьи</option>
          {categories.map((c) => (
            <option key={c.key} value={c.key}>
              {c.name}
            </option>
          ))}
        </select>
      </div>

      {/* Table */}
      <div className="border border-gray-200 bg-white rounded-xl">
        {isLoading && (
          <div className="flex items-center justify-center h-40 text-gray-400">Загрузка...</div>
        )}
        {isError && (
          <div className="flex items-center justify-center h-40 text-red-500">Ошибка загрузки</div>
        )}
        {data && (
          <>
            <div className="flex items-center justify-end px-3 py-2 border-b border-gray-100">
              <button
                onClick={resetColumns}
                className="text-xs text-gray-400 hover:text-gray-600 transition-colors"
                title="Вернуть колонки в исходный порядок и ширину"
              >
                ⟲ Сбросить колонки
              </button>
            </div>
            <div className="overflow-x-auto overflow-y-visible">
            <table className="text-sm" style={{ borderCollapse: 'separate', borderSpacing: 0, tableLayout: 'fixed', width: columns.reduce((s, c) => s + c.width, 0), minWidth: '100%' }}>
              <colgroup>
                {columns.map(c => <col key={c.key} style={{ width: c.width }} />)}
              </colgroup>
              <thead>
                <tr className="bg-gray-50 select-none">
                  {columns.map(col => (
                    <th
                      key={col.key}
                      draggable
                      onDragStart={(e) => handleDragStart(e, col.key)}
                      onDragOver={handleDragOver}
                      onDrop={(e) => handleDrop(e, col.key)}
                      className={clsx(
                        'relative px-3 py-3 font-medium text-gray-600 border-b cursor-move group',
                        col.align === 'right' && 'text-right',
                        col.align === 'center' && 'text-center',
                        col.align === 'left' && 'text-left',
                      )}
                      title="Перетащите для изменения порядка"
                    >
                      {col.label}
                      <span
                        onMouseDown={(e) => startResize(col.key, e)}
                        onDragStart={(e) => e.preventDefault()}
                        className="absolute right-0 top-0 bottom-0 w-1.5 cursor-col-resize group-hover:bg-blue-300 hover:bg-blue-500"
                        title="Потяните для изменения ширины"
                      />
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {entries.length === 0 && (
                  <tr>
                    <td colSpan={columns.length} className="text-center py-12 text-gray-400">
                      Нет операций за выбранный период
                    </td>
                  </tr>
                )}
                {entries.map((entry, i) => (
                  <tr
                    key={entry.id}
                    className={clsx(
                      'border-b border-gray-100 hover:bg-gray-50/50 transition-colors',
                      i % 2 !== 0 && 'bg-[#fafbfc]'
                    )}
                  >
                    {columns.map(col => (
                      <td
                        key={col.key}
                        className={clsx(
                          'px-3 py-2.5 border-b border-gray-100 overflow-hidden text-ellipsis whitespace-nowrap',
                          col.align === 'right' && 'text-right',
                          col.align === 'center' && 'text-center',
                          col.align === 'left' && 'text-left text-gray-700',
                        )}
                      >
                        {renderCell(entry, col)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
            </div>

            {/* Summary row */}
            {entries.length > 0 && (
              <div className="flex items-center gap-6 px-4 py-3 border-t border-gray-200 bg-gray-50 rounded-b-xl text-sm">
                <div className="flex items-center gap-1.5">
                  <span className="text-gray-500">Расходы:</span>
                  <span className="font-semibold text-red-600">−{fmt(summary.totalExpense)} ₽</span>
                </div>
                <div className="flex items-center gap-1.5">
                  <span className="text-gray-500">Доходы:</span>
                  <span className="font-semibold text-emerald-600">+{fmt(summary.totalIncome)} ₽</span>
                </div>
                <div className="flex items-center gap-1.5">
                  <span className="text-gray-500">Баланс:</span>
                  <span
                    className={clsx(
                      'font-semibold',
                      summary.balance >= 0 ? 'text-emerald-600' : 'text-red-600'
                    )}
                  >
                    {summary.balance >= 0 ? '+' : '−'}{fmt(Math.abs(summary.balance))} ₽
                  </span>
                </div>
              </div>
            )}
          </>
        )}
      </div>

      {/* Modals */}
      <JournalEntryModal open={modalOpen} onClose={handleCloseModal} editEntry={editEntry} />
      <StatementUploadModal open={uploadOpen} onClose={() => setUploadOpen(false)} />
      <ReceiptsUploadModal open={receiptsOpen} onClose={() => setReceiptsOpen(false)} />
    </div>
  )
}
