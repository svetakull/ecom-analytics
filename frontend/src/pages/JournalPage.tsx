import { useState, useMemo } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { format, subDays, startOfDay, startOfYear } from 'date-fns'
import DateRangePicker, { type DateRange } from '@/components/DateRangePicker'
import JournalEntryModal from '@/components/JournalEntryModal'
import StatementUploadModal from '@/components/StatementUploadModal'
import { api } from '@/api/client'
import { Plus, Paperclip, Pencil, Trash2, RefreshCw } from 'lucide-react'
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

export default function JournalPage() {
  const [dateRange, setDateRange] = useState<DateRange>(defaultRange)
  const [filterType, setFilterType] = useState<string>('all')
  const [filterAccount, setFilterAccount] = useState<string>('')
  const [filterCategory, setFilterCategory] = useState<string>('')
  const [modalOpen, setModalOpen] = useState(false)
  const [uploadOpen, setUploadOpen] = useState(false)
  const [editEntry, setEditEntry] = useState<JournalEntry | null>(null)
  const queryClient = useQueryClient()

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

  const entries = data?.items || []

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

  const handleDelete = (id: number) => {
    if (window.confirm('Удалить операцию?')) {
      deleteMutation.mutate(id)
    }
  }

  const handleCloseModal = () => {
    setModalOpen(false)
    setEditEntry(null)
  }

  // Find category name by key
  const categoryName = (key: string) => {
    const found = categories.find((c) => c.key === key)
    return found ? found.name : key
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
            <table className="w-full text-sm" style={{ borderCollapse: 'separate', borderSpacing: 0 }}>
              <thead>
                <tr className="bg-gray-50">
                  <th className="text-left px-4 py-3 font-medium text-gray-600 border-b">Дата</th>
                  <th className="text-left px-3 py-3 font-medium text-gray-600 border-b">Тип</th>
                  <th className="text-right px-3 py-3 font-medium text-gray-600 border-b">Сумма</th>
                  <th className="text-right px-3 py-3 font-medium text-gray-600 border-b">НДС</th>
                  <th className="text-left px-3 py-3 font-medium text-gray-600 border-b">Счёт</th>
                  <th className="text-left px-3 py-3 font-medium text-gray-600 border-b">Статья</th>
                  <th className="text-left px-3 py-3 font-medium text-gray-600 border-b">Контрагент</th>
                  <th className="text-left px-3 py-3 font-medium text-gray-600 border-b">Описание</th>
                  <th className="text-center px-3 py-3 font-medium text-gray-600 border-b">Повтор</th>
                  <th className="text-center px-3 py-3 font-medium text-gray-600 border-b w-20">Действия</th>
                </tr>
              </thead>
              <tbody>
                {entries.length === 0 && (
                  <tr>
                    <td colSpan={10} className="text-center py-12 text-gray-400">
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
                    <td className="px-4 py-2.5 text-gray-700 whitespace-nowrap">
                      {entry.scheduled_date
                        ? fmtDate(entry.scheduled_date)
                        : entry.created_at
                          ? fmtDate(entry.created_at.slice(0, 10))
                          : '—'}
                    </td>
                    <td className="px-3 py-2.5">
                      <span
                        className={clsx(
                          'inline-block px-2 py-0.5 rounded-full text-xs font-medium',
                          TYPE_COLORS[entry.entry_type]
                        )}
                      >
                        {TYPE_LABELS[entry.entry_type]}
                      </span>
                    </td>
                    <td
                      className={clsx(
                        'px-3 py-2.5 text-right tabular-nums font-medium whitespace-nowrap',
                        entry.entry_type === 'expense'
                          ? 'text-red-600'
                          : entry.entry_type === 'income'
                            ? 'text-emerald-600'
                            : 'text-blue-600'
                      )}
                    >
                      {entry.entry_type === 'expense' ? '−' : '+'}{fmt(entry.amount)} ₽
                    </td>
                    <td className="px-3 py-2.5 text-right tabular-nums text-gray-500 whitespace-nowrap">
                      {entry.nds_amount ? `${fmt(entry.nds_amount)} ₽` : '—'}
                    </td>
                    <td className="px-3 py-2.5 text-gray-700 whitespace-nowrap">{entry.account_name}</td>
                    <td className="px-3 py-2.5 text-gray-700 whitespace-nowrap">
                      {categoryName(entry.category)}
                    </td>
                    <td className="px-3 py-2.5 text-gray-700 truncate max-w-[150px]">
                      {entry.counterparty || '—'}
                    </td>
                    <td className="px-3 py-2.5 text-gray-500 truncate max-w-[180px]">
                      {entry.description || '—'}
                    </td>
                    <td className="px-3 py-2.5 text-center">
                      {entry.is_recurring ? (
                        <span className="inline-flex items-center gap-1 text-xs text-violet-600">
                          <RefreshCw size={12} />
                          {RECURRENCE_LABELS[entry.recurrence_rule || ''] || '—'}
                        </span>
                      ) : (
                        <span className="text-gray-300">—</span>
                      )}
                    </td>
                    <td className="px-3 py-2.5 text-center">
                      <div className="flex items-center justify-center gap-1">
                        <button
                          onClick={() => handleEdit(entry)}
                          className="p-1 text-gray-400 hover:text-blue-600 transition-colors rounded"
                          title="Редактировать"
                        >
                          <Pencil size={14} />
                        </button>
                        <button
                          onClick={() => handleDelete(entry.id)}
                          className="p-1 text-gray-400 hover:text-red-600 transition-colors rounded"
                          title="Удалить"
                        >
                          <Trash2 size={14} />
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>

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
    </div>
  )
}
