import { useState, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/api/client'
import { X, Info } from 'lucide-react'
import clsx from 'clsx'

type EntryType = 'expense' | 'income' | 'transfer'
type RecurrenceRule = 'monthly' | 'weekly'

interface JournalEntryForm {
  entry_type: EntryType
  amount: string
  nds_amount: string
  is_recurring: boolean
  recurrence_rule: RecurrenceRule
  recurrence_day: string
  scheduled_date: string
  backfill_from: string
  account_name: string
  category: string
  counterparty: string
  description: string
  is_distributed: boolean
  is_official: boolean
}

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
}

interface Props {
  open: boolean
  onClose: () => void
  editEntry?: JournalEntry | null
}

const TABS: { key: EntryType; label: string }[] = [
  { key: 'expense', label: 'Расход' },
  { key: 'income', label: 'Доход' },
  { key: 'transfer', label: 'Перевод' },
]

function emptyForm(): JournalEntryForm {
  return {
    entry_type: 'expense',
    amount: '',
    nds_amount: '',
    is_recurring: false,
    recurrence_rule: 'monthly',
    recurrence_day: '',
    scheduled_date: '',
    backfill_from: '',
    account_name: '',
    category: '',
    counterparty: '',
    description: '',
    is_distributed: false,
    is_official: false,
  }
}

export default function JournalEntryModal({ open, onClose, editEntry }: Props) {
  const [form, setForm] = useState<JournalEntryForm>(emptyForm())
  const [newAccountMode, setNewAccountMode] = useState(false)
  const [counterpartySuggestions, setCounterpartySuggestions] = useState<string[]>([])
  const [showSuggestions, setShowSuggestions] = useState(false)
  const queryClient = useQueryClient()

  // Fetch accounts (backend возвращает string[])
  const { data: accounts = [] } = useQuery<string[]>({
    queryKey: ['journal-accounts'],
    queryFn: () => api.get('/journal/accounts').then((r) => {
      const d = r.data
      if (Array.isArray(d)) {
        // поддержка обеих форм: string[] или {name}[]
        return d.map((item: any) => typeof item === 'string' ? item : (item?.name || '')).filter(Boolean)
      }
      return []
    }),
    enabled: open,
  })

  // Fetch categories
  const { data: categories = [] } = useQuery<{ key: string; name: string; section?: string }[]>({
    queryKey: ['journal-categories'],
    queryFn: () => api.get('/journal/categories').then((r) => r.data),
    enabled: open,
  })

  // Группировка по разделам для optgroup
  const SECTION_LABELS: Record<string, string> = {
    income: 'Доходы',
    expenses: 'Расходы',
    taxes: 'Налоги',
    advances: 'Авансы (закупка)',
    credits: 'Кредиты и удержания',
    dividends: 'Дивиденды',
  }
  const SECTION_ORDER = ['income', 'expenses', 'taxes', 'advances', 'credits', 'dividends']
  const groupedCategories = SECTION_ORDER
    .map(section => ({
      section,
      label: SECTION_LABELS[section] || section,
      items: categories.filter(c => c.section === section),
    }))
    .filter(g => g.items.length > 0)

  // Reset form when opening/closing or editing
  useEffect(() => {
    if (open) {
      if (editEntry) {
        setForm({
          entry_type: editEntry.entry_type,
          amount: String(editEntry.amount),
          nds_amount: String(editEntry.nds_amount || ''),
          is_recurring: editEntry.is_recurring,
          recurrence_rule: (editEntry.recurrence_rule as RecurrenceRule) || 'monthly',
          recurrence_day: editEntry.recurrence_day ? String(editEntry.recurrence_day) : '',
          scheduled_date: editEntry.scheduled_date || '',
          backfill_from: editEntry.backfill_from || '',
          account_name: editEntry.account_name,
          category: editEntry.category || '',
          counterparty: editEntry.counterparty || '',
          description: editEntry.description || '',
          is_distributed: editEntry.is_distributed,
          is_official: editEntry.is_official,
        })
      } else {
        setForm(emptyForm())
      }
      setNewAccountMode(false)
    }
  }, [open, editEntry])

  // Counterparty autocomplete
  useEffect(() => {
    if (form.counterparty.length < 2) {
      setCounterpartySuggestions([])
      return
    }
    const timeout = setTimeout(() => {
      api
        .get('/journal', { params: { limit: 100 } })
        .then((r) => {
          const entries: JournalEntry[] = r.data.items || r.data || []
          const unique = [...new Set(entries.map((e) => e.counterparty).filter(Boolean))]
          const filtered = unique.filter((c) =>
            c.toLowerCase().includes(form.counterparty.toLowerCase())
          )
          setCounterpartySuggestions(filtered.slice(0, 5))
        })
        .catch(() => setCounterpartySuggestions([]))
    }, 300)
    return () => clearTimeout(timeout)
  }, [form.counterparty])

  const isEditing = !!(editEntry && editEntry.id)
  const createMutation = useMutation({
    mutationFn: (data: Record<string, unknown>) =>
      isEditing
        ? api.put(`/journal/${editEntry!.id}`, data)
        : api.post('/journal', data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['journal'] })
      onClose()
    },
  })

  const handleSubmit = () => {
    const payload: Record<string, unknown> = {
      entry_type: form.entry_type,
      amount: parseFloat(form.amount.replace(/\s/g, '').replace(',', '.')) || 0,
      nds_amount: parseFloat(form.nds_amount.replace(/\s/g, '').replace(',', '.')) || 0,
      is_recurring: form.is_recurring,
      account_name: form.account_name,
      category: form.category,
      counterparty: form.counterparty,
      description: form.description,
      is_distributed: form.is_distributed,
      is_official: form.is_official,
    }
    if (form.is_recurring) {
      payload.recurrence_rule = form.recurrence_rule
      payload.recurrence_day = parseInt(form.recurrence_day) || null
    }
    if (form.scheduled_date) payload.scheduled_date = form.scheduled_date
    if (form.backfill_from) payload.backfill_from = form.backfill_from

    createMutation.mutate(payload)
  }

  const updateField = <K extends keyof JournalEntryForm>(key: K, value: JournalEntryForm[K]) =>
    setForm((prev) => ({ ...prev, [key]: value }))

  if (!open) return null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-lg max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="flex items-center justify-between px-6 pt-5 pb-3">
          <h2 className="text-lg font-bold text-gray-900">
            {isEditing ? 'Редактировать операцию' : (editEntry ? 'Копия операции' : 'Новая операция')}
          </h2>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600">
            <X size={20} />
          </button>
        </div>

        {/* Tabs */}
        <div className="flex gap-2 px-6 mb-4">
          {TABS.map((tab) => (
            <button
              key={tab.key}
              onClick={() => updateField('entry_type', tab.key)}
              className={clsx(
                'flex-1 py-2 text-sm font-medium rounded-lg border transition-colors',
                form.entry_type === tab.key
                  ? tab.key === 'expense'
                    ? 'border-red-400 bg-red-50 text-red-700'
                    : tab.key === 'income'
                      ? 'border-emerald-400 bg-emerald-50 text-emerald-700'
                      : 'border-blue-400 bg-blue-50 text-blue-700'
                  : 'border-gray-200 bg-white text-gray-600 hover:bg-gray-50'
              )}
            >
              {tab.label}
            </button>
          ))}
        </div>

        <div className="px-6 pb-6 space-y-4">
          {/* Amount row */}
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Сумма ₽ (с учётом НДС)
              </label>
              <input
                type="text"
                value={form.amount}
                onChange={(e) => updateField('amount', e.target.value)}
                placeholder="0"
                className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/30 focus:border-blue-400"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Сумма НДС ₽
              </label>
              <input
                type="text"
                value={form.nds_amount}
                onChange={(e) => updateField('nds_amount', e.target.value)}
                placeholder="0"
                className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/30 focus:border-blue-400"
              />
            </div>
          </div>

          {/* Operation type */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Тип операции</label>
            <div className="flex items-center gap-4">
              <label className="flex items-center gap-2 text-sm cursor-pointer">
                <input
                  type="radio"
                  checked={!form.is_recurring}
                  onChange={() => updateField('is_recurring', false)}
                  className="accent-blue-600"
                />
                Разовая
              </label>
              <label className="flex items-center gap-2 text-sm cursor-pointer">
                <input
                  type="radio"
                  checked={form.is_recurring}
                  onChange={() => updateField('is_recurring', true)}
                  className="accent-blue-600"
                />
                Регулярная
              </label>
            </div>
          </div>

          {/* Recurring options */}
          {form.is_recurring && (
            <div className="bg-gray-50 rounded-lg p-3 space-y-3">
              <div className="flex items-center gap-4">
                <label className="flex items-center gap-2 text-sm cursor-pointer">
                  <input
                    type="radio"
                    checked={form.recurrence_rule === 'monthly'}
                    onChange={() => updateField('recurrence_rule', 'monthly')}
                    className="accent-blue-600"
                  />
                  Каждый месяц
                </label>
                <label className="flex items-center gap-2 text-sm cursor-pointer">
                  <input
                    type="radio"
                    checked={form.recurrence_rule === 'weekly'}
                    onChange={() => updateField('recurrence_rule', 'weekly')}
                    className="accent-blue-600"
                  />
                  Каждую неделю
                </label>
              </div>
              {form.recurrence_rule === 'monthly' && (
                <div>
                  <label className="block text-sm text-gray-600 mb-1">Число месяца (1–28)</label>
                  <input
                    type="number"
                    min={1}
                    max={28}
                    value={form.recurrence_day}
                    onChange={(e) => updateField('recurrence_day', e.target.value)}
                    placeholder="15"
                    className="w-24 border border-gray-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/30 focus:border-blue-400"
                  />
                </div>
              )}
              <div>
                <label className="block text-sm text-gray-600 mb-1">
                  Создать за прошлый период (опционально)
                </label>
                <input
                  type="date"
                  value={form.backfill_from}
                  onChange={(e) => updateField('backfill_from', e.target.value)}
                  className="border border-gray-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/30 focus:border-blue-400"
                />
              </div>
            </div>
          )}

          {/* Scheduled date for one-time */}
          {!form.is_recurring && (
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Дата операции</label>
              <input
                type="date"
                value={form.scheduled_date}
                onChange={(e) => updateField('scheduled_date', e.target.value)}
                className="border border-gray-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/30 focus:border-blue-400"
              />
            </div>
          )}

          {/* Account */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Счёт</label>
            {newAccountMode ? (
              <div className="flex gap-2">
                <input
                  type="text"
                  value={form.account_name}
                  onChange={(e) => updateField('account_name', e.target.value)}
                  placeholder="Название нового счёта"
                  className="flex-1 border border-gray-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/30 focus:border-blue-400"
                />
                <button
                  onClick={() => setNewAccountMode(false)}
                  className="text-sm text-gray-500 hover:text-gray-700 px-2"
                >
                  Отмена
                </button>
              </div>
            ) : (
              <div className="flex gap-2">
                <select
                  value={form.account_name}
                  onChange={(e) => updateField('account_name', e.target.value)}
                  className="flex-1 border border-gray-200 rounded-lg px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500/30 focus:border-blue-400"
                >
                  <option value="">Выберите счёт</option>
                  {accounts.map((name) => (
                    <option key={name} value={name}>
                      {name}
                    </option>
                  ))}
                </select>
                <button
                  onClick={() => {
                    setNewAccountMode(true)
                    updateField('account_name', '')
                  }}
                  className="text-sm text-blue-600 hover:text-blue-700 px-2 whitespace-nowrap"
                >
                  + Новый
                </button>
              </div>
            )}
          </div>

          {/* Category */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Статья</label>
            <select
              value={form.category}
              onChange={(e) => updateField('category', e.target.value)}
              className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500/30 focus:border-blue-400"
            >
              <option value="">Выберите статью</option>
              {groupedCategories.map((group) => (
                <optgroup key={group.section} label={group.label}>
                  {group.items.map((c) => (
                    <option key={c.key} value={c.key}>
                      {c.name}
                    </option>
                  ))}
                </optgroup>
              ))}
            </select>
          </div>

          {/* Counterparty */}
          <div className="relative">
            <label className="block text-sm font-medium text-gray-700 mb-1">Контрагент</label>
            <input
              type="text"
              value={form.counterparty}
              onChange={(e) => {
                updateField('counterparty', e.target.value)
                setShowSuggestions(true)
              }}
              onBlur={() => setTimeout(() => setShowSuggestions(false), 200)}
              placeholder="ООО Ромашка"
              className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/30 focus:border-blue-400"
            />
            {showSuggestions && counterpartySuggestions.length > 0 && (
              <div className="absolute z-10 top-full mt-1 w-full bg-white border border-gray-200 rounded-lg shadow-lg max-h-40 overflow-y-auto">
                {counterpartySuggestions.map((s) => (
                  <button
                    key={s}
                    onMouseDown={() => {
                      updateField('counterparty', s)
                      setShowSuggestions(false)
                    }}
                    className="w-full text-left px-3 py-2 text-sm hover:bg-blue-50 text-gray-700"
                  >
                    {s}
                  </button>
                ))}
              </div>
            )}
          </div>

          {/* Description */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Описание</label>
            <textarea
              value={form.description}
              onChange={(e) => updateField('description', e.target.value)}
              rows={2}
              placeholder="Комментарий к операции..."
              className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm resize-none focus:outline-none focus:ring-2 focus:ring-blue-500/30 focus:border-blue-400"
            />
          </div>

          {/* Checkboxes */}
          <div className="space-y-2">
            <label className="flex items-center gap-2 text-sm cursor-pointer">
              <input
                type="checkbox"
                checked={form.is_distributed}
                onChange={(e) => updateField('is_distributed', e.target.checked)}
                className="accent-blue-600 w-4 h-4"
              />
              Распределить расход
            </label>
            <label className="flex items-center gap-2 text-sm cursor-pointer">
              <input
                type="checkbox"
                checked={form.is_official}
                onChange={(e) => updateField('is_official', e.target.checked)}
                className="accent-blue-600 w-4 h-4"
              />
              Официальный расход
              <span className="text-gray-400 cursor-help" title="Расход учитывается в официальной отчётности">
                <Info size={14} />
              </span>
            </label>
          </div>

          {/* Error */}
          {createMutation.isError && (
            <div className="text-sm text-red-600 bg-red-50 rounded-lg px-3 py-2">
              Ошибка сохранения. Проверьте данные и попробуйте снова.
            </div>
          )}

          {/* Buttons */}
          <div className="flex gap-3 pt-2">
            <button
              onClick={handleSubmit}
              disabled={createMutation.isPending || !form.amount || !form.account_name}
              className="flex-1 bg-blue-600 text-white font-medium py-2.5 rounded-lg hover:bg-blue-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors text-sm"
            >
              {createMutation.isPending
                ? 'Сохранение...'
                : editEntry
                  ? 'Сохранить'
                  : 'Создать операцию'}
            </button>
            <button
              onClick={onClose}
              className="px-6 py-2.5 border border-gray-200 rounded-lg text-sm text-gray-600 hover:bg-gray-50 transition-colors"
            >
              Отмена
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
