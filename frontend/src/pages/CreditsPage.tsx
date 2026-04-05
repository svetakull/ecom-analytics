/**
 * Кредиты — учёт тела, процентов, платежей и остатка.
 */
import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/api/client'
import { Plus, Pencil, Trash2, ChevronRight, ChevronDown } from 'lucide-react'
import clsx from 'clsx'

interface Credit {
  id: number
  name: string
  bank: string | null
  principal: number
  interest_rate: number | null
  start_date: string | null
  end_date: string | null
  monthly_payment: number | null
  note: string | null
  is_active: boolean
  body_paid: number
  interest_paid: number
  total_paid: number
  payments_count: number
  balance: number
}

interface Payment {
  id: number
  credit_id: number
  payment_date: string
  body_amount: number
  interest_amount: number
  total_amount: number
  balance_after: number | null
  balance_calc?: number
  note: string | null
}

const fmt = (n: number) =>
  n.toLocaleString('ru-RU', { minimumFractionDigits: 0, maximumFractionDigits: 0 })

const fmtDate = (s: string | null) => {
  if (!s) return '—'
  const d = new Date(s + 'T00:00:00')
  return d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: 'numeric' })
}

export default function CreditsPage() {
  const qc = useQueryClient()
  const [expanded, setExpanded] = useState<Set<number>>(new Set())
  const [editingCredit, setEditingCredit] = useState<Credit | null>(null)
  const [creditModalOpen, setCreditModalOpen] = useState(false)
  const [paymentModal, setPaymentModal] = useState<{ creditId: number; payment?: Payment } | null>(null)

  const { data: credits = [], isLoading } = useQuery<Credit[]>({
    queryKey: ['credits'],
    queryFn: () => api.get('/credits').then(r => r.data),
  })

  const deleteMutation = useMutation({
    mutationFn: (id: number) => api.delete(`/credits/${id}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['credits'] }),
  })

  const toggle = (id: number) => {
    setExpanded(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  const handleDeleteCredit = (c: Credit) => {
    if (window.confirm(`Удалить кредит «${c.name}» вместе с платежами?`)) {
      deleteMutation.mutate(c.id)
    }
  }

  const totalPrincipal = credits.reduce((s, c) => s + c.principal, 0)
  const totalBalance = credits.reduce((s, c) => s + c.balance, 0)
  const totalPaid = credits.reduce((s, c) => s + c.total_paid, 0)
  const totalInterest = credits.reduce((s, c) => s + c.interest_paid, 0)

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Кредиты</h1>
          <p className="text-xs text-gray-500 mt-0.5">Учёт кредитов: тело, проценты, платежи, остаток</p>
        </div>
        <button
          onClick={() => { setEditingCredit(null); setCreditModalOpen(true) }}
          className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors"
        >
          <Plus size={16} /> Новый кредит
        </button>
      </div>

      {/* Сводка */}
      {credits.length > 0 && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
          <SummaryCard label="Всего кредитов" value={String(credits.length)} sub={`${credits.filter(c => c.is_active).length} активных`} />
          <SummaryCard label="Тело всех кредитов" value={fmt(totalPrincipal) + ' ₽'} />
          <SummaryCard label="Остаток" value={fmt(totalBalance) + ' ₽'} color="text-red-600" />
          <SummaryCard label="Выплачено" value={fmt(totalPaid) + ' ₽'} sub={`в т.ч. % ${fmt(totalInterest)} ₽`} color="text-emerald-600" />
        </div>
      )}

      {isLoading && <div className="text-gray-400 text-sm">Загрузка...</div>}

      {credits.length > 0 && <PeriodSummary />}

      {!isLoading && credits.length === 0 && (
        <div className="bg-white border border-dashed border-gray-300 rounded-xl px-4 py-12 text-center text-gray-400 text-sm">
          Нет кредитов. Нажми «+ Новый кредит» чтобы добавить.
        </div>
      )}

      <div className="space-y-3">
        {credits.map(c => {
          const isOpen = expanded.has(c.id)
          const pct = c.principal > 0 ? (c.body_paid / c.principal * 100) : 0
          return (
            <div key={c.id} className={clsx('bg-white border rounded-xl', c.is_active ? 'border-gray-200' : 'border-gray-100 opacity-60')}>
              <div className="flex items-center gap-3 px-4 py-3">
                <button onClick={() => toggle(c.id)} className="text-gray-400 hover:text-gray-700">
                  {isOpen ? <ChevronDown size={18} /> : <ChevronRight size={18} />}
                </button>
                <div className="flex-1">
                  <div className="flex items-center gap-2 flex-wrap">
                    <span className="font-semibold text-gray-900">{c.name}</span>
                    {c.bank && <span className="text-xs text-gray-500">· {c.bank}</span>}
                    {c.interest_rate != null && <span className="text-xs text-gray-500">· {c.interest_rate}% годовых</span>}
                    {!c.is_active && <span className="text-[10px] px-1.5 py-0.5 bg-gray-100 text-gray-500 rounded">закрыт</span>}
                  </div>
                  <div className="text-xs text-gray-400 mt-0.5">
                    {fmtDate(c.start_date)} — {fmtDate(c.end_date)} · {c.payments_count} платежей
                  </div>
                </div>
                <div className="flex items-center gap-6 text-sm">
                  <div className="text-right">
                    <div className="text-[10px] text-gray-400 uppercase tracking-wide">Тело</div>
                    <div className="font-semibold text-gray-800">{fmt(c.principal)} ₽</div>
                  </div>
                  <div className="text-right">
                    <div className="text-[10px] text-gray-400 uppercase tracking-wide">Выплачено</div>
                    <div className="font-semibold text-emerald-600">{fmt(c.body_paid)} ₽</div>
                    <div className="text-[10px] text-gray-400">{pct.toFixed(0)}%</div>
                  </div>
                  <div className="text-right">
                    <div className="text-[10px] text-gray-400 uppercase tracking-wide">Остаток</div>
                    <div className="font-semibold text-red-600">{fmt(c.balance)} ₽</div>
                  </div>
                  <div className="text-right">
                    <div className="text-[10px] text-gray-400 uppercase tracking-wide">Проценты</div>
                    <div className="font-semibold text-gray-700">{fmt(c.interest_paid)} ₽</div>
                  </div>
                </div>
                <div className="flex items-center gap-1 ml-2">
                  <button onClick={() => { setEditingCredit(c); setCreditModalOpen(true) }}
                    className="p-1.5 text-gray-400 hover:text-blue-600 rounded" title="Редактировать">
                    <Pencil size={14} />
                  </button>
                  <button onClick={() => handleDeleteCredit(c)}
                    className="p-1.5 text-gray-400 hover:text-red-600 rounded" title="Удалить">
                    <Trash2 size={14} />
                  </button>
                </div>
              </div>
              {isOpen && (
                <PaymentsTable
                  creditId={c.id}
                  principal={c.principal}
                  onAddPayment={() => setPaymentModal({ creditId: c.id })}
                  onEditPayment={(p) => setPaymentModal({ creditId: c.id, payment: p })}
                />
              )}
            </div>
          )
        })}
      </div>

      {creditModalOpen && (
        <CreditModal
          credit={editingCredit}
          onClose={() => setCreditModalOpen(false)}
        />
      )}
      {paymentModal && (
        <PaymentModal
          creditId={paymentModal.creditId}
          payment={paymentModal.payment}
          onClose={() => setPaymentModal(null)}
        />
      )}
    </div>
  )
}

interface PeriodRow {
  period: string
  sum_body: number
  sum_interest: number
  sum_total: number
  credits: { credit_id: number; credit_name: string; body: number; interest: number; total: number; count: number }[]
}

function PeriodSummary() {
  const [granularity, setGranularity] = useState<'month' | 'week'>('month')
  const [showDetails, setShowDetails] = useState(false)
  const { data: rows = [], isLoading } = useQuery<PeriodRow[]>({
    queryKey: ['credits-summary', granularity],
    queryFn: () => api.get('/credits/summary-by-period', { params: { period: granularity } }).then(r => r.data),
  })

  const fmtPeriod = (p: string) => {
    const d = new Date(p + 'T00:00:00')
    if (granularity === 'month') {
      return d.toLocaleDateString('ru-RU', { month: 'long', year: 'numeric' })
    }
    const end = new Date(d); end.setDate(end.getDate() + 6)
    const f = (dt: Date) => `${dt.getDate().toString().padStart(2,'0')}.${(dt.getMonth()+1).toString().padStart(2,'0')}`
    return `${f(d)}–${f(end)}`
  }

  const grandBody = rows.reduce((s, r) => s + r.sum_body, 0)
  const grandInterest = rows.reduce((s, r) => s + r.sum_interest, 0)
  const grandTotal = rows.reduce((s, r) => s + r.sum_total, 0)

  return (
    <div className="bg-white border border-gray-200 rounded-xl overflow-hidden">
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-100 bg-gray-50">
        <div className="text-sm font-semibold text-gray-800">Сводная по периодам</div>
        <div className="flex items-center gap-3">
          <label className="flex items-center gap-1 text-xs text-gray-600">
            <input type="checkbox" checked={showDetails} onChange={(e) => setShowDetails(e.target.checked)} />
            Показать по кредитам
          </label>
          <select
            value={granularity}
            onChange={(e) => setGranularity(e.target.value as 'month' | 'week')}
            className="border border-gray-200 rounded px-2 py-1 text-xs bg-white focus:outline-none focus:ring-2 focus:ring-blue-500/30"
          >
            <option value="month">По месяцам</option>
            <option value="week">По неделям</option>
          </select>
        </div>
      </div>
      {isLoading ? (
        <div className="px-4 py-6 text-sm text-gray-400">Загрузка...</div>
      ) : rows.length === 0 ? (
        <div className="px-4 py-6 text-sm text-gray-400 text-center">Нет платежей</div>
      ) : (
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 border-b border-gray-100 text-gray-500">
              <th className="text-left px-4 py-2 font-medium text-xs">Период</th>
              <th className="text-right px-3 py-2 font-medium text-xs">Тело</th>
              <th className="text-right px-3 py-2 font-medium text-xs">Проценты</th>
              <th className="text-right px-3 py-2 font-medium text-xs">Платёж итого</th>
              <th className="text-right px-3 py-2 font-medium text-xs">Кол-во</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(r => (
              <>
                <tr key={r.period} className="border-b border-gray-50 bg-white hover:bg-gray-50/50">
                  <td className="px-4 py-2 font-medium text-gray-800">{fmtPeriod(r.period)}</td>
                  <td className="text-right px-3 py-2 tabular-nums">{fmt(r.sum_body)}</td>
                  <td className="text-right px-3 py-2 tabular-nums text-gray-500">{fmt(r.sum_interest)}</td>
                  <td className="text-right px-3 py-2 tabular-nums font-semibold">{fmt(r.sum_total)}</td>
                  <td className="text-right px-3 py-2 tabular-nums text-gray-400 text-xs">
                    {r.credits.reduce((s, c) => s + c.count, 0)}
                  </td>
                </tr>
                {showDetails && r.credits.map(c => (
                  <tr key={`${r.period}-${c.credit_id}`} className="border-b border-gray-50 bg-gray-50/30">
                    <td className="pl-10 pr-4 py-1.5 text-xs text-gray-500 truncate max-w-[300px]" title={c.credit_name}>
                      {c.credit_name}
                    </td>
                    <td className="text-right px-3 py-1.5 tabular-nums text-xs">{fmt(c.body)}</td>
                    <td className="text-right px-3 py-1.5 tabular-nums text-xs text-gray-500">{fmt(c.interest)}</td>
                    <td className="text-right px-3 py-1.5 tabular-nums text-xs">{fmt(c.total)}</td>
                    <td className="text-right px-3 py-1.5 tabular-nums text-xs text-gray-400">{c.count}</td>
                  </tr>
                ))}
              </>
            ))}
            <tr className="bg-indigo-50/50 border-t-2 border-t-indigo-100">
              <td className="px-4 py-2 font-bold text-gray-900">ИТОГО</td>
              <td className="text-right px-3 py-2 tabular-nums font-bold">{fmt(grandBody)}</td>
              <td className="text-right px-3 py-2 tabular-nums font-bold text-gray-700">{fmt(grandInterest)}</td>
              <td className="text-right px-3 py-2 tabular-nums font-bold">{fmt(grandTotal)}</td>
              <td></td>
            </tr>
          </tbody>
        </table>
      )}
    </div>
  )
}

function SummaryCard({ label, value, sub, color = 'text-gray-800' }: { label: string; value: string; sub?: string; color?: string }) {
  return (
    <div className="bg-white border border-gray-200 rounded-xl px-4 py-3">
      <div className="text-[10px] text-gray-400 uppercase tracking-wide">{label}</div>
      <div className={clsx('text-lg font-bold tabular-nums mt-0.5', color)}>{value}</div>
      {sub && <div className="text-[10px] text-gray-400 mt-0.5">{sub}</div>}
    </div>
  )
}

function PaymentsTable({ creditId, principal, onAddPayment, onEditPayment }:
  { creditId: number; principal: number; onAddPayment: () => void; onEditPayment: (p: Payment) => void }) {
  const qc = useQueryClient()
  const { data: payments = [], isLoading } = useQuery<Payment[]>({
    queryKey: ['credit-payments', creditId],
    queryFn: () => api.get(`/credits/${creditId}/payments`).then(r => r.data),
  })
  const deleteMut = useMutation({
    mutationFn: (id: number) => api.delete(`/credits/payments/${id}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['credit-payments', creditId] })
      qc.invalidateQueries({ queryKey: ['credits'] })
    },
  })
  return (
    <div className="border-t border-gray-100">
      <div className="flex items-center justify-between px-4 py-2 bg-gray-50">
        <div className="text-xs text-gray-500">Платежи</div>
        <div className="flex items-center gap-3">
          <AutoImportButton creditId={creditId} />
          <button onClick={onAddPayment} className="text-xs text-blue-600 hover:text-blue-700 font-medium">
            + Добавить платёж
          </button>
        </div>
      </div>
      {isLoading ? (
        <div className="text-xs text-gray-400 px-4 py-3">Загрузка...</div>
      ) : payments.length === 0 ? (
        <div className="text-xs text-gray-400 px-4 py-4 text-center">Нет платежей</div>
      ) : (
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 border-b border-gray-100 text-gray-500">
              <th className="text-left px-4 py-1.5 font-medium text-xs">Дата</th>
              <th className="text-right px-3 py-1.5 font-medium text-xs">Тело</th>
              <th className="text-right px-3 py-1.5 font-medium text-xs">Проценты</th>
              <th className="text-right px-3 py-1.5 font-medium text-xs">Платёж</th>
              <th className="text-right px-3 py-1.5 font-medium text-xs">Остаток</th>
              <th className="text-left px-3 py-1.5 font-medium text-xs">Примечание</th>
              <th className="w-16"></th>
            </tr>
          </thead>
          <tbody>
            {payments.map(p => (
              <tr key={p.id} className="border-b border-gray-50 hover:bg-gray-50/50">
                <td className="px-4 py-1.5 whitespace-nowrap">{fmtDate(p.payment_date)}</td>
                <td className="text-right px-3 py-1.5 tabular-nums">{fmt(p.body_amount)}</td>
                <td className="text-right px-3 py-1.5 tabular-nums text-gray-500">{fmt(p.interest_amount)}</td>
                <td className="text-right px-3 py-1.5 tabular-nums font-semibold">{fmt(p.total_amount)}</td>
                <td className="text-right px-3 py-1.5 tabular-nums text-red-600">{fmt(p.balance_calc ?? principal - p.body_amount)}</td>
                <td className="px-3 py-1.5 text-xs text-gray-500 max-w-[280px] truncate" title={p.note || ''}>{p.note || '—'}</td>
                <td className="px-3 py-1.5 text-right">
                  <button onClick={() => onEditPayment(p)} className="p-1 text-gray-400 hover:text-blue-600 rounded" title="Редактировать">
                    <Pencil size={12} />
                  </button>
                  <button onClick={() => { if (window.confirm('Удалить платёж?')) deleteMut.mutate(p.id) }}
                    className="p-1 text-gray-400 hover:text-red-600 rounded" title="Удалить">
                    <Trash2 size={12} />
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

function CreditModal({ credit, onClose }: { credit: Credit | null; onClose: () => void }) {
  const qc = useQueryClient()
  const [form, setForm] = useState({
    name: credit?.name || '',
    bank: credit?.bank || '',
    principal: credit?.principal ? String(credit.principal) : '',
    interest_rate: credit?.interest_rate != null ? String(credit.interest_rate) : '',
    start_date: credit?.start_date || '',
    end_date: credit?.end_date || '',
    monthly_payment: credit?.monthly_payment != null ? String(credit.monthly_payment) : '',
    note: credit?.note || '',
    is_active: credit?.is_active ?? true,
  })

  const saveMut = useMutation({
    mutationFn: (data: any) => credit
      ? api.patch(`/credits/${credit.id}`, data)
      : api.post('/credits', data),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['credits'] }); onClose() },
  })

  const handleSave = () => {
    const payload: any = {
      name: form.name,
      bank: form.bank || null,
      principal: parseFloat(form.principal) || 0,
      interest_rate: form.interest_rate ? parseFloat(form.interest_rate) : null,
      start_date: form.start_date || null,
      end_date: form.end_date || null,
      monthly_payment: form.monthly_payment ? parseFloat(form.monthly_payment) : null,
      note: form.note || null,
      is_active: form.is_active,
    }
    saveMut.mutate(payload)
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40" onClick={onClose}>
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-lg mx-4 p-6" onClick={e => e.stopPropagation()}>
        <h2 className="text-lg font-bold mb-4">{credit ? 'Редактировать кредит' : 'Новый кредит'}</h2>
        <div className="space-y-3">
          <Field label="Название *"><input type="text" value={form.name} onChange={e => setForm({ ...form, name: e.target.value })} className="field" /></Field>
          <div className="grid grid-cols-2 gap-3">
            <Field label="Банк"><input type="text" value={form.bank} onChange={e => setForm({ ...form, bank: e.target.value })} className="field" /></Field>
            <Field label="Ставка, % годовых"><input type="number" step="0.01" value={form.interest_rate} onChange={e => setForm({ ...form, interest_rate: e.target.value })} className="field" /></Field>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <Field label="Тело кредита, ₽ *"><input type="number" step="0.01" value={form.principal} onChange={e => setForm({ ...form, principal: e.target.value })} className="field" /></Field>
            <Field label="Ежемесячный платёж, ₽"><input type="number" step="0.01" value={form.monthly_payment} onChange={e => setForm({ ...form, monthly_payment: e.target.value })} className="field" /></Field>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <Field label="Дата выдачи"><input type="date" value={form.start_date} onChange={e => setForm({ ...form, start_date: e.target.value })} className="field" /></Field>
            <Field label="Дата окончания"><input type="date" value={form.end_date} onChange={e => setForm({ ...form, end_date: e.target.value })} className="field" /></Field>
          </div>
          <Field label="Примечание"><textarea rows={2} value={form.note} onChange={e => setForm({ ...form, note: e.target.value })} className="field" /></Field>
          <label className="flex items-center gap-2 text-sm">
            <input type="checkbox" checked={form.is_active} onChange={e => setForm({ ...form, is_active: e.target.checked })} />
            Активный
          </label>
        </div>
        <div className="flex justify-end gap-2 mt-6">
          <button onClick={onClose} className="px-4 py-2 text-sm text-gray-600 hover:bg-gray-100 rounded-lg">Отмена</button>
          <button onClick={handleSave} disabled={!form.name || saveMut.isPending} className="px-4 py-2 text-sm bg-blue-600 text-white font-medium rounded-lg hover:bg-blue-700 disabled:opacity-40">
            {saveMut.isPending ? 'Сохранение...' : 'Сохранить'}
          </button>
        </div>
      </div>
      <style>{`.field { width: 100%; border: 1px solid #e5e7eb; border-radius: 8px; padding: 6px 10px; font-size: 14px; outline: none; }`}</style>
    </div>
  )
}

function PaymentModal({ creditId, payment, onClose }: { creditId: number; payment?: Payment; onClose: () => void }) {
  const qc = useQueryClient()
  const [form, setForm] = useState({
    payment_date: payment?.payment_date || new Date().toISOString().slice(0, 10),
    body_amount: payment?.body_amount ? String(payment.body_amount) : '',
    interest_amount: payment?.interest_amount ? String(payment.interest_amount) : '',
    total_amount: payment?.total_amount ? String(payment.total_amount) : '',
    note: payment?.note || '',
  })

  const saveMut = useMutation({
    mutationFn: (data: any) => payment
      ? api.patch(`/credits/payments/${payment.id}`, data)
      : api.post(`/credits/${creditId}/payments`, data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['credit-payments', creditId] })
      qc.invalidateQueries({ queryKey: ['credits'] })
      onClose()
    },
  })

  const handleSave = () => {
    const body = parseFloat(form.body_amount) || 0
    const interest = parseFloat(form.interest_amount) || 0
    const total = form.total_amount ? parseFloat(form.total_amount) : (body + interest)
    saveMut.mutate({
      payment_date: form.payment_date,
      body_amount: body,
      interest_amount: interest,
      total_amount: total,
      note: form.note || null,
    })
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40" onClick={onClose}>
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-md mx-4 p-6" onClick={e => e.stopPropagation()}>
        <h2 className="text-lg font-bold mb-4">{payment ? 'Редактировать платёж' : 'Новый платёж'}</h2>
        <div className="space-y-3">
          <Field label="Дата"><input type="date" value={form.payment_date} onChange={e => setForm({ ...form, payment_date: e.target.value })} className="field" /></Field>
          <div className="grid grid-cols-2 gap-3">
            <Field label="Тело, ₽"><input type="number" step="0.01" value={form.body_amount} onChange={e => setForm({ ...form, body_amount: e.target.value })} className="field" /></Field>
            <Field label="Проценты, ₽"><input type="number" step="0.01" value={form.interest_amount} onChange={e => setForm({ ...form, interest_amount: e.target.value })} className="field" /></Field>
          </div>
          <Field label="Платёж, ₽ (авто)"><input type="number" step="0.01" value={form.total_amount} onChange={e => setForm({ ...form, total_amount: e.target.value })} className="field" placeholder="тело + проценты" /></Field>
          <Field label="Примечание"><input type="text" value={form.note} onChange={e => setForm({ ...form, note: e.target.value })} className="field" /></Field>
        </div>
        <div className="flex justify-end gap-2 mt-6">
          <button onClick={onClose} className="px-4 py-2 text-sm text-gray-600 hover:bg-gray-100 rounded-lg">Отмена</button>
          <button onClick={handleSave} disabled={saveMut.isPending} className="px-4 py-2 text-sm bg-blue-600 text-white font-medium rounded-lg hover:bg-blue-700 disabled:opacity-40">
            {saveMut.isPending ? 'Сохранение...' : 'Сохранить'}
          </button>
        </div>
      </div>
      <style>{`.field { width: 100%; border: 1px solid #e5e7eb; border-radius: 8px; padding: 6px 10px; font-size: 14px; outline: none; }`}</style>
    </div>
  )
}

function AutoImportButton({ creditId }: { creditId: number }) {
  const qc = useQueryClient()
  const importMut = useMutation({
    mutationFn: () => api.post(`/credits/${creditId}/auto-import`, { source: 'wb' }),
    onSuccess: (r: any) => {
      qc.invalidateQueries({ queryKey: ['credit-payments', creditId] })
      qc.invalidateQueries({ queryKey: ['credits'] })
      window.alert(`Импортировано платежей: ${r?.data?.created ?? 0}`)
    },
    onError: () => window.alert('Ошибка импорта'),
  })
  return (
    <button
      onClick={() => importMut.mutate()}
      disabled={importMut.isPending}
      className="text-xs text-emerald-600 hover:text-emerald-700 font-medium disabled:opacity-40"
      title="Импортировать платежи из удержаний WB"
    >
      {importMut.isPending ? '...' : '↓ Импорт из WB'}
    </button>
  )
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <label className="block text-xs text-gray-500 mb-1">{label}</label>
      {children}
    </div>
  )
}
