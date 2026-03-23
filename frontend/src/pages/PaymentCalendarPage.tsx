import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '@/api/client'
import { AlertTriangle, TrendingUp, Wallet, Calendar, ShieldAlert } from 'lucide-react'
import clsx from 'clsx'

const fmt = (n: number) => n.toLocaleString('ru-RU', { minimumFractionDigits: 0, maximumFractionDigits: 0 })
const fmtDate = (s: string) => {
  const d = new Date(s + 'T00:00:00')
  return d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit' })
}

interface Line {
  key: string
  name: string
  amount: number
  level: number
  bold: boolean
  editable: boolean
  source: string
  cash_gap?: boolean
  category?: string
}

interface Week {
  period: string
  period_end: string
  is_forecast: boolean
  is_current: boolean
  lines: Line[]
}

interface Warning {
  week: string
  deficit: number
  message: string
}

interface CalendarData {
  current_balance: number
  weeks_ahead: number
  weeks: Week[]
  warnings: Warning[]
}

export default function PaymentCalendarPage() {
  const [weeksAhead] = useState(12)

  const { data, isLoading, isError } = useQuery<CalendarData>({
    queryKey: ['payment-calendar', weeksAhead],
    queryFn: () => api.get(`/payment-calendar?weeks_ahead=${weeksAhead}`).then(r => r.data),
  })

  if (isLoading) return <div className="flex items-center justify-center h-40 text-gray-400">Загрузка...</div>
  if (isError || !data) return <div className="flex items-center justify-center h-40 text-red-500">Ошибка загрузки</div>

  const weeks = data.weeks
  const warnings = data.warnings
  const hasGap = warnings.length > 0

  // Минимальный баланс
  const minBalance = Math.min(...weeks.map(w => {
    const bl = w.lines.find(l => l.key === 'balance_end')
    return bl?.amount ?? 0
  }))

  // Дней до разрыва
  const today = new Date()
  const firstGapWeek = warnings[0]
  let daysToGap = '—'
  if (firstGapWeek) {
    const gapDate = new Date(firstGapWeek.week + 'T00:00:00')
    daysToGap = String(Math.max(0, Math.round((gapDate.getTime() - today.getTime()) / 86400000)))
  }

  // Все уникальные ключи строк (из первой недели)
  const lineKeys = weeks[0]?.lines?.map(l => l.key) ?? []

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Calendar size={24} className="text-violet-600" />
            Платёжный календарь
          </h1>
          <p className="text-sm text-gray-500 mt-1">Прогноз поступлений и расходов на {weeksAhead} недель</p>
        </div>
      </div>

      {/* Warnings */}
      {hasGap && (
        <div className="bg-red-50 border border-red-200 rounded-xl p-4 flex items-start gap-3">
          <ShieldAlert size={24} className="text-red-600 shrink-0 mt-0.5" />
          <div>
            <p className="font-bold text-red-800">Обнаружен кассовый разрыв!</p>
            {warnings.map((w, i) => (
              <p key={i} className="text-red-700 text-sm">{w.message} (неделя {fmtDate(w.week)})</p>
            ))}
          </div>
        </div>
      )}

      {/* KPI Cards */}
      <div className="grid grid-cols-4 gap-4">
        <div className="bg-white rounded-xl border p-4">
          <div className="text-xs text-gray-500 flex items-center gap-1"><Wallet size={12} /> Текущий баланс</div>
          <div className="text-xl font-bold mt-1">{fmt(data.current_balance)} ₽</div>
        </div>
        <div className="bg-white rounded-xl border p-4">
          <div className="text-xs text-gray-500 flex items-center gap-1"><TrendingUp size={12} /> Мин. баланс (прогноз)</div>
          <div className={clsx('text-xl font-bold mt-1', minBalance < 0 ? 'text-red-600' : 'text-emerald-600')}>
            {fmt(minBalance)} ₽
          </div>
        </div>
        <div className="bg-white rounded-xl border p-4">
          <div className="text-xs text-gray-500 flex items-center gap-1"><AlertTriangle size={12} /> Кассовый разрыв</div>
          <div className={clsx('text-xl font-bold mt-1', hasGap ? 'text-red-600' : 'text-emerald-600')}>
            {hasGap ? 'ДА' : 'НЕТ'}
          </div>
        </div>
        <div className="bg-white rounded-xl border p-4">
          <div className="text-xs text-gray-500">Дней до разрыва</div>
          <div className={clsx('text-xl font-bold mt-1', daysToGap !== '—' ? 'text-red-600' : 'text-gray-400')}>
            {daysToGap}
          </div>
        </div>
      </div>

      {/* Table */}
      <div className="border border-gray-200 bg-white rounded-xl">
        <table className="w-auto text-sm" style={{ borderCollapse: 'separate', borderSpacing: 0 }}>
          <thead>
            <tr className="bg-gray-50">
              <th className="sticky left-0 z-20 bg-gray-50 text-left px-4 py-3 font-medium text-gray-700 border-b border-r min-w-[200px]">
                Статья
              </th>
              {weeks.map((w, i) => {
                const hasGapCol = w.lines.some(l => l.key === 'balance_end' && l.cash_gap)
                return (
                  <th
                    key={i}
                    className={clsx(
                      'text-right px-3 py-3 font-medium border-b min-w-[110px] text-xs',
                      hasGapCol && 'bg-red-50 text-red-700',
                      w.is_current && 'bg-blue-50 border-x-2 border-blue-300',
                      w.is_forecast && !hasGapCol && !w.is_current && 'bg-gray-50 text-gray-500',
                      !w.is_forecast && !hasGapCol && !w.is_current && 'text-gray-700',
                    )}
                  >
                    <div>{fmtDate(w.period)}–{fmtDate(w.period_end)}</div>
                    {w.is_forecast && <div className="text-[10px] text-gray-400">прогноз</div>}
                    {w.is_current && <div className="text-[10px] text-blue-500">текущая</div>}
                  </th>
                )
              })}
            </tr>
          </thead>
          <tbody>
            {lineKeys.map(key => {
              const sampleLine = weeks[0]?.lines.find(l => l.key === key)
              if (!sampleLine) return null
              const isSection = sampleLine.level === 0 && sampleLine.amount === 0 && sampleLine.key.startsWith('section_')
              const isTotalInflows = key === 'total_inflows'
              const isTotalOutflows = key === 'total_outflows'
              const isNetFlow = key === 'net_flow'
              const isBalance = key === 'balance_end'

              return (
                <tr
                  key={key}
                  className={clsx(
                    'border-b border-gray-100',
                    isSection && 'bg-gray-100',
                    isBalance && 'bg-gray-50',
                  )}
                >
                  <td
                    className={clsx(
                      'sticky left-0 z-10 px-4 py-2 border-r bg-white',
                      sampleLine.bold && 'font-bold',
                      sampleLine.level === 1 && 'pl-8',
                      isSection && 'bg-gray-100 font-bold text-gray-600 text-xs uppercase tracking-wide',
                    )}
                  >
                    {sampleLine.name}
                  </td>
                  {weeks.map((w, wi) => {
                    const line = w.lines.find(l => l.key === key)
                    const val = line?.amount ?? 0
                    const cashGap = line?.cash_gap
                    const hasGapCol = w.lines.some(l => l.key === 'balance_end' && l.cash_gap)

                    if (isSection) {
                      return <td key={wi} className={clsx('px-3 py-2', hasGapCol && 'bg-red-50', w.is_current && 'bg-blue-50')} />
                    }

                    return (
                      <td
                        key={wi}
                        className={clsx(
                          'text-right px-3 py-2 tabular-nums',
                          sampleLine.bold && 'font-bold',
                          cashGap && 'bg-red-100 text-red-700 font-bold',
                          hasGapCol && !cashGap && 'bg-red-50',
                          w.is_current && !cashGap && 'bg-blue-50',
                          w.is_forecast && !hasGapCol && !w.is_current && 'text-gray-400',
                          isTotalInflows && val > 0 && 'text-emerald-600',
                          isTotalOutflows && val > 0 && 'text-red-600',
                          isNetFlow && val > 0 && 'text-emerald-600',
                          isNetFlow && val < 0 && 'text-red-600',
                          isBalance && val < 0 && 'text-red-700',
                          isBalance && val >= 0 && 'text-emerald-700',
                        )}
                      >
                        {val === 0 ? '—' : fmt(val)}
                      </td>
                    )
                  })}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
