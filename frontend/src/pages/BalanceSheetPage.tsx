import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '@/api/client'
import { Scale, ArrowUpRight, ArrowDownRight } from 'lucide-react'
import clsx from 'clsx'

const fmt = (n: number) => n.toLocaleString('ru-RU', { minimumFractionDigits: 0, maximumFractionDigits: 0 })

interface Line {
  key: string
  name: string
  amount: number
  compare_amount?: number
  source: string
  editable: boolean
  level: number
  bold: boolean
  entry_id?: number
}

interface Section {
  key: string
  name: string
  lines: Line[]
}

interface BalanceData {
  as_of_date: string
  compare_date: string | null
  sections: Section[]
  balanced: boolean
  imbalance: number
  total_assets: number
  total_liabilities_equity: number
}

export default function BalanceSheetPage() {
  const [asOf, setAsOf] = useState(() => new Date().toISOString().slice(0, 10))
  const [compare, setCompare] = useState('')

  const params = new URLSearchParams({ as_of_date: asOf })
  if (compare) params.set('compare_date', compare)

  const { data, isLoading, isError } = useQuery<BalanceData>({
    queryKey: ['balance-sheet', asOf, compare],
    queryFn: () => api.get(`/balance-sheet?${params}`).then(r => r.data),
  })

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold flex items-center gap-2">
          <Scale size={24} className="text-amber-600" />
          Управленческий баланс
        </h1>
        <div className="flex items-center gap-4">
          <div>
            <label className="text-xs text-gray-500">На дату</label>
            <input type="date" value={asOf} onChange={e => setAsOf(e.target.value)}
              className="block border rounded px-3 py-1.5 text-sm" />
          </div>
          <div>
            <label className="text-xs text-gray-500">Сравнить с</label>
            <input type="date" value={compare} onChange={e => setCompare(e.target.value)}
              className="block border rounded px-3 py-1.5 text-sm" />
          </div>
        </div>
      </div>

      {isLoading && <div className="flex items-center justify-center h-40 text-gray-400">Загрузка...</div>}
      {isError && <div className="flex items-center justify-center h-40 text-red-500">Ошибка загрузки</div>}

      {data && (
        <>
          {/* Balance check */}
          {!data.balanced && (
            <div className="bg-amber-50 border border-amber-200 rounded-xl p-4 text-amber-800">
              ⚠️ Баланс не сходится! Разница: {fmt(data.imbalance)} ₽
              (Активы {fmt(data.total_assets)} ≠ Пассивы + Капитал {fmt(data.total_liabilities_equity)})
            </div>
          )}

          {/* KPI */}
          <div className="grid grid-cols-3 gap-4">
            {data.sections.map(s => (
              <div key={s.key} className={clsx(
                'bg-white rounded-xl border p-4',
                s.key === 'assets' && 'border-emerald-200',
                s.key === 'liabilities' && 'border-red-200',
                s.key === 'equity' && 'border-blue-200',
              )}>
                <div className="text-xs text-gray-500 uppercase tracking-wide">{s.name}</div>
                <div className={clsx('text-2xl font-bold mt-1',
                  s.key === 'assets' && 'text-emerald-700',
                  s.key === 'liabilities' && 'text-red-700',
                  s.key === 'equity' && 'text-blue-700',
                )}>
                  {fmt(s.lines.find(l => l.key.startsWith('total_'))?.amount ?? 0)} ₽
                </div>
              </div>
            ))}
          </div>

          {/* Sections */}
          {data.sections.map(section => (
            <div key={section.key} className="bg-white rounded-xl border">
              <div className={clsx(
                'px-4 py-3 font-bold text-sm uppercase tracking-wide rounded-t-xl',
                section.key === 'assets' && 'bg-emerald-50 text-emerald-800',
                section.key === 'liabilities' && 'bg-red-50 text-red-800',
                section.key === 'equity' && 'bg-blue-50 text-blue-800',
              )}>
                {section.name}
              </div>
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b bg-gray-50">
                    <th className="text-left px-4 py-2 font-medium text-gray-600">Статья</th>
                    <th className="text-right px-4 py-2 font-medium text-gray-600">
                      {new Date(asOf + 'T00:00:00').toLocaleDateString('ru-RU', { day: 'numeric', month: 'short', year: 'numeric' })}
                    </th>
                    {data.compare_date && (
                      <>
                        <th className="text-right px-4 py-2 font-medium text-gray-500">
                          {new Date(compare + 'T00:00:00').toLocaleDateString('ru-RU', { day: 'numeric', month: 'short', year: 'numeric' })}
                        </th>
                        <th className="text-right px-4 py-2 font-medium text-gray-500">Δ</th>
                      </>
                    )}
                    <th className="text-center px-3 py-2 font-medium text-gray-500 w-16">Источник</th>
                  </tr>
                </thead>
                <tbody>
                  {section.lines.map(line => {
                    const delta = (line.compare_amount !== undefined) ? line.amount - line.compare_amount : null
                    return (
                      <tr key={line.key} className={clsx(
                        'border-b border-gray-100',
                        line.bold && 'bg-gray-50',
                      )}>
                        <td className={clsx('px-4 py-2', line.bold && 'font-bold', line.level === 1 && 'pl-8 text-gray-600')}>
                          {line.name}
                        </td>
                        <td className={clsx('text-right px-4 py-2 tabular-nums', line.bold && 'font-bold')}>
                          {line.amount === 0 ? '—' : fmt(line.amount)}
                        </td>
                        {data.compare_date && (
                          <>
                            <td className="text-right px-4 py-2 tabular-nums text-gray-500">
                              {(line.compare_amount ?? 0) === 0 ? '—' : fmt(line.compare_amount!)}
                            </td>
                            <td className={clsx('text-right px-4 py-2 tabular-nums',
                              delta && delta > 0 && 'text-emerald-600',
                              delta && delta < 0 && 'text-red-600',
                            )}>
                              {delta === null || delta === 0 ? '—' : (
                                <span className="flex items-center justify-end gap-0.5">
                                  {delta > 0 ? <ArrowUpRight size={12} /> : <ArrowDownRight size={12} />}
                                  {fmt(Math.abs(delta))}
                                </span>
                              )}
                            </td>
                          </>
                        )}
                        <td className="text-center px-3 py-2">
                          <span className={clsx('text-[10px] px-1.5 py-0.5 rounded-full',
                            line.source === 'auto' ? 'bg-blue-100 text-blue-600' : 'bg-gray-100 text-gray-500'
                          )}>
                            {line.source === 'auto' ? 'авто' : 'ручн.'}
                          </span>
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          ))}

          {/* Balance equation */}
          <div className={clsx(
            'rounded-xl border p-4 text-center font-bold',
            data.balanced ? 'bg-emerald-50 border-emerald-200 text-emerald-800' : 'bg-red-50 border-red-200 text-red-800'
          )}>
            Активы {fmt(data.total_assets)} = Пассивы + Капитал {fmt(data.total_liabilities_equity)}
            {data.balanced ? ' ✅' : ` ❌ (разница ${fmt(data.imbalance)})`}
          </div>
        </>
      )}
    </div>
  )
}
