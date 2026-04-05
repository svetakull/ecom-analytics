/**
 * Настройки налоговых ставок по периодам (УСН %, НДС %).
 * Для каждого месяца года можно задать ставки, они влияют на расчёт налогов и маржи в РнП.
 */
import { useEffect, useMemo, useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/api/client'

interface TaxRate {
  id?: number
  year: number
  month: number | null
  quarter: number | null
  channel_id: number | null
  usn_pct: number
  nds_pct: number
}

const MONTHS = [
  { num: 1, name: 'Январь' }, { num: 2, name: 'Февраль' }, { num: 3, name: 'Март' },
  { num: 4, name: 'Апрель' }, { num: 5, name: 'Май' }, { num: 6, name: 'Июнь' },
  { num: 7, name: 'Июль' }, { num: 8, name: 'Август' }, { num: 9, name: 'Сентябрь' },
  { num: 10, name: 'Октябрь' }, { num: 11, name: 'Ноябрь' }, { num: 12, name: 'Декабрь' },
]

const QUARTERS = [
  { num: 1, name: '1 квартал', months: [1, 2, 3] },
  { num: 2, name: '2 квартал', months: [4, 5, 6] },
  { num: 3, name: '3 квартал', months: [7, 8, 9] },
  { num: 4, name: '4 квартал', months: [10, 11, 12] },
]

export default function TaxRatesPage() {
  const currentYear = new Date().getFullYear()
  const [year, setYear] = useState(currentYear)
  const queryClient = useQueryClient()

  const { data: rates = [], isLoading } = useQuery<TaxRate[]>({
    queryKey: ['tax-rates', year],
    queryFn: () => api.get('/tax-rates', { params: { year } }).then(r => r.data),
  })

  // Локальное состояние для редактирования: { "m:1": {usn, nds}, "q:1": {usn, nds}, ... }
  const [values, setValues] = useState<Record<string, { usn: string; nds: string }>>({})

  useEffect(() => {
    const map: Record<string, { usn: string; nds: string }> = {}
    for (const r of rates) {
      let key = ''
      if (r.month !== null) key = `m:${r.month}`
      else if (r.quarter !== null) key = `q:${r.quarter}`
      else key = 'y'
      map[key] = { usn: String(r.usn_pct || 0), nds: String(r.nds_pct || 0) }
    }
    setValues(map)
  }, [rates])

  const saveMutation = useMutation({
    mutationFn: (items: any[]) => api.post('/tax-rates/bulk', { items }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['tax-rates', year] })
    },
  })

  const setVal = (key: string, field: 'usn' | 'nds', v: string) => {
    setValues(prev => ({ ...prev, [key]: { ...(prev[key] || { usn: '0', nds: '0' }), [field]: v } }))
  }

  const handleSave = () => {
    const items: any[] = []
    for (const [key, v] of Object.entries(values)) {
      const usn = parseFloat(v.usn.replace(',', '.')) || 0
      const nds = parseFloat(v.nds.replace(',', '.')) || 0
      if (key === 'y') {
        items.push({ year, usn_pct: usn, nds_pct: nds })
      } else if (key.startsWith('q:')) {
        items.push({ year, quarter: parseInt(key.slice(2)), usn_pct: usn, nds_pct: nds })
      } else if (key.startsWith('m:')) {
        items.push({ year, month: parseInt(key.slice(2)), usn_pct: usn, nds_pct: nds })
      }
    }
    saveMutation.mutate(items)
  }

  // При изменении квартальной ставки — прокидывается на месяцы квартала (если они ещё не заданы)
  const handleQuarterChange = (q: typeof QUARTERS[0], field: 'usn' | 'nds', v: string) => {
    setVal(`q:${q.num}`, field, v)
    // Автоматически заполняем месяцы квартала
    for (const m of q.months) {
      const cur = values[`m:${m}`]
      if (!cur || !cur[field] || cur[field] === '0') {
        setVal(`m:${m}`, field, v)
      }
    }
  }

  const yearOptions = useMemo(() => {
    const years: number[] = []
    for (let y = currentYear - 3; y <= currentYear + 1; y++) years.push(y)
    return years
  }, [currentYear])

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Налоговые ставки</h1>
          <p className="text-xs text-gray-500 mt-0.5">
            Ставки УСН и НДС по периодам. Применяются к расчёту налогов и маржи в РнП.
          </p>
        </div>
        <div className="flex items-center gap-3">
          <select
            value={year}
            onChange={(e) => setYear(parseInt(e.target.value))}
            className="border border-gray-200 rounded-lg px-3 py-1.5 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500/30"
          >
            {yearOptions.map(y => <option key={y} value={y}>{y}</option>)}
          </select>
          <button
            onClick={handleSave}
            disabled={saveMutation.isPending}
            className="px-4 py-1.5 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 disabled:opacity-40 transition-colors"
          >
            {saveMutation.isPending ? 'Сохранение...' : 'Сохранить'}
          </button>
        </div>
      </div>

      {isLoading ? (
        <div className="text-gray-400 text-sm">Загрузка...</div>
      ) : (
        <div className="bg-white border border-gray-200 rounded-xl p-4">
          <div className="grid grid-cols-1 lg:grid-cols-4 gap-4">
            {QUARTERS.map(q => (
              <div key={q.num} className="border border-gray-200 rounded-lg overflow-hidden">
                {/* Заголовок квартала */}
                <div className="bg-gray-50 px-3 py-2 border-b border-gray-200">
                  <div className="text-sm font-semibold text-gray-800">{q.name}</div>
                  <div className="grid grid-cols-2 gap-2 mt-2">
                    <div>
                      <div className="text-[10px] uppercase text-gray-400 mb-1">УСН, %</div>
                      <input
                        type="text"
                        value={values[`q:${q.num}`]?.usn ?? ''}
                        onChange={(e) => handleQuarterChange(q, 'usn', e.target.value)}
                        className="w-full border border-gray-200 rounded px-2 py-1 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                        placeholder="0"
                      />
                    </div>
                    <div>
                      <div className="text-[10px] uppercase text-gray-400 mb-1">НДС, %</div>
                      <input
                        type="text"
                        value={values[`q:${q.num}`]?.nds ?? ''}
                        onChange={(e) => handleQuarterChange(q, 'nds', e.target.value)}
                        className="w-full border border-gray-200 rounded px-2 py-1 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                        placeholder="0"
                      />
                    </div>
                  </div>
                </div>
                {/* Месяцы */}
                <div className="divide-y divide-gray-100">
                  {q.months.map(m => {
                    const month = MONTHS[m - 1]
                    const key = `m:${m}`
                    return (
                      <div key={m} className="px-3 py-2">
                        <div className="text-xs text-gray-600 mb-1">{month.name}</div>
                        <div className="grid grid-cols-2 gap-2">
                          <input
                            type="text"
                            value={values[key]?.usn ?? ''}
                            onChange={(e) => setVal(key, 'usn', e.target.value)}
                            className="w-full border border-gray-200 rounded px-2 py-1 text-xs text-right focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                            placeholder="УСН"
                          />
                          <input
                            type="text"
                            value={values[key]?.nds ?? ''}
                            onChange={(e) => setVal(key, 'nds', e.target.value)}
                            className="w-full border border-gray-200 rounded px-2 py-1 text-xs text-right focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                            placeholder="НДС"
                          />
                        </div>
                      </div>
                    )
                  })}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {saveMutation.isSuccess && (
        <div className="text-xs text-emerald-600">✓ Сохранено</div>
      )}
    </div>
  )
}
