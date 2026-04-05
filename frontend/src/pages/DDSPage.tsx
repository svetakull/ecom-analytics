import { useState, useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { format, subDays, startOfDay, startOfYear } from 'date-fns'
import DateRangePicker, { type DateRange } from '@/components/DateRangePicker'
import { MultiSelectDropdown, WBIcon, OzonIcon, LamodaIcon } from '@/components/MultiSelectDropdown'
import { WeeklyTableHeader, useWeeklyColumns } from '@/components/WeeklyTableHeader'
import clsx from 'clsx'
import { api } from '@/api/client'
import { Pencil, Check, X } from 'lucide-react'

const fmt = (n: number) =>
  n.toLocaleString('ru-RU', { minimumFractionDigits: 0, maximumFractionDigits: 0 })

interface DDSLine {
  key: string
  name: string
  amount: number
  level: number
  bold?: boolean
  editable?: boolean
  section?: string
  category?: string | null
}

interface DDSData {
  date_from: string
  date_to: string
  periods: any[]
  total: any
}

function defaultRange(): DateRange {
  const now = new Date()
  return { from: startOfYear(now), to: subDays(startOfDay(now), 1) }
}

export default function DDSPage() {
  const [dateRange, setDateRange] = useState<DateRange>(defaultRange)
  const [selectedChannels, setSelectedChannels] = useState<string[]>([])
  const [editingCell, setEditingCell] = useState<{ key: string; period: string; category: string } | null>(null)
  const [editValue, setEditValue] = useState('')
  const queryClient = useQueryClient()

  const dateFrom = format(dateRange.from, 'yyyy-MM-dd')
  const dateTo = format(dateRange.to, 'yyyy-MM-dd')

  const { data, isLoading, isError } = useQuery<DDSData>({
    queryKey: ['dds', dateFrom, dateTo, selectedChannels],
    queryFn: () =>
      api.get('/dds', {
        params: {
          date_from: dateFrom, date_to: dateTo,
          ...(selectedChannels.length > 0 ? { channels: selectedChannels } : {}),
        },
      }).then((r) => r.data),
    staleTime: 5 * 60 * 1000,
  })

  const allColumns = data ? [data.total, ...data.periods] : []
  const { totalCol, monthGroups, expandedMonths, toggleMonth, visibleColumns } = useWeeklyColumns(allColumns)

  const saveMutation = useMutation({
    mutationFn: (params: { category: string; date: string; amount: number }) =>
      api.post('/dds/manual', params),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dds'] })
      setEditingCell(null)
    },
  })

  const startEdit = useCallback((key: string, period: string, category: string, currentVal: number) => {
    setEditingCell({ key, period, category })
    setEditValue(currentVal === 0 ? '' : String(currentVal))
  }, [])

  const confirmEdit = useCallback(() => {
    if (!editingCell) return
    const num = parseFloat(editValue.replace(/\s/g, '').replace(',', '.')) || 0
    const periodDate = editingCell.period.length === 7 ? editingCell.period + '-01' : editingCell.period
    saveMutation.mutate({ category: editingCell.category, date: periodDate, amount: num })
  }, [editingCell, editValue, saveMutation])

  const cancelEdit = useCallback(() => setEditingCell(null), [])

  const lineNames = data?.total?.lines || []

  return (
    <div>
      <div className="flex items-center justify-between gap-4 mb-4 flex-wrap">
        <div>
          <h1 className="text-xl font-bold text-gray-900">ДДС — Движение денежных средств</h1>
          {data && <div className="text-xs text-gray-400 mt-0.5">{data.date_from} — {data.date_to}</div>}
        </div>
        <div className="flex items-center gap-2 flex-wrap">
          <DateRangePicker value={dateRange} onChange={setDateRange} />
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
        </div>
      </div>

      <div className="border border-gray-200 bg-white rounded-xl">
        {isLoading && <div className="flex items-center justify-center h-40 text-gray-400">Загрузка...</div>}
        {isError && <div className="flex items-center justify-center h-40 text-red-500">Ошибка загрузки</div>}
        {data && visibleColumns.length > 0 && (
          <table className="w-auto text-sm" style={{ borderCollapse: 'separate', borderSpacing: 0, tableLayout: 'fixed' }}>
            <colgroup>
              <col style={{ width: 220 }} />
              {visibleColumns.map((col: any) => (
                <col key={col.period} style={{ width: col.period === 'total' ? 100 : 90 }} />
              ))}
            </colgroup>
            <WeeklyTableHeader
              monthGroups={monthGroups}
              totalCol={totalCol}
              expandedMonths={expandedMonths}
              toggleMonth={toggleMonth}
              visibleColumns={visibleColumns}
            />
            <tbody>
              {lineNames.map((line: DDSLine, i: number) => {
                const isTotal = line.bold && (line.key.startsWith('itogo') || line.key === 'chisty_potok' || line.key === 'ostatok_konec')
                const isAddAction = (line as any).is_action === 'add_account'

                if (isAddAction) {
                  return (
                    <tr key={line.key} className="border-b border-gray-100">
                      <td
                        colSpan={1 + visibleColumns.length}
                        className="sticky left-0 px-4 py-2 text-sm bg-white border-b border-gray-100"
                        style={{ paddingLeft: `${16 + line.level * 20}px` }}
                      >
                        <button
                          onClick={async () => {
                            const name = window.prompt('Название счёта:')
                            if (!name || !name.trim()) return
                            const trimmed = name.trim()
                            // Сохраняем 0 по новому счёту на конец выбранного периода
                            try {
                              await api.post('/dds/manual', {
                                category: `balance_acc:${trimmed}`,
                                date: dateTo,
                                amount: 0,
                              })
                              queryClient.invalidateQueries({ queryKey: ['dds'] })
                            } catch (e) { console.error(e) }
                          }}
                          className="text-blue-600 hover:text-blue-700 text-sm font-medium"
                        >
                          + Добавить счёт
                        </button>
                      </td>
                    </tr>
                  )
                }

                return (
                  <tr
                    key={line.key}
                    className={clsx(
                      'border-b border-gray-100 hover:bg-gray-50/50',
                      line.level === 0 && line.bold && !isTotal && 'border-t-2 border-t-gray-200',
                    )}
                  >
                    <td
                      className={clsx(
                        'sticky left-0 z-10 px-4 py-2 text-sm whitespace-nowrap border-b border-gray-100',
                        i % 2 === 0 ? 'bg-white' : 'bg-[#fafbfc]',
                        isTotal && 'bg-gray-50',
                        line.bold ? 'font-semibold text-gray-900' : 'text-gray-700',
                        line.level === 0 && line.bold && !isTotal && 'text-gray-900 uppercase text-xs tracking-wider pt-3',
                      )}
                      style={{ paddingLeft: `${16 + line.level * 20}px` }}
                    >
                      {line.name}
                    </td>
                    {visibleColumns.map((col: any) => {
                      const colLine = col.lines?.find((l: DDSLine) => l.key === line.key)
                      const val = colLine?.amount || 0
                      const isEditing = editingCell?.key === line.key && editingCell?.period === col.period
                      const isMonthTotal = col._isMonthTotal || col.is_month_total || col.period?.startsWith('month:')
                      const canEdit = line.editable && line.category && col.period !== 'total' && !isMonthTotal

                      return (
                        <td
                          key={col.period}
                          className={clsx(
                            'px-3 py-2 text-right text-sm tabular-nums border-b border-gray-100 whitespace-nowrap',
                            col.period === 'total' ? 'bg-blue-50/50' :
                            isMonthTotal ? 'bg-amber-50/50 border-l border-l-amber-200' : '',
                            line.bold ? 'font-semibold' : '',
                            val < 0 ? 'text-red-600' : isTotal && val > 0 ? 'text-emerald-600' : 'text-gray-800',
                            canEdit && 'cursor-pointer group',
                          )}
                          onClick={() => {
                            if (canEdit && !isEditing) startEdit(line.key, col.period, line.category!, val)
                          }}
                        >
                          {isEditing ? (
                            <div className="flex items-center justify-end gap-1">
                              <input
                                type="text"
                                value={editValue}
                                onChange={(e) => setEditValue(e.target.value)}
                                onKeyDown={(e) => {
                                  if (e.key === 'Enter') confirmEdit()
                                  if (e.key === 'Escape') cancelEdit()
                                }}
                                className="w-24 text-right text-sm border border-blue-300 rounded px-1.5 py-0.5 focus:outline-none focus:ring-1 focus:ring-blue-400"
                                autoFocus
                              />
                              <button onClick={confirmEdit} className="text-emerald-600 hover:text-emerald-700"><Check size={14} /></button>
                              <button onClick={cancelEdit} className="text-gray-400 hover:text-gray-600"><X size={14} /></button>
                            </div>
                          ) : (
                            <span className="inline-flex items-center gap-1">
                              {val !== 0 ? fmt(val) : '—'}
                              {canEdit && <Pencil size={11} className="text-gray-300 opacity-0 group-hover:opacity-100 transition-opacity" />}
                            </span>
                          )}
                        </td>
                      )
                    })}
                  </tr>
                )
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
