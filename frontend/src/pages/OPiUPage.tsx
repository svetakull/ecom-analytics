import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { format, subDays, startOfDay, startOfYear } from 'date-fns'
import DateRangePicker, { type DateRange } from '@/components/DateRangePicker'
import { MultiSelectDropdown, WBIcon, OzonIcon, LamodaIcon } from '@/components/MultiSelectDropdown'
import { WeeklyTableHeader, useWeeklyColumns } from '@/components/WeeklyTableHeader'
import clsx from 'clsx'
import { api } from '@/api/client'

const fmt = (n: number) =>
  n.toLocaleString('ru-RU', { minimumFractionDigits: 0, maximumFractionDigits: 0 })

interface OPiULine {
  key: string
  name: string
  amount: number
  pct: number
  level: number
  bold?: boolean
  editable?: boolean
}

interface OPiUData {
  date_from: string
  date_to: string
  months: any[]
  total: any
}

function defaultRange(): DateRange {
  const now = new Date()
  return { from: startOfYear(now), to: subDays(startOfDay(now), 1) }
}

export default function OPiUPage() {
  const [dateRange, setDateRange] = useState<DateRange>(defaultRange)
  const [selectedChannels, setSelectedChannels] = useState<string[]>([])

  const dateFrom = format(dateRange.from, 'yyyy-MM-dd')
  const dateTo = format(dateRange.to, 'yyyy-MM-dd')

  const { data, isLoading, isError } = useQuery<OPiUData>({
    queryKey: ['opiu', dateFrom, dateTo, selectedChannels],
    queryFn: () =>
      api.get('/opiu', {
        params: {
          date_from: dateFrom, date_to: dateTo,
          ...(selectedChannels.length > 0 ? { channels: selectedChannels } : {}),
        },
      }).then((r) => r.data),
    staleTime: 5 * 60 * 1000,
  })

  const allColumns = data ? [data.total, ...data.months] : []
  const { totalCol, monthGroups, expandedMonths, toggleMonth, visibleColumns } = useWeeklyColumns(allColumns)
  const lineNames = data?.total?.lines || []

  return (
    <div>
      <div className="flex items-center justify-between gap-4 mb-4 flex-wrap">
        <div>
          <h1 className="text-xl font-bold text-gray-900">ОПиУ — Отчёт о прибылях и убытках</h1>
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
          <table className="w-auto text-sm" style={{ borderCollapse: 'separate', borderSpacing: 0 }}>
            <WeeklyTableHeader
              monthGroups={monthGroups}
              totalCol={totalCol}
              expandedMonths={expandedMonths}
              toggleMonth={toggleMonth}
              visibleColumns={visibleColumns}
            />
            <tbody>
              {lineNames.map((line: OPiULine, i: number) => {
                const isBold = line.bold
                const isTotal = line.key === 'chistaya' || line.key === 'ebitda' || line.key === 'valovaya'

                return (
                  <tr
                    key={line.key}
                    className={clsx(
                      'border-b border-gray-100 hover:bg-gray-50/50',
                      isTotal && 'bg-gray-50',
                      i % 2 === 0 ? '' : 'bg-[#fafbfc]'
                    )}
                  >
                    <td
                      className={clsx(
                        'sticky left-0 z-10 px-4 py-2 text-sm whitespace-nowrap border-b border-gray-100',
                        i % 2 === 0 ? 'bg-white' : 'bg-[#fafbfc]',
                        isTotal && 'bg-gray-50',
                        isBold ? 'font-semibold text-gray-900' : 'text-gray-700',
                      )}
                      style={{ paddingLeft: `${16 + line.level * 24}px` }}
                    >
                      {line.name}
                    </td>
                    {visibleColumns.map((col: any) => {
                      const colLine = col.lines?.find((l: OPiULine) => l.key === line.key)
                      const val = colLine?.amount || 0
                      const isMonthTotal = col._isMonthTotal || col.is_month_total || col.period?.startsWith('month:')
                      return (
                        <td
                          key={col.period}
                          className={clsx(
                            'px-4 py-2 text-right text-sm tabular-nums border-b border-gray-100 whitespace-nowrap',
                            col.period === 'total' ? 'bg-blue-50/50' :
                            isMonthTotal ? 'bg-amber-50/50 border-l border-l-amber-200' : '',
                            isBold ? 'font-semibold' : '',
                            val < 0 ? 'text-red-600' : isTotal && val > 0 ? 'text-emerald-600' : 'text-gray-800',
                          )}
                        >
                          {val !== 0 ? fmt(val) : '—'}
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
