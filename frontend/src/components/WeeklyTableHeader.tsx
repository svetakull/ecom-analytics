import { useState, useMemo } from 'react'
import clsx from 'clsx'
import { ChevronRight, ChevronDown } from 'lucide-react'

interface Column {
  period: string
  label?: string
  lines: any[]
  is_month_total?: boolean
}

interface MonthGroup {
  monthKey: string
  label: string
  weekColumns: Column[]
  totalColumn: Column | null
}

const monthNames: Record<string, string> = {
  '01': 'Январь', '02': 'Февраль', '03': 'Март', '04': 'Апрель',
  '05': 'Май', '06': 'Июнь', '07': 'Июль', '08': 'Август',
  '09': 'Сентябрь', '10': 'Октябрь', '11': 'Ноябрь', '12': 'Декабрь',
}

function getMonthKey(col: Column): string {
  if (col.is_month_total || col.period.startsWith('month:')) {
    return col.period.replace('month:', '')
  }
  if (col.period.length === 10) {
    const d = new Date(col.period)
    d.setDate(d.getDate() + 3) // Thursday rule
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`
  }
  return col.period.slice(0, 7)
}

function weekLabel(period: string): string {
  if (period.length === 10) {
    const d = new Date(period)
    const end = new Date(d)
    end.setDate(end.getDate() + 6)
    const f = (dt: Date) => `${dt.getDate().toString().padStart(2, '0')}.${(dt.getMonth() + 1).toString().padStart(2, '0')}`
    return `${f(d)}–${f(end)}`
  }
  return period
}

export function useWeeklyColumns(columns: Column[]) {
  const [expandedMonths, setExpandedMonths] = useState<Set<string>>(new Set())

  const { totalCol, monthGroups } = useMemo(() => {
    const totalCol = columns.find(c => c.period === 'total') || null
    const rest = columns.filter(c => c.period !== 'total')

    const groups: MonthGroup[] = []
    let currentMonth = ''
    let currentGroup: MonthGroup | null = null

    for (const col of rest) {
      const isMonthTotal = col.is_month_total || col.period.startsWith('month:')
      const mk = getMonthKey(col)

      if (mk !== currentMonth) {
        const m = mk.split('-')
        currentGroup = {
          monthKey: mk,
          label: `${monthNames[m[1]] || m[1]} ${m[0]}`,
          weekColumns: [],
          totalColumn: null,
        }
        groups.push(currentGroup)
        currentMonth = mk
      }

      if (isMonthTotal) {
        if (currentGroup) currentGroup.totalColumn = col
      } else {
        if (currentGroup) currentGroup.weekColumns.push(col)
      }
    }

    return { totalCol, monthGroups: groups }
  }, [columns])

  const toggleMonth = (mk: string) => {
    setExpandedMonths(prev => {
      const next = new Set(prev)
      if (next.has(mk)) next.delete(mk)
      else next.add(mk)
      return next
    })
  }

  // Build visible columns in order
  const visibleColumns = useMemo(() => {
    const result: (Column & { _isMonthTotal?: boolean; _monthKey?: string })[] = []
    if (totalCol) result.push(totalCol)

    for (const g of monthGroups) {
      const isExpanded = expandedMonths.has(g.monthKey)
      if (isExpanded) {
        for (const wc of g.weekColumns) {
          result.push(wc)
        }
      }
      // Always show month total
      if (g.totalColumn) {
        result.push({ ...g.totalColumn, _isMonthTotal: true, _monthKey: g.monthKey })
      } else if (g.weekColumns.length > 0) {
        // No month total column from backend — shouldn't happen but fallback
        result.push({ ...g.weekColumns[0], _isMonthTotal: true, _monthKey: g.monthKey })
      }
    }

    return result
  }, [totalCol, monthGroups, expandedMonths])

  return { totalCol, monthGroups, expandedMonths, toggleMonth, visibleColumns }
}

interface WeeklyTableHeaderProps {
  monthGroups: MonthGroup[]
  totalCol: Column | null
  expandedMonths: Set<string>
  toggleMonth: (mk: string) => void
  visibleColumns: Column[]
}

export function WeeklyTableHeader({ monthGroups, totalCol, expandedMonths, toggleMonth, visibleColumns }: WeeklyTableHeaderProps) {
  return (
    <thead className="sticky top-0 z-20" style={{ background: '#f9fafb' }}>
      {/* Row 1: Month groups */}
      <tr>
        <th
          rowSpan={2}
          className="sticky left-0 z-30 bg-gray-50 px-4 py-2 text-left text-xs font-semibold text-gray-600 w-[220px] border-b border-gray-200 align-bottom"
        >
          Статья
        </th>
        {totalCol && (
          <th
            rowSpan={2}
            className="bg-blue-50 text-blue-800 px-2 py-2 text-right text-xs font-bold border-b border-gray-200 w-[90px] align-bottom"
          >
            Итого
          </th>
        )}
        {monthGroups.map((g) => {
          const isExpanded = expandedMonths.has(g.monthKey)
          const colSpan = isExpanded ? g.weekColumns.length + 1 : 1
          return (
            <th
              key={g.monthKey}
              colSpan={colSpan}
              className="bg-gray-100 text-gray-700 px-2 py-1.5 text-center text-xs font-bold border-b border-gray-300 border-l border-l-gray-300 cursor-pointer hover:bg-gray-200 transition-colors select-none"
              onClick={() => toggleMonth(g.monthKey)}
            >
              <span className="inline-flex items-center gap-1">
                {isExpanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
                {g.label}
              </span>
            </th>
          )
        })}
      </tr>
      {/* Row 2: Week labels (only visible if month expanded) */}
      <tr>
        {monthGroups.map((g) => {
          const isExpanded = expandedMonths.has(g.monthKey)
          if (!isExpanded) {
            // Плейсхолдер под свёрнутым месяцем, чтобы колонки недель
            // в развёрнутых месяцах не сползали влево
            return (
              <th
                key={`${g.monthKey}-ph`}
                className="bg-gray-100 border-b border-gray-300 border-l border-l-gray-300"
              />
            )
          }
          return [
            ...g.weekColumns.map((wc) => (
              <th
                key={wc.period}
                className="bg-gray-50 text-gray-500 px-2 py-1.5 text-right text-[11px] font-medium border-b border-gray-200 w-[85px]"
              >
                {weekLabel(wc.period)}
              </th>
            )),
            <th
              key={`${g.monthKey}-total`}
              className="bg-amber-50 text-amber-700 px-2 py-1.5 text-right text-[11px] font-semibold border-b border-gray-200 border-l border-l-amber-200 w-[85px]"
            >
              Итого
            </th>,
          ]
        })}
      </tr>
    </thead>
  )
}
