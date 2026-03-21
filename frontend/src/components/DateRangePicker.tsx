import { useState, useRef, useEffect } from 'react'
import {
  format,
  startOfMonth,
  endOfMonth,
  eachDayOfInterval,
  startOfWeek,
  endOfWeek,
  addMonths,
  subMonths,
  isSameDay,
  isBefore,
  isAfter,
  isSameMonth,
  isToday,
  parseISO,
} from 'date-fns'
import { ru } from 'date-fns/locale'
import { ChevronLeft, ChevronRight, CalendarDays, X } from 'lucide-react'
import clsx from 'clsx'

export interface DateRange {
  from: Date
  to: Date
}

interface Props {
  value: DateRange
  onChange: (range: DateRange) => void
}

const PRESETS = [
  { label: 'Последние 7 дней', days: 7 },
  { label: 'Последние 14 дней', days: 14 },
  { label: 'Последние 30 дней', days: 30 },
  { label: 'Последние 90 дней', days: 90 },
]

const WEEKDAYS = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']

function getToday() {
  const d = new Date()
  d.setHours(0, 0, 0, 0)
  return d
}

function getYesterday() {
  const d = getToday()
  d.setDate(d.getDate() - 1)
  return d
}

function getDaysGrid(month: Date) {
  const start = startOfWeek(startOfMonth(month), { weekStartsOn: 1 })
  const end = endOfWeek(endOfMonth(month), { weekStartsOn: 1 })
  return eachDayOfInterval({ start, end })
}

function parseInputDate(s: string): Date | null {
  const parts = s.match(/^(\d{2})\.(\d{2})\.(\d{4})$/)
  if (!parts) return null
  const d = new Date(+parts[3], +parts[2] - 1, +parts[1])
  return isNaN(d.getTime()) ? null : d
}

// ── Month grid ─────────────────────────────────────────────────────────────

function MonthGrid({
  month,
  selecting,
  hoverDate,
  maxDate,
  onDayClick,
  onDayHover,
}: {
  month: Date
  selecting: { start: Date | null; end: Date | null }
  hoverDate: Date | null
  maxDate: Date
  onDayClick: (d: Date) => void
  onDayHover: (d: Date) => void
}) {
  const days = getDaysGrid(month)
  const rangeStart = selecting.start
  const rangeEnd =
    selecting.end ?? (rangeStart && hoverDate ? hoverDate : null)

  const inRange = (d: Date) => {
    if (!rangeStart || !rangeEnd) return false
    const lo = isBefore(rangeStart, rangeEnd) ? rangeStart : rangeEnd
    const hi = isBefore(rangeStart, rangeEnd) ? rangeEnd : rangeStart
    return isAfter(d, lo) && isBefore(d, hi)
  }
  const isStart = (d: Date) =>
    !!rangeStart && isSameDay(d, rangeStart)
  const isEnd = (d: Date) =>
    !!rangeEnd && isSameDay(d, rangeEnd)

  return (
    <div className="min-w-[210px]">
      <div className="text-center text-sm font-semibold text-gray-800 mb-2 capitalize">
        {format(month, 'LLLL yyyy', { locale: ru })}
      </div>
      <div className="grid grid-cols-7 mb-1">
        {WEEKDAYS.map((w) => (
          <div key={w} className="text-center text-[11px] text-gray-400 font-medium py-1">
            {w}
          </div>
        ))}
      </div>
      <div className="grid grid-cols-7">
        {days.map((day) => {
          const inCurrent = isSameMonth(day, month)
          const disabled = isAfter(day, maxDate)
          const start = isStart(day)
          const end = isEnd(day)
          const inR = inRange(day)
          return (
            <div
              key={day.toISOString()}
              onMouseEnter={() => !disabled && inCurrent && onDayHover(day)}
              onClick={() => !disabled && inCurrent && onDayClick(day)}
              className={clsx(
                'relative flex items-center justify-center h-8 text-xs select-none transition-colors',
                disabled ? 'cursor-default opacity-25 text-gray-400' : inCurrent ? 'cursor-pointer' : 'cursor-default opacity-30',
                !disabled && inR && 'bg-blue-50',
                !disabled && (start || end) && 'bg-blue-600 text-white rounded-full z-10',
                !disabled && !start && !end && inCurrent && inR && 'hover:bg-blue-100',
                !disabled && !start && !end && !inR && inCurrent && 'hover:bg-gray-100 rounded',
                start && 'rounded-full',
                end && 'rounded-full',
              )}
            >
              {day.getDate()}
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ── Main component ────────────────────────────────────────────────────────

export default function DateRangePicker({ value, onChange }: Props) {
  const [open, setOpen] = useState(false)
  const [leftMonth, setLeftMonth] = useState(() => startOfMonth(value.from))
  const [selecting, setSelecting] = useState<{ start: Date | null; end: Date | null }>({
    start: value.from,
    end: value.to,
  })
  const [hoverDate, setHoverDate] = useState<Date | null>(null)
  const [fromInput, setFromInput] = useState(format(value.from, 'dd.MM.yyyy'))
  const [toInput, setToInput] = useState(format(value.to, 'dd.MM.yyyy'))
  const popoverRef = useRef<HTMLDivElement>(null)
  const triggerRef = useRef<HTMLButtonElement>(null)

  const rightMonth = addMonths(leftMonth, 1)

  // Sync inputs when selecting changes
  useEffect(() => {
    if (selecting.start) setFromInput(format(selecting.start, 'dd.MM.yyyy'))
    if (selecting.end) setToInput(format(selecting.end, 'dd.MM.yyyy'))
  }, [selecting])

  // Close on outside click
  useEffect(() => {
    if (!open) return
    const handler = (e: MouseEvent) => {
      if (
        popoverRef.current &&
        !popoverRef.current.contains(e.target as Node) &&
        triggerRef.current &&
        !triggerRef.current.contains(e.target as Node)
      ) {
        setOpen(false)
      }
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [open])

  const handleDayClick = (day: Date) => {
    if (!selecting.start || (selecting.start && selecting.end)) {
      // Start new selection
      setSelecting({ start: day, end: null })
    } else {
      // Complete selection
      const lo = isBefore(day, selecting.start) ? day : selecting.start
      const hi = isBefore(day, selecting.start) ? selecting.start : day
      setSelecting({ start: lo, end: hi })
    }
  }

  const applyPreset = (days: number) => {
    const yesterday = getYesterday()
    const from = new Date(yesterday)
    from.setDate(yesterday.getDate() - days + 1)
    setSelecting({ start: from, end: yesterday })
    setLeftMonth(startOfMonth(from))
  }

  const handleApply = () => {
    if (selecting.start && selecting.end) {
      onChange({ from: selecting.start, to: selecting.end })
      setOpen(false)
    }
  }

  const handleReset = () => {
    const yesterday = getYesterday()
    const from = new Date(yesterday)
    from.setDate(yesterday.getDate() - 29)
    setSelecting({ start: from, end: yesterday })
    setLeftMonth(startOfMonth(from))
  }

  const handleFromInput = (v: string) => {
    setFromInput(v)
    const d = parseInputDate(v)
    if (d) setSelecting((s) => ({ ...s, start: d }))
  }

  const handleToInput = (v: string) => {
    setToInput(v)
    const d = parseInputDate(v)
    if (d) setSelecting((s) => ({ ...s, end: d }))
  }

  const triggerLabel = `${format(value.from, 'dd.MM.yyyy')} — ${format(value.to, 'dd.MM.yyyy')}`
  const dayCount = Math.round((value.to.getTime() - value.from.getTime()) / 86400000) + 1

  return (
    <div className="relative">
      {/* Trigger */}
      <button
        ref={triggerRef}
        onClick={() => {
          setSelecting({ start: value.from, end: value.to })
          setLeftMonth(startOfMonth(value.from))
          setOpen((o) => !o)
        }}
        className={clsx(
          'flex items-center gap-2 px-3 py-1.5 rounded-lg border text-sm transition-colors',
          open
            ? 'border-blue-500 bg-blue-50 text-blue-700'
            : 'border-gray-200 bg-white text-gray-700 hover:border-gray-300 hover:bg-gray-50'
        )}
      >
        <CalendarDays size={15} className="text-gray-400" />
        <span>{triggerLabel}</span>
        <span className="text-xs text-gray-400 bg-gray-100 px-1.5 py-0.5 rounded">
          {dayCount} дн
        </span>
      </button>

      {/* Popover */}
      {open && (
        <div
          ref={popoverRef}
          className="absolute top-full mt-2 left-0 z-50 bg-white rounded-2xl border border-gray-200 shadow-xl p-4 flex gap-6"
          style={{ minWidth: 580 }}
        >
          {/* Calendars */}
          <div className="flex gap-6">
            {/* Nav left */}
            <div>
              <div className="flex justify-between items-center mb-2">
                <button
                  onClick={() => setLeftMonth((m) => subMonths(m, 1))}
                  className="p-1 rounded hover:bg-gray-100 text-gray-500"
                >
                  <ChevronLeft size={16} />
                </button>
                <div />
              </div>
              <MonthGrid
                month={leftMonth}
                selecting={selecting}
                hoverDate={hoverDate}
                maxDate={getYesterday()}
                onDayClick={handleDayClick}
                onDayHover={setHoverDate}
              />
            </div>

            <div>
              <div className="flex justify-between items-center mb-2">
                <div />
                <button
                  onClick={() => setLeftMonth((m) => addMonths(m, 1))}
                  className="p-1 rounded hover:bg-gray-100 text-gray-500"
                >
                  <ChevronRight size={16} />
                </button>
              </div>
              <MonthGrid
                month={rightMonth}
                selecting={selecting}
                hoverDate={hoverDate}
                maxDate={getYesterday()}
                onDayClick={handleDayClick}
                onDayHover={setHoverDate}
              />
            </div>
          </div>

          {/* Right panel: presets + inputs + actions */}
          <div className="flex flex-col justify-between min-w-[170px]">
            <div className="space-y-1.5">
              {PRESETS.map((p) => (
                <button
                  key={p.days}
                  onClick={() => applyPreset(p.days)}
                  className="w-full text-left text-sm px-3 py-2 rounded-lg border border-gray-200 hover:border-blue-400 hover:bg-blue-50 hover:text-blue-700 transition-colors"
                >
                  {p.label}
                </button>
              ))}
            </div>

            <div className="mt-4 space-y-3">
              {/* Date inputs */}
              <div className="flex items-center gap-2">
                <input
                  type="text"
                  value={fromInput}
                  onChange={(e) => handleFromInput(e.target.value)}
                  placeholder="дд.мм.гггг"
                  className="w-full border border-gray-200 rounded-lg px-2 py-1.5 text-xs text-center focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                />
                <span className="text-gray-400 text-xs">—</span>
                <input
                  type="text"
                  value={toInput}
                  onChange={(e) => handleToInput(e.target.value)}
                  placeholder="дд.мм.гггг"
                  className="w-full border border-gray-200 rounded-lg px-2 py-1.5 text-xs text-center focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                />
              </div>

              {/* Action buttons */}
              <div className="flex gap-2">
                <button
                  onClick={handleReset}
                  className="flex-1 text-sm px-3 py-2 rounded-lg border border-gray-200 text-gray-600 hover:bg-gray-50 transition-colors"
                >
                  Сбросить
                </button>
                <button
                  onClick={handleApply}
                  disabled={!selecting.start || !selecting.end}
                  className="flex-1 text-sm px-3 py-2 rounded-lg bg-blue-600 text-white font-medium hover:bg-blue-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                >
                  Готово
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
