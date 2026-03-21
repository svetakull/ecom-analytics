import { useState, useRef, useEffect } from 'react'

// ── Channel icon components for marketplace branding ──────────────────────

export function WBIcon({ size = 20 }: { size?: number }) {
  return (
    <div
      style={{ width: size, height: size }}
      className="rounded-full bg-purple-600 flex items-center justify-center flex-shrink-0"
    >
      <span className="text-white font-bold" style={{ fontSize: size * 0.38 }}>WB</span>
    </div>
  )
}

export function OzonIcon({ size = 20 }: { size?: number }) {
  return (
    <div
      style={{ width: size, height: size }}
      className="rounded-full bg-blue-500 flex items-center justify-center flex-shrink-0"
    >
      <span className="text-white font-bold" style={{ fontSize: size * 0.45 }}>O</span>
    </div>
  )
}

export function LamodaIcon({ size = 20 }: { size?: number }) {
  return (
    <div
      style={{ width: size, height: size }}
      className="rounded-full bg-gray-900 flex items-center justify-center flex-shrink-0"
    >
      <span className="text-white font-bold" style={{ fontSize: size * 0.38 }}>L</span>
    </div>
  )
}

export function ChannelIcon({ type, size = 20 }: { type: string; size?: number }) {
  if (type === 'wb') return <WBIcon size={size} />
  if (type === 'ozon') return <OzonIcon size={size} />
  if (type === 'lamoda') return <LamodaIcon size={size} />
  return null
}

// ── MultiSelectDropdown ───────────────────────────────────────────────────

export interface SelectOption<T extends string = string> {
  value: T
  label: string
  icon?: React.ReactNode
}

interface MultiSelectDropdownProps<T extends string> {
  options: SelectOption<T>[]
  selected: T[]
  onChange: (selected: T[]) => void
  placeholder: string
  selectedLabel?: string   // e.g. "Маркетплейсы" → shows "Маркетплейсы: 2"
  searchPlaceholder?: string
  emptyMessage?: string
  maxHeight?: string
}

export function MultiSelectDropdown<T extends string>({
  options,
  selected,
  onChange,
  placeholder,
  selectedLabel,
  searchPlaceholder = 'Поиск...',
  emptyMessage = 'Ничего не найдено',
  maxHeight = '240px',
}: MultiSelectDropdownProps<T>) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState('')
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false)
        setSearch('')
      }
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const filtered = options.filter((o) =>
    o.label.toLowerCase().includes(search.toLowerCase())
  )

  const toggle = (value: T) => {
    onChange(
      selected.includes(value)
        ? selected.filter((v) => v !== value)
        : [...selected, value]
    )
  }

  const label =
    selected.length === 0
      ? placeholder
      : `${selectedLabel || placeholder}: ${selected.length}`

  return (
    <div className="relative" ref={ref}>
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className={`flex items-center gap-2 px-3 py-1.5 rounded-xl border text-sm transition-colors shadow-sm ${
          open
            ? 'border-blue-400 bg-white text-gray-800'
            : selected.length > 0
            ? 'border-blue-300 bg-blue-50 text-blue-700'
            : 'border-gray-200 bg-white text-gray-600 hover:bg-gray-50'
        }`}
      >
        <span>{label}</span>
        <svg
          className={`w-3.5 h-3.5 text-gray-400 transition-transform ${open ? 'rotate-180' : ''}`}
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
          strokeWidth={2}
        >
          <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {open && (
        <div className="absolute top-full mt-1.5 left-0 z-50 bg-white border border-gray-200 rounded-xl shadow-xl min-w-[220px]">
          {/* Search */}
          <div className="p-2">
            <div className="flex items-center gap-2 border border-blue-300 rounded-lg px-3 py-2 bg-white focus-within:border-blue-500">
              <svg className="w-3.5 h-3.5 text-gray-400 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <circle cx="11" cy="11" r="8" /><path d="m21 21-4.35-4.35" />
              </svg>
              <input
                type="text"
                placeholder={searchPlaceholder}
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="flex-1 text-sm outline-none min-w-0"
                autoFocus
              />
              {search && (
                <button onClick={() => setSearch('')} className="text-gray-300 hover:text-gray-500 text-xs">✕</button>
              )}
            </div>
          </div>

          {/* Options */}
          <div className="overflow-y-auto px-1 pb-1" style={{ maxHeight }}>
            {filtered.map((o) => {
              const isSelected = selected.includes(o.value)
              return (
                <label
                  key={String(o.value)}
                  className={`flex items-center gap-3 px-3 py-2 rounded-lg cursor-pointer transition-colors ${
                    isSelected ? 'bg-blue-50' : 'hover:bg-gray-50'
                  }`}
                >
                  <input
                    type="checkbox"
                    checked={isSelected}
                    onChange={() => toggle(o.value)}
                    className="w-4 h-4 rounded accent-blue-600 flex-shrink-0"
                  />
                  {o.icon && <span className="flex-shrink-0">{o.icon}</span>}
                  <span className="text-sm text-gray-700 truncate">{o.label}</span>
                </label>
              )
            })}
            {filtered.length === 0 && (
              <div className="px-3 py-3 text-sm text-gray-400 text-center">{emptyMessage}</div>
            )}
          </div>

          {/* Actions */}
          <div className="flex gap-2 p-2 border-t border-gray-100">
            <button
              onClick={() => { setOpen(false); setSearch('') }}
              className="flex-1 px-3 py-1.5 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors"
            >
              Готово
            </button>
            <button
              onClick={() => { onChange([]); setSearch('') }}
              className="px-3 py-1.5 border border-gray-200 text-sm text-gray-600 rounded-lg hover:bg-gray-50 transition-colors"
            >
              Сбросить
            </button>
          </div>
        </div>
      )}
    </div>
  )
}
