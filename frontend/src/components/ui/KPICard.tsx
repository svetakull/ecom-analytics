import clsx from 'clsx'
import { TrendingUp, TrendingDown, Minus } from 'lucide-react'
import type { KPICard as KPICardType } from '@/types'

interface Props {
  data: KPICardType
  className?: string
}

const fmt = (value: number, unit: string) => {
  if (unit === '₽') return `${value.toLocaleString('ru-RU', { maximumFractionDigits: 0 })} ₽`
  if (unit === '%') return `${value.toFixed(1)}%`
  return `${value.toLocaleString('ru-RU')} ${unit}`
}

export default function KPICard({ data, className }: Props) {
  const { title, value, unit, trend_pct, trend_direction } = data

  return (
    <div className={clsx('bg-white rounded-xl border border-gray-200 p-5 shadow-sm', className)}>
      <div className="text-xs font-medium text-gray-400 uppercase tracking-wide mb-2">{title}</div>
      <div className="text-2xl font-bold text-gray-900">{fmt(value, unit)}</div>
      {trend_pct !== 0 && (
        <div
          className={clsx(
            'flex items-center gap-1 mt-2 text-xs font-medium',
            trend_direction === 'up' ? 'text-green-600' : trend_direction === 'down' ? 'text-red-500' : 'text-gray-400'
          )}
        >
          {trend_direction === 'up' ? (
            <TrendingUp size={13} />
          ) : trend_direction === 'down' ? (
            <TrendingDown size={13} />
          ) : (
            <Minus size={13} />
          )}
          {Math.abs(trend_pct).toFixed(1)}% vs вчера
        </div>
      )}
    </div>
  )
}
