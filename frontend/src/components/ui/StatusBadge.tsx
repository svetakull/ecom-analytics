import clsx from 'clsx'

interface Props {
  value: number
  warning?: number
  danger?: number
  suffix?: string
  inverse?: boolean
}

export default function StatusBadge({ value, warning = 15, danger = 7, suffix = '', inverse = false }: Props) {
  const isBad = inverse ? value > danger : value < danger
  const isWarn = inverse ? value > warning : value < warning

  return (
    <span
      className={clsx(
        'inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold',
        isBad
          ? 'bg-red-100 text-red-700'
          : isWarn
          ? 'bg-yellow-100 text-yellow-700'
          : 'bg-green-100 text-green-700'
      )}
    >
      {value.toFixed(1)}{suffix}
    </span>
  )
}
