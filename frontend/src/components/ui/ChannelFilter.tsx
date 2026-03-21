import clsx from 'clsx'

interface Props {
  value: string | null
  onChange: (v: string | null) => void
}

const CHANNELS = [
  { value: null, label: 'Все' },
  { value: 'wb', label: 'WB' },
  { value: 'ozon', label: 'Ozon' },
]

export default function ChannelFilter({ value, onChange }: Props) {
  return (
    <div className="flex gap-1 p-1 bg-gray-100 rounded-lg">
      {CHANNELS.map((ch) => (
        <button
          key={String(ch.value)}
          onClick={() => onChange(ch.value)}
          className={clsx(
            'px-3 py-1.5 text-sm rounded-md transition-all font-medium',
            value === ch.value
              ? 'bg-white text-blue-700 shadow-sm'
              : 'text-gray-500 hover:text-gray-700'
          )}
        >
          {ch.label}
        </button>
      ))}
    </div>
  )
}
