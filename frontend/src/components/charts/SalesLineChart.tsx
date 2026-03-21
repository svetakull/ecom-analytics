import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts'
import { format, parseISO } from 'date-fns'
import { ru } from 'date-fns/locale'

interface DataPoint {
  date: string
  orders_qty: number
  orders_rub: number
  sales_qty?: number
  sales_rub?: number
}

interface Props {
  data: DataPoint[]
  showRub?: boolean
}

export default function SalesLineChart({ data, showRub = false }: Props) {
  const formatted = data.map((d) => ({
    ...d,
    label: format(parseISO(d.date), 'd MMM', { locale: ru }),
  }))

  return (
    <ResponsiveContainer width="100%" height={280}>
      <LineChart data={formatted} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
        <XAxis dataKey="label" tick={{ fontSize: 11 }} tickLine={false} axisLine={false} />
        <YAxis tick={{ fontSize: 11 }} tickLine={false} axisLine={false} />
        <Tooltip
          contentStyle={{ borderRadius: 8, border: '1px solid #e5e7eb', fontSize: 12 }}
          formatter={(value: number, name: string) => {
            if (name === 'orders_rub' || name === 'sales_rub')
              return [`${value.toLocaleString('ru-RU')} ₽`, name === 'orders_rub' ? 'Заказы ₽' : 'Продажи ₽']
            return [value, name === 'orders_qty' ? 'Заказы шт' : 'Продажи шт']
          }}
        />
        <Legend
          formatter={(value) => {
            const labels: Record<string, string> = {
              orders_qty: 'Заказы, шт',
              sales_qty: 'Продажи, шт',
              orders_rub: 'Заказы, ₽',
              sales_rub: 'Продажи, ₽',
            }
            return labels[value] || value
          }}
        />
        {showRub ? (
          <>
            <Line type="monotone" dataKey="orders_rub" stroke="#3b82f6" strokeWidth={2} dot={false} />
            <Line type="monotone" dataKey="sales_rub" stroke="#10b981" strokeWidth={2} dot={false} />
          </>
        ) : (
          <>
            <Line type="monotone" dataKey="orders_qty" stroke="#3b82f6" strokeWidth={2} dot={false} />
            {data[0]?.sales_qty !== undefined && (
              <Line type="monotone" dataKey="sales_qty" stroke="#10b981" strokeWidth={2} dot={false} />
            )}
          </>
        )}
      </LineChart>
    </ResponsiveContainer>
  )
}
