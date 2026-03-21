import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { salesApi } from '@/api/endpoints'
import ChannelFilter from '@/components/ui/ChannelFilter'
import SalesLineChart from '@/components/charts/SalesLineChart'

const STATUS_LABELS: Record<string, string> = {
  new: 'Новый',
  confirmed: 'Подтверждён',
  shipped: 'Отправлен',
  delivered: 'Доставлен',
  cancelled: 'Отменён',
  returned: 'Возврат',
}

const STATUS_COLORS: Record<string, string> = {
  new: 'bg-blue-100 text-blue-700',
  confirmed: 'bg-yellow-100 text-yellow-700',
  shipped: 'bg-purple-100 text-purple-700',
  delivered: 'bg-green-100 text-green-700',
  cancelled: 'bg-gray-100 text-gray-500',
  returned: 'bg-red-100 text-red-700',
}

export default function SalesPage() {
  const [channel, setChannel] = useState<string | null>(null)
  const [view, setView] = useState<'rub' | 'qty'>('qty')

  const summaryQ = useQuery({
    queryKey: ['sales-summary', channel],
    queryFn: () =>
      salesApi.summary({ channel_type: channel || undefined }).then((r) => r.data),
  })

  const dynamicQ = useQuery({
    queryKey: ['sales-dynamic', channel],
    queryFn: () =>
      salesApi.dynamic({ channel_type: channel || undefined }).then((r) => r.data),
  })

  const ordersQ = useQuery({
    queryKey: ['orders', channel],
    queryFn: () =>
      salesApi.orders({ channel_type: channel || undefined, limit: 100 }).then((r) => r.data),
  })

  const s = summaryQ.data

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <h1 className="text-xl font-bold text-gray-800">Продажи</h1>
        <ChannelFilter value={channel} onChange={setChannel} />
      </div>

      {/* Summary Cards */}
      {s && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          {[
            { label: 'Заказы', value: `${s.total_orders_qty.toLocaleString('ru-RU')} шт` },
            { label: 'Выручка (заказы)', value: `${s.total_orders_rub.toLocaleString('ru-RU', { maximumFractionDigits: 0 })} ₽` },
            { label: 'Выкуп', value: `${s.buyout_rate_pct.toFixed(1)}%` },
            { label: 'Средний чек', value: `${s.avg_order_price.toLocaleString('ru-RU', { maximumFractionDigits: 0 })} ₽` },
          ].map((item) => (
            <div key={item.label} className="bg-white rounded-xl border border-gray-200 px-4 py-3 shadow-sm">
              <div className="text-xs text-gray-400 mb-0.5">{item.label}</div>
              <div className="text-lg font-bold text-gray-800">{item.value}</div>
            </div>
          ))}
        </div>
      )}

      {/* Chart */}
      <div className="bg-white rounded-xl border border-gray-200 p-5 shadow-sm">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-sm font-semibold text-gray-700">Динамика за 30 дней</h2>
          <div className="flex gap-1 p-1 bg-gray-100 rounded-lg">
            {(['qty', 'rub'] as const).map((v) => (
              <button
                key={v}
                onClick={() => setView(v)}
                className={`px-3 py-1 text-xs rounded-md font-medium transition-all ${view === v ? 'bg-white shadow-sm text-blue-700' : 'text-gray-500'}`}
              >
                {v === 'qty' ? 'Штуки' : 'Рубли'}
              </button>
            ))}
          </div>
        </div>
        {dynamicQ.data && <SalesLineChart data={dynamicQ.data} showRub={view === 'rub'} />}
      </div>

      {/* Orders Table */}
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
        <div className="px-5 py-3 border-b border-gray-100">
          <h2 className="text-sm font-semibold text-gray-700">Последние заказы</h2>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-200">
              <tr>
                {['ID', 'Артикул', 'Название', 'Канал', 'Дата', 'Кол-во', 'Цена', 'Статус'].map((h) => (
                  <th key={h} className={`px-3 py-2.5 text-xs font-medium text-gray-400 uppercase ${h === 'ID' || h === 'Кол-во' || h === 'Цена' ? 'text-right' : 'text-left'}`}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {ordersQ.isLoading && (
                <tr><td colSpan={8} className="px-3 py-6 text-center text-gray-400">Загрузка...</td></tr>
              )}
              {ordersQ.data?.map((order) => (
                <tr key={order.id} className="hover:bg-gray-50 transition-colors">
                  <td className="px-3 py-2.5 text-right text-gray-400 text-xs">{order.id}</td>
                  <td className="px-3 py-2.5 font-mono text-xs">{order.seller_article}</td>
                  <td className="px-3 py-2.5 text-gray-600 max-w-[200px] truncate">{order.sku_name}</td>
                  <td className="px-3 py-2.5">
                    <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${order.channel === 'Wildberries' ? 'bg-purple-100 text-purple-700' : 'bg-blue-100 text-blue-700'}`}>
                      {order.channel}
                    </span>
                  </td>
                  <td className="px-3 py-2.5 text-gray-500 text-xs">{order.order_date}</td>
                  <td className="px-3 py-2.5 text-right">{order.qty}</td>
                  <td className="px-3 py-2.5 text-right font-medium">{order.price.toLocaleString('ru-RU', { maximumFractionDigits: 0 })} ₽</td>
                  <td className="px-3 py-2.5">
                    <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${STATUS_COLORS[order.status] || 'bg-gray-100 text-gray-600'}`}>
                      {STATUS_LABELS[order.status] || order.status}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
