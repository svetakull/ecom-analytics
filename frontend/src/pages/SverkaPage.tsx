import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { format } from 'date-fns'
import { ClipboardCheck, Loader2, AlertTriangle, CheckCircle, XCircle, ArrowLeftRight } from 'lucide-react'
import { sverkaApi } from '@/api/endpoints'

interface Discrepancy {
  status: string
  source: string
  ms_order: string
  mp_supply: string
  ms_article: string
  mp_article: string
  ms_qty: number | null
  mp_qty: number | null
  agent: string
  organization: string
  comment: string
}

interface MsOrder {
  order_number: string
  date: string
  article: string
  name: string
  quantity: number
  agent: string
  status: string
  store: string
}

interface MpSupply {
  supply_id?: string
  vendor_code?: string
  offer_id?: string
  quantity: number
  warehouse?: string
  storage_warehouse?: string
  create_date?: string
  created_date?: string
}

interface SverkaResult {
  channel: string
  date_from: string
  date_to: string
  summary: {
    total_ms: number
    total_mp: number
    matched: number
    only_ms: number
    only_mp: number
    qty_mismatch: number
  }
  ms_orders: MsOrder[]
  mp_supplies: MpSupply[]
  discrepancies: Discrepancy[]
}

const defaultFrom = format(new Date(new Date().getFullYear(), new Date().getMonth(), 1), 'yyyy-MM-dd')
const defaultTo = format(new Date(), 'yyyy-MM-dd')

export default function SverkaPage() {
  const [dateFrom, setDateFrom] = useState(defaultFrom)
  const [dateTo, setDateTo] = useState(defaultTo)
  const [channel, setChannel] = useState('wb')
  const [agentName, setAgentName] = useState('')
  const [organization, setOrganization] = useState('LanaUlika')
  const [activeTab, setActiveTab] = useState<'discrepancies' | 'ms' | 'mp'>('discrepancies')

  const mutation = useMutation({
    mutationFn: () =>
      sverkaApi
        .run({
          date_from: dateFrom,
          date_to: dateTo,
          channel,
          agent_name: agentName || (channel === 'ozon' ? 'Озон' : ''),
          organization: organization || undefined,
        })
        .then((r) => r.data as SverkaResult),
  })

  const data = mutation.data

  const statusBadge = (status: string) => {
    switch (status) {
      case 'only_moysklad':
        return <span className="px-2 py-0.5 rounded text-xs font-medium bg-amber-100 text-amber-800">Нет в МП</span>
      case 'only_wb':
      case 'only_ozon':
        return <span className="px-2 py-0.5 rounded text-xs font-medium bg-red-100 text-red-800">Нет в МС</span>
      case 'qty_mismatch':
        return <span className="px-2 py-0.5 rounded text-xs font-medium bg-purple-100 text-purple-800">Кол-во</span>
      default:
        return <span className="px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-600">{status}</span>
    }
  }

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center gap-3">
        <ClipboardCheck className="text-indigo-600" size={24} />
        <h1 className="text-2xl font-bold text-gray-900">Сверка поставок</h1>
      </div>

      {/* Filters */}
      <div className="bg-white rounded-lg border border-gray-200 p-4">
        <div className="flex flex-wrap items-end gap-4">
          <div>
            <label className="block text-xs text-gray-500 mb-1">Дата от</label>
            <input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} className="border border-gray-300 rounded px-3 py-1.5 text-sm" />
          </div>
          <div>
            <label className="block text-xs text-gray-500 mb-1">Дата до</label>
            <input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} className="border border-gray-300 rounded px-3 py-1.5 text-sm" />
          </div>
          <div>
            <label className="block text-xs text-gray-500 mb-1">Канал</label>
            <select value={channel} onChange={(e) => setChannel(e.target.value)} className="border border-gray-300 rounded px-3 py-1.5 text-sm">
              <option value="wb">Wildberries</option>
              <option value="ozon">Ozon</option>
            </select>
          </div>
          <div>
            <label className="block text-xs text-gray-500 mb-1">Контрагент МС</label>
            <input
              type="text"
              value={agentName}
              onChange={(e) => setAgentName(e.target.value)}
              placeholder={channel === 'ozon' ? 'Озон' : 'WB ...'}
              className="border border-gray-300 rounded px-3 py-1.5 text-sm w-48"
            />
          </div>
          <div>
            <label className="block text-xs text-gray-500 mb-1">Организация</label>
            <input type="text" value={organization} onChange={(e) => setOrganization(e.target.value)} placeholder="LanaUlika" className="border border-gray-300 rounded px-3 py-1.5 text-sm w-36" />
          </div>
          <button
            onClick={() => mutation.mutate()}
            disabled={mutation.isPending}
            className="bg-indigo-600 hover:bg-indigo-700 text-white px-4 py-1.5 rounded text-sm font-medium disabled:opacity-50 flex items-center gap-2"
          >
            {mutation.isPending ? <Loader2 size={14} className="animate-spin" /> : <ArrowLeftRight size={14} />}
            Запустить сверку
          </button>
        </div>
      </div>

      {/* Error */}
      {mutation.isError && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700 text-sm">
          Ошибка: {(mutation.error as Error)?.message || 'Неизвестная ошибка'}
        </div>
      )}

      {/* Loading */}
      {mutation.isPending && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-6 text-center text-blue-700">
          <Loader2 size={24} className="animate-spin mx-auto mb-2" />
          <p className="text-sm">Загрузка данных из МойСклад и {channel === 'wb' ? 'Wildberries' : 'Ozon'}...</p>
          <p className="text-xs text-blue-500 mt-1">Это может занять 30-60 секунд</p>
        </div>
      )}

      {/* Results */}
      {data && (
        <>
          {/* Summary Cards */}
          <div className="grid grid-cols-2 md:grid-cols-6 gap-3">
            <SumCard label="Позиций МС" value={data.summary.total_ms} color="blue" />
            <SumCard label="Позиций МП" value={data.summary.total_mp} color="blue" />
            <SumCard label="Совпало" value={data.summary.matched} color="emerald" icon={<CheckCircle size={14} />} />
            <SumCard label="Только МС" value={data.summary.only_ms} color="amber" icon={<AlertTriangle size={14} />} />
            <SumCard label="Только МП" value={data.summary.only_mp} color="red" icon={<XCircle size={14} />} />
            <SumCard label="Расхождение" value={data.summary.qty_mismatch} color="purple" icon={<ArrowLeftRight size={14} />} />
          </div>

          {/* Tabs */}
          <div className="flex gap-1 border-b border-gray-200">
            {[
              { key: 'discrepancies' as const, label: `Расхождения (${data.discrepancies.length})` },
              { key: 'ms' as const, label: `Заказы МС (${data.ms_orders.length})` },
              { key: 'mp' as const, label: `Поставки МП (${data.mp_supplies.length})` },
            ].map((tab) => (
              <button
                key={tab.key}
                onClick={() => setActiveTab(tab.key)}
                className={`px-4 py-2 text-sm font-medium border-b-2 -mb-px ${activeTab === tab.key ? 'border-indigo-600 text-indigo-600' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
              >
                {tab.label}
              </button>
            ))}
          </div>

          {/* Discrepancies Table */}
          {activeTab === 'discrepancies' && (
            <div className="bg-white rounded-lg border border-gray-200 overflow-x-auto">
              {data.discrepancies.length === 0 ? (
                <div className="p-8 text-center text-emerald-600">
                  <CheckCircle size={32} className="mx-auto mb-2" />
                  <p className="font-medium">Расхождений не найдено</p>
                </div>
              ) : (
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-gray-50 border-b border-gray-200">
                      <th className="text-left px-3 py-2 font-medium text-gray-600">Статус</th>
                      <th className="text-left px-3 py-2 font-medium text-gray-600">Заказ МС</th>
                      <th className="text-left px-3 py-2 font-medium text-gray-600">Поставка МП</th>
                      <th className="text-left px-3 py-2 font-medium text-gray-600">Артикул МС</th>
                      <th className="text-left px-3 py-2 font-medium text-gray-600">Артикул МП</th>
                      <th className="text-right px-3 py-2 font-medium text-gray-600">Кол-во МС</th>
                      <th className="text-right px-3 py-2 font-medium text-gray-600">Кол-во МП</th>
                      <th className="text-left px-3 py-2 font-medium text-gray-600">Комментарий</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.discrepancies.map((d, i) => (
                      <tr key={i} className={i % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                        <td className="px-3 py-2">{statusBadge(d.status)}</td>
                        <td className="px-3 py-2 text-gray-900 font-mono text-xs">{d.ms_order || '—'}</td>
                        <td className="px-3 py-2 text-gray-900 font-mono text-xs">{d.mp_supply || '—'}</td>
                        <td className="px-3 py-2 text-gray-700">{d.ms_article || '—'}</td>
                        <td className="px-3 py-2 text-gray-700">{d.mp_article || '—'}</td>
                        <td className="px-3 py-2 text-right text-gray-900">{d.ms_qty ?? '—'}</td>
                        <td className="px-3 py-2 text-right text-gray-900">{d.mp_qty ?? '—'}</td>
                        <td className="px-3 py-2 text-gray-500 text-xs">{d.comment}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          )}

          {/* MS Orders Table */}
          {activeTab === 'ms' && (
            <div className="bg-white rounded-lg border border-gray-200 overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="bg-gray-50 border-b border-gray-200">
                    <th className="text-left px-3 py-2 font-medium text-gray-600">Номер заказа</th>
                    <th className="text-left px-3 py-2 font-medium text-gray-600">Дата</th>
                    <th className="text-left px-3 py-2 font-medium text-gray-600">Товар</th>
                    <th className="text-right px-3 py-2 font-medium text-gray-600">Кол-во</th>
                    <th className="text-left px-3 py-2 font-medium text-gray-600">Контрагент</th>
                    <th className="text-left px-3 py-2 font-medium text-gray-600">Статус</th>
                    <th className="text-left px-3 py-2 font-medium text-gray-600">Склад</th>
                  </tr>
                </thead>
                <tbody>
                  {data.ms_orders.map((r, i) => (
                    <tr key={i} className={i % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                      <td className="px-3 py-2 font-mono text-xs text-gray-900">{r.order_number}</td>
                      <td className="px-3 py-2 text-gray-600 text-xs">{r.date ? r.date.slice(0, 19).replace('T', ' ') : ''}</td>
                      <td className="px-3 py-2 text-gray-900">{r.article}</td>
                      <td className="px-3 py-2 text-right text-gray-900 font-medium">{r.quantity}</td>
                      <td className="px-3 py-2 text-gray-600">{r.agent}</td>
                      <td className="px-3 py-2 text-gray-600">{r.status}</td>
                      <td className="px-3 py-2 text-gray-600 text-xs">{r.store}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* MP Supplies Table */}
          {activeTab === 'mp' && (
            <div className="bg-white rounded-lg border border-gray-200 overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="bg-gray-50 border-b border-gray-200">
                    <th className="text-left px-3 py-2 font-medium text-gray-600">Supply ID</th>
                    <th className="text-left px-3 py-2 font-medium text-gray-600">Артикул</th>
                    <th className="text-right px-3 py-2 font-medium text-gray-600">Кол-во</th>
                    <th className="text-left px-3 py-2 font-medium text-gray-600">Склад</th>
                    <th className="text-left px-3 py-2 font-medium text-gray-600">Дата создания</th>
                  </tr>
                </thead>
                <tbody>
                  {data.mp_supplies.map((r, i) => (
                    <tr key={i} className={i % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                      <td className="px-3 py-2 font-mono text-xs text-gray-900">{r.supply_id || ''}</td>
                      <td className="px-3 py-2 text-gray-900">{r.vendor_code || r.offer_id || ''}</td>
                      <td className="px-3 py-2 text-right text-gray-900 font-medium">{r.quantity}</td>
                      <td className="px-3 py-2 text-gray-600 text-xs">{r.warehouse || r.storage_warehouse || ''}</td>
                      <td className="px-3 py-2 text-gray-600 text-xs">{(r.create_date || r.created_date || '').slice(0, 19).replace('T', ' ')}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </div>
  )
}

function SumCard({ label, value, color, icon }: { label: string; value: number; color: string; icon?: React.ReactNode }) {
  const colorMap: Record<string, string> = {
    blue: 'bg-blue-50 text-blue-700 border-blue-200',
    emerald: 'bg-emerald-50 text-emerald-700 border-emerald-200',
    amber: 'bg-amber-50 text-amber-700 border-amber-200',
    red: 'bg-red-50 text-red-700 border-red-200',
    purple: 'bg-purple-50 text-purple-700 border-purple-200',
  }
  return (
    <div className={`rounded-lg border p-3 ${colorMap[color] || colorMap.blue}`}>
      <div className="flex items-center gap-1.5 text-xs opacity-75 mb-1">
        {icon}
        {label}
      </div>
      <div className="text-xl font-bold">{value}</div>
    </div>
  )
}
