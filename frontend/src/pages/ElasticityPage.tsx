import { useState } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { TrendingDown, ArrowLeft, Target, DollarSign, BarChart3, RotateCcw, Loader2 } from 'lucide-react'
import { ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Line, ComposedChart } from 'recharts'
import { elasticityApi } from '@/api/endpoints'

interface SkuRow {
  sku_id: number
  seller_article: string
  name: string
  current_price: number
  orders_day: number
  elasticity: number
  r_squared: number
  data_points: number
  margin_pct: number
  net_profit_30d: number
  optimal_price: number
  optimal_profit: number
  profit_delta_pct: number
  turnover_days: number
  stock: number
}

interface Scenario {
  label: string
  price: number
  orders_day: number
  orders_total: number
  revenue: number
  cogs: number
  logistics: number
  commission: number
  storage: number
  advertising: number
  gross_margin: number
  margin_pct: number
  net_profit: number
  turnover_days: number
  turns_per_year: number
  invested_capital: number
  annual_profit: number
  annual_roi_pct: number
  roi_pct: number
}

interface SkuDetail {
  sku_id: number
  seller_article: string
  name: string
  elasticity: { elasticity: number; r_squared: number; data_points: number; scatter: { price: number; orders: number }[] }
  unit_economics: { avg_price: number; spp_pct: number; avg_orders_day: number; cogs_per_unit: number }
  stock: number
  spp_pct: number
  scenarios: Scenario[]
  optimal_price: number
  optimal_profit: number
}

const fmt = (n: number) => n >= 1000 ? `${(n / 1000).toFixed(0)}K` : n.toFixed(0)
const fmtPrice = (n: number) => n.toLocaleString('ru-RU', { maximumFractionDigits: 0 })

function elasticityColor(e: number) {
  if (e < -1.5) return 'text-red-600 bg-red-50'
  if (e < -0.5) return 'text-amber-600 bg-amber-50'
  return 'text-emerald-600 bg-emerald-50'
}

function elasticityLabel(e: number) {
  if (e < -1.5) return 'Высокая'
  if (e < -0.5) return 'Средняя'
  return 'Низкая'
}

export default function ElasticityPage() {
  const [selectedSku, setSelectedSku] = useState<number | null>(null)
  const [channel] = useState('wb')

  const { data: dashboard, isLoading } = useQuery({
    queryKey: ['elasticity-dashboard', channel],
    queryFn: () => elasticityApi.dashboard(channel).then(r => r.data as { skus: SkuRow[] }),
  })

  if (selectedSku) {
    return <SkuDetailView skuId={selectedSku} channel={channel} onBack={() => setSelectedSku(null)} />
  }

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center gap-3">
        <TrendingDown className="text-violet-600" size={24} />
        <h1 className="text-2xl font-bold text-gray-900">Ценовая аналитика</h1>
        <span className="text-sm text-gray-500 ml-2">WB / Топ-20 SKU</span>
      </div>

      {isLoading && (
        <div className="flex items-center gap-2 text-gray-500 py-12 justify-center">
          <Loader2 className="animate-spin" size={20} />
          Расчёт эластичности...
        </div>
      )}

      {dashboard && (
        <div className="bg-white rounded-lg border border-gray-200 overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 border-b border-gray-200">
                <th className="text-left px-3 py-2.5 font-medium text-gray-600">Артикул</th>
                <th className="text-right px-3 py-2.5 font-medium text-gray-600">Цена</th>
                <th className="text-right px-3 py-2.5 font-medium text-gray-600">Заказов/день</th>
                <th className="text-center px-3 py-2.5 font-medium text-gray-600">Эластичность</th>
                <th className="text-right px-3 py-2.5 font-medium text-gray-600">R²</th>
                <th className="text-right px-3 py-2.5 font-medium text-gray-600">Маржа %</th>
                <th className="text-right px-3 py-2.5 font-medium text-gray-600">Прибыль 30д</th>
                <th className="text-right px-3 py-2.5 font-medium text-gray-600">Рек. цена</th>
                <th className="text-right px-3 py-2.5 font-medium text-gray-600">Потенциал</th>
                <th className="text-right px-3 py-2.5 font-medium text-gray-600">Оборач.</th>
                <th className="text-right px-3 py-2.5 font-medium text-gray-600">Остаток</th>
              </tr>
            </thead>
            <tbody>
              {dashboard.skus.map((s, i) => (
                <tr
                  key={s.sku_id}
                  onClick={() => setSelectedSku(s.sku_id)}
                  className={`cursor-pointer hover:bg-violet-50 transition ${i % 2 === 0 ? 'bg-white' : 'bg-gray-50'}`}
                >
                  <td className="px-3 py-2 font-medium text-gray-900">{s.seller_article}</td>
                  <td className="px-3 py-2 text-right">{fmtPrice(s.current_price)}</td>
                  <td className="px-3 py-2 text-right">{s.orders_day.toFixed(1)}</td>
                  <td className="px-3 py-2 text-center">
                    <span className={`px-2 py-0.5 rounded text-xs font-medium ${elasticityColor(s.elasticity)}`}>
                      {s.elasticity.toFixed(2)} {elasticityLabel(s.elasticity)}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-right text-gray-500">{s.r_squared.toFixed(2)}</td>
                  <td className="px-3 py-2 text-right">{s.margin_pct.toFixed(1)}%</td>
                  <td className="px-3 py-2 text-right font-medium">{fmt(s.net_profit_30d)}</td>
                  <td className="px-3 py-2 text-right text-violet-600 font-medium">{fmtPrice(s.optimal_price)}</td>
                  <td className="px-3 py-2 text-right">
                    <span className={s.profit_delta_pct > 0 ? 'text-emerald-600' : 'text-gray-500'}>
                      {s.profit_delta_pct > 0 ? '+' : ''}{s.profit_delta_pct.toFixed(1)}%
                    </span>
                  </td>
                  <td className="px-3 py-2 text-right text-gray-500">{s.turnover_days.toFixed(0)}д</td>
                  <td className="px-3 py-2 text-right text-gray-500">{s.stock}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function SkuDetailView({ skuId, channel, onBack }: { skuId: number; channel: string; onBack: () => void }) {
  const [sppPct, setSppPct] = useState<number | undefined>()
  const [sliderPrice, setSliderPrice] = useState<number | null>(null)

  const { data, isLoading } = useQuery({
    queryKey: ['elasticity-sku', skuId, channel, sppPct],
    queryFn: () => elasticityApi.sku(skuId, channel, sppPct).then(r => r.data as SkuDetail),
  })

  const forecastMut = useMutation({
    mutationFn: (price: number) => elasticityApi.forecast(skuId, price, channel, sppPct).then(r => r.data as Scenario),
  })

  const d = data
  const scatter = d?.elasticity?.scatter || []

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center gap-3">
        <button onClick={onBack} className="p-1 hover:bg-gray-100 rounded">
          <ArrowLeft size={20} />
        </button>
        <TrendingDown className="text-violet-600" size={24} />
        <div>
          <h1 className="text-2xl font-bold text-gray-900">{d?.seller_article || '...'}</h1>
          <p className="text-sm text-gray-500">{d?.name}</p>
        </div>
      </div>

      {isLoading && (
        <div className="flex items-center gap-2 text-gray-500 py-12 justify-center">
          <Loader2 className="animate-spin" size={20} />
          Расчёт...
        </div>
      )}

      {d && (
        <>
          {/* Summary Cards */}
          <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
            <Card label="Эластичность" value={d.elasticity.elasticity.toFixed(2)} sub={`R²=${d.elasticity.r_squared.toFixed(2)}`} color="violet" icon={<BarChart3 size={14} />} />
            <Card label="Цена до СПП" value={fmtPrice(d.unit_economics.avg_price)} sub={`СПП ${d.spp_pct}%`} color="blue" icon={<DollarSign size={14} />} />
            <Card label="Заказов/день" value={d.unit_economics.avg_orders_day.toFixed(1)} sub={`Себест. ${fmtPrice(d.unit_economics.cogs_per_unit)}`} color="emerald" />
            <Card label="Оптимальная цена" value={fmtPrice(d.optimal_price)} sub={`Прибыль ${fmt(d.optimal_profit)}`} color="amber" icon={<Target size={14} />} />
            <Card label="Остаток" value={String(d.stock)} sub="шт на складах" color="gray" />
          </div>

          {/* Scatter Plot */}
          {scatter.length > 2 && (
            <div className="bg-white rounded-lg border border-gray-200 p-4">
              <h3 className="text-sm font-medium text-gray-700 mb-3">Цена vs Заказы (7-дневные окна)</h3>
              <ResponsiveContainer width="100%" height={300}>
                <ComposedChart data={scatter}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                  <XAxis dataKey="price" name="Цена" tickFormatter={(v: number) => fmtPrice(v)} label={{ value: 'Цена после СПП, ₽', position: 'insideBottom', offset: -5, style: { fontSize: 11 } }} />
                  <YAxis dataKey="orders" name="Заказов/день" label={{ value: 'Заказов/день', angle: -90, position: 'insideLeft', style: { fontSize: 11 } }} />
                  <Tooltip formatter={(v: number) => v.toFixed(2)} labelFormatter={(v: number) => `Цена: ${fmtPrice(v)} ₽`} />
                  <Scatter dataKey="orders" fill="#7c3aed" fillOpacity={0.6} />
                </ComposedChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* Price Slider */}
          <div className="bg-white rounded-lg border border-gray-200 p-4">
            <h3 className="text-sm font-medium text-gray-700 mb-3">Прогноз при изменении цены</h3>
            <div className="flex items-center gap-4 flex-wrap">
              <div>
                <label className="block text-xs text-gray-500 mb-1">Цена до СПП</label>
                <input
                  type="range"
                  min={Math.round(d.unit_economics.avg_price * 0.7)}
                  max={Math.round(d.unit_economics.avg_price * 1.3)}
                  step={50}
                  value={sliderPrice ?? d.unit_economics.avg_price}
                  onChange={e => {
                    const p = Number(e.target.value)
                    setSliderPrice(p)
                    forecastMut.mutate(p)
                  }}
                  className="w-64"
                />
                <div className="text-lg font-bold text-violet-600 mt-1">{fmtPrice(sliderPrice ?? d.unit_economics.avg_price)} ₽</div>
              </div>
              <div>
                <label className="block text-xs text-gray-500 mb-1">СПП %</label>
                <input
                  type="number"
                  value={sppPct ?? d.spp_pct}
                  onChange={e => setSppPct(Number(e.target.value))}
                  className="border border-gray-300 rounded px-2 py-1 text-sm w-20"
                />
              </div>
              {forecastMut.data && (
                <div className="flex gap-6 text-sm ml-4">
                  <div><span className="text-gray-500">Заказов/день:</span> <strong>{forecastMut.data.orders_day.toFixed(1)}</strong></div>
                  <div><span className="text-gray-500">Выручка/мес:</span> <strong>{fmt(forecastMut.data.revenue)}</strong></div>
                  <div><span className="text-gray-500">Маржа:</span> <strong>{forecastMut.data.margin_pct.toFixed(1)}%</strong></div>
                  <div><span className="text-gray-500">Чист.прибыль:</span> <strong className="text-emerald-600">{fmt(forecastMut.data.net_profit)}</strong></div>
                  <div><span className="text-gray-500">Оборач.:</span> <strong>{forecastMut.data.turnover_days.toFixed(0)}д</strong></div>
                  <div><span className="text-gray-500">Оборотов/год:</span> <strong>{forecastMut.data.turns_per_year?.toFixed(1)}</strong></div>
                  <div><span className="text-gray-500">Прибыль/год:</span> <strong className="text-violet-600">{fmt(forecastMut.data.annual_profit || 0)}</strong></div>
                </div>
              )}
            </div>
          </div>

          {/* Scenarios Table */}
          <div className="bg-white rounded-lg border border-gray-200 overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200">
                  <th className="text-left px-3 py-2.5 font-medium text-gray-600">Сценарий</th>
                  <th className="text-right px-3 py-2.5 font-medium text-gray-600">Цена</th>
                  <th className="text-right px-3 py-2.5 font-medium text-gray-600">Заказов/мес</th>
                  <th className="text-right px-3 py-2.5 font-medium text-gray-600">Выручка</th>
                  <th className="text-right px-3 py-2.5 font-medium text-gray-600">Себест.</th>
                  <th className="text-right px-3 py-2.5 font-medium text-gray-600">Логистика</th>
                  <th className="text-right px-3 py-2.5 font-medium text-gray-600">Комиссия</th>
                  <th className="text-right px-3 py-2.5 font-medium text-gray-600">Маржа %</th>
                  <th className="text-right px-3 py-2.5 font-medium text-gray-600">Прибыль/мес</th>
                  <th className="text-right px-3 py-2.5 font-medium text-gray-600">Оборач.</th>
                  <th className="text-right px-3 py-2.5 font-medium text-gray-600">Оборотов/год</th>
                  <th className="text-right px-3 py-2.5 font-medium text-gray-600">Прибыль/год</th>
                  <th className="text-right px-3 py-2.5 font-medium text-gray-600">ROI год</th>
                </tr>
              </thead>
              <tbody>
                {d.scenarios.map((s, i) => {
                  const isOptimal = s.label === 'Оптимум'
                  const isCurrent = s.label === 'Текущая'
                  return (
                    <tr key={i} className={`${isOptimal ? 'bg-violet-50 font-medium' : isCurrent ? 'bg-blue-50' : i % 2 === 0 ? 'bg-white' : 'bg-gray-50'}`}>
                      <td className="px-3 py-2 font-medium">{s.label}</td>
                      <td className="px-3 py-2 text-right">{fmtPrice(s.price)}</td>
                      <td className="px-3 py-2 text-right">{s.orders_total}</td>
                      <td className="px-3 py-2 text-right">{fmt(s.revenue)}</td>
                      <td className="px-3 py-2 text-right text-gray-500">{fmt(s.cogs)}</td>
                      <td className="px-3 py-2 text-right text-gray-500">{fmt(s.logistics)}</td>
                      <td className="px-3 py-2 text-right text-gray-500">{fmt(s.commission)}</td>
                      <td className="px-3 py-2 text-right">{s.margin_pct.toFixed(1)}%</td>
                      <td className={`px-3 py-2 text-right font-medium ${s.net_profit > 0 ? 'text-emerald-600' : 'text-red-600'}`}>{fmt(s.net_profit)}</td>
                      <td className="px-3 py-2 text-right text-gray-500">{s.turnover_days.toFixed(0)}д</td>
                      <td className="px-3 py-2 text-right font-medium">{s.turns_per_year?.toFixed(1) || '—'}</td>
                      <td className={`px-3 py-2 text-right font-medium ${(s.annual_profit || 0) > 0 ? 'text-emerald-600' : 'text-red-600'}`}>{fmt(s.annual_profit || 0)}</td>
                      <td className="px-3 py-2 text-right">{s.annual_roi_pct?.toFixed(0) || 0}%</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  )
}

function Card({ label, value, sub, color, icon }: { label: string; value: string; sub?: string; color: string; icon?: React.ReactNode }) {
  const colors: Record<string, string> = {
    violet: 'bg-violet-50 text-violet-700 border-violet-200',
    blue: 'bg-blue-50 text-blue-700 border-blue-200',
    emerald: 'bg-emerald-50 text-emerald-700 border-emerald-200',
    amber: 'bg-amber-50 text-amber-700 border-amber-200',
    gray: 'bg-gray-50 text-gray-700 border-gray-200',
  }
  return (
    <div className={`rounded-lg border p-3 ${colors[color] || colors.gray}`}>
      <div className="flex items-center gap-1.5 text-xs opacity-75 mb-1">{icon}{label}</div>
      <div className="text-xl font-bold">{value}</div>
      {sub && <div className="text-xs opacity-60 mt-0.5">{sub}</div>}
    </div>
  )
}
