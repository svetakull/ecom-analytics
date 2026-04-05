import { useState, useCallback, useRef } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/api/client'
import {
  Download, Upload, Search, ChevronDown, ChevronUp,
  Trash2, Plus, Save, X, FileSpreadsheet, AlertCircle, CheckCircle2,
} from 'lucide-react'
import clsx from 'clsx'

/* ─── helpers ─── */
const fmtNum = (n: number, d = 2) =>
  n.toLocaleString('ru-RU', { minimumFractionDigits: d, maximumFractionDigits: d })

const fmtDate = (iso: string) => {
  if (!iso) return '—'
  const [y, m, d] = iso.slice(0, 10).split('-')
  return `${d}.${m}.${y}`
}

/* ─── types ─── */
interface CostPriceEntry {
  id: number
  sku_id: number
  channel_id: number
  seller_article: string
  marketplace_article: string
  marketplace: string
  size: string | null
  cost_price: number
  fulfillment: number
  vat_rate: number
  effective_from: string | null
  is_default: boolean
}

interface CostPriceGroup {
  sku_id: number
  channel_id: number
  seller_article: string
  mp_article: string
  marketplace: string
  size: string | null
  default_cost_price: number
  default_fulfillment: number
  default_vat_rate: number
  history_count: number
  entries: CostPriceEntry[]
}

type Marketplace = 'wb' | 'ozon' | 'lamoda'
const MP_LABELS: Record<Marketplace, string> = { wb: 'WB', ozon: 'Ozon', lamoda: 'Lamoda' }

/* ─── page ─── */
export default function CostPricePage() {
  const queryClient = useQueryClient()
  const [marketplace, setMarketplace] = useState<Marketplace>('wb')
  const [search, setSearch] = useState('')
  const [expandedKey, setExpandedKey] = useState<string | null>(null)
  const [importOpen, setImportOpen] = useState(false)
  const [filterNoCost, setFilterNoCost] = useState(false)
  const [filterNoFulfill, setFilterNoFulfill] = useState(false)
  const [filterNoVat, setFilterNoVat] = useState(false)

  /* ── query ── */
  const { data: rows = [], isLoading, isError } = useQuery<CostPriceGroup[]>({
    queryKey: ['cost-prices', marketplace, search],
    queryFn: async () => {
      const { data } = await api.get<CostPriceEntry[]>('/cost-prices', {
        params: { marketplace, ...(search ? { article: search } : {}) },
      })
      // Группируем плоский список по (sku_id, channel_id, size)
      const map = new Map<string, CostPriceGroup>()
      for (const e of data) {
        const key = `${e.sku_id}-${e.channel_id}-${e.size ?? ''}`
        if (!map.has(key)) {
          map.set(key, {
            sku_id: e.sku_id,
            channel_id: e.channel_id,
            seller_article: e.seller_article,
            mp_article: e.marketplace_article || '',
            marketplace: e.marketplace,
            size: e.size,
            default_cost_price: 0,
            default_fulfillment: 0,
            default_vat_rate: 0,
            history_count: 0,
            entries: [],
          })
        }
        const g = map.get(key)!
        g.entries.push(e)
        if (e.is_default) {
          g.default_cost_price = e.cost_price
          g.default_fulfillment = e.fulfillment
          g.default_vat_rate = e.vat_rate
        } else {
          g.history_count++
        }
      }
      return Array.from(map.values())
    },
    staleTime: 30_000,
  })

  /* ── export ── */
  const handleExport = useCallback(async () => {
    const resp = await api.get('/cost-prices/export', {
      params: { marketplace },
      responseType: 'blob',
    })
    const url = window.URL.createObjectURL(resp.data)
    const a = document.createElement('a')
    a.href = url
    a.download = `cost_prices_${marketplace}.xlsx`
    a.click()
    window.URL.revokeObjectURL(url)
  }, [marketplace])

  const rowKey = (g: CostPriceGroup) => `${g.sku_id}-${g.channel_id}-${g.size ?? ''}`

  const filteredRows = rows.filter((g) => {
    if (filterNoCost && g.default_cost_price > 0) return false
    if (filterNoFulfill && g.default_fulfillment > 0) return false
    if (filterNoVat && g.default_vat_rate > 0) return false
    return true
  })

  const invalidate = () => queryClient.invalidateQueries({ queryKey: ['cost-prices'] })

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between gap-4 mb-4 flex-wrap">
        <h1 className="text-xl font-bold text-gray-900">Себестоимость</h1>
        <div className="flex items-center gap-2">
          <button
            onClick={handleExport}
            className="flex items-center gap-1.5 px-3 py-1.5 border border-gray-200 bg-white text-sm text-gray-700 rounded-lg hover:bg-gray-50 transition-colors"
          >
            <Download size={15} />
            Экспорт Excel
          </button>
          <button
            onClick={() => setImportOpen(true)}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors"
          >
            <Upload size={15} />
            Импорт Excel
          </button>
        </div>
      </div>

      {/* Filters */}
      <div className="flex items-center gap-3 mb-4 flex-wrap">
        {/* Marketplace buttons */}
        <div className="flex rounded-lg border border-gray-200 overflow-hidden">
          {(Object.keys(MP_LABELS) as Marketplace[]).map((mp) => (
            <button
              key={mp}
              onClick={() => { setMarketplace(mp); setExpandedKey(null) }}
              className={clsx(
                'px-4 py-1.5 text-sm font-medium transition-colors',
                marketplace === mp
                  ? 'bg-blue-600 text-white'
                  : 'bg-white text-gray-600 hover:bg-gray-50'
              )}
            >
              {MP_LABELS[mp]}
            </button>
          ))}
        </div>

        {/* Search */}
        <div className="relative">
          <Search size={15} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
          <input
            type="text"
            placeholder="Поиск по артикулу..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="pl-9 pr-3 py-1.5 border border-gray-200 rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500/30 w-64"
          />
        </div>

        {/* Not-filled filters */}
        <div className="flex items-center gap-3 ml-1">
          <label className="flex items-center gap-1.5 text-sm text-gray-700 cursor-pointer">
            <input
              type="checkbox"
              checked={filterNoCost}
              onChange={(e) => setFilterNoCost(e.target.checked)}
              className="accent-blue-600"
            />
            Себестоимость не заполнена
          </label>
          <label className="flex items-center gap-1.5 text-sm text-gray-700 cursor-pointer">
            <input
              type="checkbox"
              checked={filterNoFulfill}
              onChange={(e) => setFilterNoFulfill(e.target.checked)}
              className="accent-blue-600"
            />
            Фулфилмент не заполнен
          </label>
          <label className="flex items-center gap-1.5 text-sm text-gray-700 cursor-pointer">
            <input
              type="checkbox"
              checked={filterNoVat}
              onChange={(e) => setFilterNoVat(e.target.checked)}
              className="accent-blue-600"
            />
            НДС не заполнен
          </label>
        </div>
      </div>

      {/* Table */}
      <div className="border border-gray-200 bg-white rounded-xl">
        {isLoading && (
          <div className="flex items-center justify-center h-40 text-gray-400">Загрузка...</div>
        )}
        {isError && (
          <div className="flex items-center justify-center h-40 text-red-500">Ошибка загрузки</div>
        )}
        {!isLoading && !isError && (
          <table className="w-full text-sm" style={{ borderCollapse: 'separate', borderSpacing: 0 }}>
            <thead>
              <tr className="bg-gray-50">
                <th className="text-left px-4 py-3 font-medium text-gray-600 border-b w-8" />
                <th className="text-left px-3 py-3 font-medium text-gray-600 border-b">Артикул продавца</th>
                <th className="text-left px-3 py-3 font-medium text-gray-600 border-b">Артикул МП</th>
                <th className="text-left px-3 py-3 font-medium text-gray-600 border-b">МП</th>
                <th className="text-left px-3 py-3 font-medium text-gray-600 border-b">Размер</th>
                <th className="text-right px-3 py-3 font-medium text-gray-600 border-b">Себестоим.</th>
                <th className="text-right px-3 py-3 font-medium text-gray-600 border-b">Фулфилмент</th>
                <th className="text-right px-3 py-3 font-medium text-gray-600 border-b">НДС %</th>
                <th className="text-center px-3 py-3 font-medium text-gray-600 border-b">Записей</th>
              </tr>
            </thead>
            <tbody>
              {filteredRows.length === 0 && (
                <tr>
                  <td colSpan={9} className="text-center py-12 text-gray-400">
                    Нет данных о себестоимости
                  </td>
                </tr>
              )}
              {filteredRows.map((group, i) => {
                const key = rowKey(group)
                const isOpen = expandedKey === key
                return (
                  <GroupRow
                    key={key}
                    group={group}
                    index={i}
                    isOpen={isOpen}
                    onToggle={() => setExpandedKey(isOpen ? null : key)}
                    onInvalidate={invalidate}
                  />
                )
              })}
            </tbody>
          </table>
        )}
      </div>

      {/* Import modal */}
      {importOpen && (
        <ImportModal
          marketplace={marketplace}
          onClose={() => setImportOpen(false)}
          onSuccess={() => { setImportOpen(false); invalidate() }}
        />
      )}
    </div>
  )
}

/* ═══════════════════════════════════════════════
   GroupRow — one row in the table + accordion
   ═══════════════════════════════════════════════ */
interface GroupRowProps {
  group: CostPriceGroup
  index: number
  isOpen: boolean
  onToggle: () => void
  onInvalidate: () => void
}

function GroupRow({ group, index, isOpen, onToggle, onInvalidate }: GroupRowProps) {
  const queryClient = useQueryClient()

  /* ── inline editing state for default values ── */
  const [editingDefaults, setEditingDefaults] = useState(false)
  const [defCost, setDefCost] = useState(group.default_cost_price)
  const [defFulfill, setDefFulfill] = useState(group.default_fulfillment)
  const [defVat, setDefVat] = useState(group.default_vat_rate)

  /* ── new history entry state ── */
  const [showAdd, setShowAdd] = useState(false)
  const [newDate, setNewDate] = useState('')
  const [newCost, setNewCost] = useState<number | ''>('')
  const [newFulfill, setNewFulfill] = useState<number | ''>('')
  const [newVat, setNewVat] = useState<number | ''>('')

  /* ── mutations ── */
  const updateMutation = useMutation({
    mutationFn: (payload: { id: number; cost_price?: number; fulfillment?: number; vat_rate?: number }) => {
      const { id, ...body } = payload
      return api.put(`/cost-prices/${id}`, body)
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['cost-prices'] })
      setEditingDefaults(false)
    },
  })

  const createMutation = useMutation({
    mutationFn: (body: Record<string, unknown>) => api.post('/cost-prices', body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['cost-prices'] })
      setShowAdd(false)
      setNewDate('')
      setNewCost('')
      setNewFulfill('')
      setNewVat('')
    },
  })

  const deleteMutation = useMutation({
    mutationFn: (id: number) => api.delete(`/cost-prices/${id}`),
    onSuccess: () => onInvalidate(),
  })

  const handleSaveDefaults = () => {
    const defaultEntry = group.entries.find((e) => e.is_default)
    if (!defaultEntry) return
    updateMutation.mutate({
      id: defaultEntry.id,
      cost_price: defCost,
      fulfillment: defFulfill,
      vat_rate: defVat,
    })
  }

  const handleAddEntry = () => {
    if (newCost === '' && newFulfill === '' && newVat === '') return
    createMutation.mutate({
      sku_id: group.sku_id,
      channel_id: group.channel_id,
      cost_price: newCost || group.default_cost_price,
      fulfillment: newFulfill || group.default_fulfillment,
      vat_rate: newVat || group.default_vat_rate,
      effective_from: newDate || undefined,
      size: group.size || undefined,
    })
  }

  const handleDelete = (id: number) => {
    if (window.confirm('Удалить запись?')) {
      deleteMutation.mutate(id)
    }
  }

  const historyEntries = group.entries.filter((e) => !e.is_default)

  return (
    <>
      {/* Main row */}
      <tr
        onClick={onToggle}
        className={clsx(
          'border-b border-gray-100 hover:bg-gray-50/50 transition-colors cursor-pointer',
          index % 2 !== 0 && 'bg-[#fafbfc]'
        )}
      >
        <td className="px-4 py-2.5 text-gray-400">
          {isOpen ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
        </td>
        <td className="px-3 py-2.5 text-gray-900 font-medium">{group.seller_article}</td>
        <td className="px-3 py-2.5 text-gray-600">{group.mp_article || '—'}</td>
        <td className="px-3 py-2.5">
          <span className="inline-block px-2 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-700">
            {group.marketplace.toUpperCase()}
          </span>
        </td>
        <td className="px-3 py-2.5 text-gray-600">{group.size || '—'}</td>
        <td className="px-3 py-2.5 text-right tabular-nums text-gray-900">{fmtNum(group.default_cost_price)} &#8381;</td>
        <td className="px-3 py-2.5 text-right tabular-nums text-gray-900">{fmtNum(group.default_fulfillment)} &#8381;</td>
        <td className="px-3 py-2.5 text-right tabular-nums text-gray-900">{fmtNum(group.default_vat_rate, 0)}%</td>
        <td className="px-3 py-2.5 text-center text-gray-500">{group.history_count}</td>
      </tr>

      {/* Expanded accordion */}
      {isOpen && (
        <tr>
          <td colSpan={9} className="p-0">
            <div className="bg-gray-50/70 border-b border-gray-200 px-6 py-4">
              {/* Default values */}
              <div className="mb-4">
                <div className="flex items-center gap-2 mb-2">
                  <h3 className="text-sm font-semibold text-gray-700">Значения по умолчанию</h3>
                  {!editingDefaults && (
                    <button
                      onClick={(e) => { e.stopPropagation(); setEditingDefaults(true) }}
                      className="text-xs text-blue-600 hover:text-blue-700"
                    >
                      Изменить
                    </button>
                  )}
                </div>

                {editingDefaults ? (
                  <div className="flex items-end gap-3 flex-wrap">
                    <label className="text-xs text-gray-500">
                      Себестоимость
                      <input
                        type="number"
                        step="0.01"
                        value={defCost}
                        onChange={(e) => setDefCost(+e.target.value)}
                        className="block mt-0.5 w-32 border border-gray-300 rounded-lg px-2.5 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                      />
                    </label>
                    <label className="text-xs text-gray-500">
                      Фулфилмент
                      <input
                        type="number"
                        step="0.01"
                        value={defFulfill}
                        onChange={(e) => setDefFulfill(+e.target.value)}
                        className="block mt-0.5 w-32 border border-gray-300 rounded-lg px-2.5 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                      />
                    </label>
                    <label className="text-xs text-gray-500">
                      НДС %
                      <input
                        type="number"
                        step="1"
                        value={defVat}
                        onChange={(e) => setDefVat(+e.target.value)}
                        className="block mt-0.5 w-24 border border-gray-300 rounded-lg px-2.5 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                      />
                    </label>
                    <div className="flex items-center gap-1.5">
                      <button
                        onClick={handleSaveDefaults}
                        disabled={updateMutation.isPending}
                        className="flex items-center gap-1 px-3 py-1.5 bg-emerald-600 text-white text-sm rounded-lg hover:bg-emerald-700 transition-colors disabled:opacity-50"
                      >
                        <Save size={14} />
                        Сохранить
                      </button>
                      <button
                        onClick={() => {
                          setEditingDefaults(false)
                          setDefCost(group.default_cost_price)
                          setDefFulfill(group.default_fulfillment)
                          setDefVat(group.default_vat_rate)
                        }}
                        className="flex items-center gap-1 px-3 py-1.5 border border-gray-200 bg-white text-sm text-gray-600 rounded-lg hover:bg-gray-50 transition-colors"
                      >
                        <X size={14} />
                        Отмена
                      </button>
                    </div>
                  </div>
                ) : (
                  <div className="flex items-center gap-6 text-sm">
                    <span className="text-gray-500">
                      Себестоимость: <span className="text-gray-900 font-medium">{fmtNum(group.default_cost_price)} &#8381;</span>
                    </span>
                    <span className="text-gray-500">
                      Фулфилмент: <span className="text-gray-900 font-medium">{fmtNum(group.default_fulfillment)} &#8381;</span>
                    </span>
                    <span className="text-gray-500">
                      НДС: <span className="text-gray-900 font-medium">{fmtNum(group.default_vat_rate, 0)}%</span>
                    </span>
                  </div>
                )}
              </div>

              {/* History */}
              <div>
                <h3 className="text-sm font-semibold text-gray-700 mb-2">
                  История изменений ({historyEntries.length})
                </h3>

                {historyEntries.length > 0 ? (
                  <div className="border border-gray-200 rounded-lg bg-white mb-3">
                    <table className="w-full text-sm" style={{ borderCollapse: 'separate', borderSpacing: 0 }}>
                      <thead>
                        <tr className="bg-gray-50">
                          <th className="text-left px-3 py-2 font-medium text-gray-600 border-b text-xs">Дата</th>
                          <th className="text-right px-3 py-2 font-medium text-gray-600 border-b text-xs">Себестоимость</th>
                          <th className="text-right px-3 py-2 font-medium text-gray-600 border-b text-xs">Фулфилмент</th>
                          <th className="text-right px-3 py-2 font-medium text-gray-600 border-b text-xs">НДС %</th>
                          <th className="text-center px-3 py-2 font-medium text-gray-600 border-b text-xs w-16" />
                        </tr>
                      </thead>
                      <tbody>
                        {historyEntries.map((entry) => (
                          <tr key={entry.id} className="border-b border-gray-100 last:border-b-0 hover:bg-gray-50/50">
                            <td className="px-3 py-2 text-gray-700">{fmtDate(entry.effective_from ?? '')}</td>
                            <td className="px-3 py-2 text-right tabular-nums">{fmtNum(entry.cost_price)} &#8381;</td>
                            <td className="px-3 py-2 text-right tabular-nums">{fmtNum(entry.fulfillment)} &#8381;</td>
                            <td className="px-3 py-2 text-right tabular-nums">{fmtNum(entry.vat_rate, 0)}%</td>
                            <td className="px-3 py-2 text-center">
                              <button
                                onClick={() => handleDelete(entry.id)}
                                className="p-1 text-gray-400 hover:text-red-600 transition-colors rounded"
                                title="Удалить"
                              >
                                <Trash2 size={14} />
                              </button>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <p className="text-sm text-gray-400 mb-3">Нет записей в истории</p>
                )}

                {/* Add new history entry */}
                {showAdd ? (
                  <div className="flex items-end gap-3 flex-wrap border border-gray-200 rounded-lg bg-white p-3">
                    <label className="text-xs text-gray-500">
                      Дата
                      <input
                        type="date"
                        value={newDate}
                        onChange={(e) => setNewDate(e.target.value)}
                        className="block mt-0.5 w-36 border border-gray-300 rounded-lg px-2.5 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                      />
                    </label>
                    <label className="text-xs text-gray-500">
                      Себестоимость
                      <input
                        type="number"
                        step="0.01"
                        placeholder={fmtNum(group.default_cost_price)}
                        value={newCost}
                        onChange={(e) => setNewCost(e.target.value === '' ? '' : +e.target.value)}
                        className="block mt-0.5 w-32 border border-gray-300 rounded-lg px-2.5 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                      />
                    </label>
                    <label className="text-xs text-gray-500">
                      Фулфилмент
                      <input
                        type="number"
                        step="0.01"
                        placeholder={fmtNum(group.default_fulfillment)}
                        value={newFulfill}
                        onChange={(e) => setNewFulfill(e.target.value === '' ? '' : +e.target.value)}
                        className="block mt-0.5 w-32 border border-gray-300 rounded-lg px-2.5 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                      />
                    </label>
                    <label className="text-xs text-gray-500">
                      НДС %
                      <input
                        type="number"
                        step="1"
                        placeholder={fmtNum(group.default_vat_rate, 0)}
                        value={newVat}
                        onChange={(e) => setNewVat(e.target.value === '' ? '' : +e.target.value)}
                        className="block mt-0.5 w-24 border border-gray-300 rounded-lg px-2.5 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                      />
                    </label>
                    <div className="flex items-center gap-1.5">
                      <button
                        onClick={handleAddEntry}
                        disabled={createMutation.isPending}
                        className="flex items-center gap-1 px-3 py-1.5 bg-emerald-600 text-white text-sm rounded-lg hover:bg-emerald-700 transition-colors disabled:opacity-50"
                      >
                        <Save size={14} />
                        Сохранить
                      </button>
                      <button
                        onClick={() => setShowAdd(false)}
                        className="flex items-center gap-1 px-3 py-1.5 border border-gray-200 bg-white text-sm text-gray-600 rounded-lg hover:bg-gray-50 transition-colors"
                      >
                        <X size={14} />
                        Отмена
                      </button>
                    </div>
                  </div>
                ) : (
                  <button
                    onClick={() => setShowAdd(true)}
                    className="flex items-center gap-1.5 text-sm text-blue-600 hover:text-blue-700 transition-colors"
                  >
                    <Plus size={14} />
                    Добавить изменение
                  </button>
                )}
              </div>

              {/* Mutation errors */}
              {(updateMutation.isError || createMutation.isError || deleteMutation.isError) && (
                <div className="mt-3 flex items-center gap-2 text-sm text-red-600">
                  <AlertCircle size={14} />
                  {deleteMutation.isError && (deleteMutation.error as any)?.response?.status === 422
                    ? 'Нельзя удалить запись по умолчанию'
                    : 'Ошибка сохранения'}
                </div>
              )}
            </div>
          </td>
        </tr>
      )}
    </>
  )
}

/* ═══════════════════════════════════════════════
   Import Modal
   ═══════════════════════════════════════════════ */
interface ImportModalProps {
  marketplace: string
  onClose: () => void
  onSuccess: () => void
}

interface ImportResult {
  created: number
  updated: number
  errors: string[]
}

function ImportModal({ marketplace, onClose, onSuccess }: ImportModalProps) {
  const [file, setFile] = useState<File | null>(null)
  const [mode, setMode] = useState<'update' | 'overwrite'>('update')
  const [dragOver, setDragOver] = useState(false)
  const [result, setResult] = useState<ImportResult | null>(null)
  const inputRef = useRef<HTMLInputElement>(null)

  const importMutation = useMutation({
    mutationFn: async () => {
      if (!file) return
      const formData = new FormData()
      formData.append('file', file)
      formData.append('mode', mode)
      const resp = await api.post('/cost-prices/import', formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
        params: { marketplace },
      })
      return resp.data as ImportResult
    },
    onSuccess: (data) => {
      if (data) setResult(data)
    },
  })

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault()
    setDragOver(false)
    const f = e.dataTransfer.files[0]
    if (f && (f.name.endsWith('.xlsx') || f.name.endsWith('.xls'))) {
      setFile(f)
    }
  }

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files?.[0]) setFile(e.target.files[0])
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40" onClick={onClose}>
      <div
        className="bg-white rounded-xl shadow-xl w-full max-w-lg mx-4"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Modal header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-200">
          <h2 className="text-lg font-semibold text-gray-900">Импорт себестоимости</h2>
          <button onClick={onClose} className="p-1 text-gray-400 hover:text-gray-600 transition-colors">
            <X size={18} />
          </button>
        </div>

        <div className="px-5 py-4">
          {result ? (
            /* Result report */
            <div>
              <div className="flex items-center gap-2 mb-3">
                <CheckCircle2 size={20} className="text-emerald-600" />
                <span className="text-sm font-medium text-gray-900">Импорт завершён</span>
              </div>
              <div className="text-sm text-gray-600 space-y-1 mb-4">
                <p>Создано: <span className="font-medium text-gray-900">{result.created}</span></p>
                <p>Обновлено: <span className="font-medium text-gray-900">{result.updated}</span></p>
                {result.errors.length > 0 && (
                  <div className="mt-2">
                    <p className="text-red-600 font-medium">Ошибки ({result.errors.length}):</p>
                    <ul className="list-disc pl-5 mt-1 text-red-600 text-xs max-h-32 overflow-y-auto">
                      {result.errors.map((err, i) => (
                        <li key={i}>{err}</li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
              <button
                onClick={onSuccess}
                className="w-full px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors"
              >
                Закрыть
              </button>
            </div>
          ) : (
            <>
              {/* Drop zone */}
              <div
                onDragOver={(e) => { e.preventDefault(); setDragOver(true) }}
                onDragLeave={() => setDragOver(false)}
                onDrop={handleDrop}
                onClick={() => inputRef.current?.click()}
                className={clsx(
                  'border-2 border-dashed rounded-xl px-6 py-10 text-center cursor-pointer transition-colors mb-4',
                  dragOver
                    ? 'border-blue-400 bg-blue-50'
                    : file
                      ? 'border-emerald-300 bg-emerald-50/50'
                      : 'border-gray-300 hover:border-gray-400 bg-gray-50'
                )}
              >
                <input
                  ref={inputRef}
                  type="file"
                  accept=".xlsx,.xls"
                  onChange={handleFileChange}
                  className="hidden"
                />
                {file ? (
                  <div className="flex items-center justify-center gap-2">
                    <FileSpreadsheet size={20} className="text-emerald-600" />
                    <span className="text-sm text-gray-700 font-medium">{file.name}</span>
                  </div>
                ) : (
                  <>
                    <Upload size={28} className="mx-auto text-gray-400 mb-2" />
                    <p className="text-sm text-gray-500">Перетащите .xlsx файл или нажмите для выбора</p>
                  </>
                )}
              </div>

              {/* Mode */}
              <div className="mb-4">
                <p className="text-xs font-medium text-gray-500 mb-2">Режим импорта</p>
                <div className="flex gap-3">
                  <label className="flex items-center gap-2 text-sm text-gray-700 cursor-pointer">
                    <input
                      type="radio"
                      name="import-mode"
                      checked={mode === 'update'}
                      onChange={() => setMode('update')}
                      className="accent-blue-600"
                    />
                    Обновить существующие
                  </label>
                  <label className="flex items-center gap-2 text-sm text-gray-700 cursor-pointer">
                    <input
                      type="radio"
                      name="import-mode"
                      checked={mode === 'overwrite'}
                      onChange={() => setMode('overwrite')}
                      className="accent-blue-600"
                    />
                    Перезаписать все
                  </label>
                </div>
              </div>

              {/* Error */}
              {importMutation.isError && (
                <div className="flex items-center gap-2 text-sm text-red-600 mb-3">
                  <AlertCircle size={14} />
                  Ошибка импорта. Проверьте формат файла.
                </div>
              )}

              {/* Actions */}
              <div className="flex justify-end gap-2">
                <button
                  onClick={onClose}
                  className="px-4 py-2 border border-gray-200 bg-white text-sm text-gray-600 rounded-lg hover:bg-gray-50 transition-colors"
                >
                  Отмена
                </button>
                <button
                  onClick={() => importMutation.mutate()}
                  disabled={!file || importMutation.isPending}
                  className="px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50"
                >
                  {importMutation.isPending ? 'Загрузка...' : 'Импортировать'}
                </button>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  )
}
