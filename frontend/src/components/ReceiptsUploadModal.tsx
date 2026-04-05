import { useState, useCallback, useRef } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/api/client'
import { X, Upload, Camera, AlertCircle } from 'lucide-react'
import clsx from 'clsx'
import { groupCategoriesForSelect } from '@/utils/categoryGroups'

interface PreviewRow {
  row_index: number
  filename: string
  date: string
  amount: number
  counterparty: string
  description: string
  category: string
  auto_classified: boolean
  entry_type: string
  bank: string
  error?: string
}

interface Props {
  open: boolean
  onClose: () => void
}

const fmt = (n: number) =>
  n.toLocaleString('ru-RU', { minimumFractionDigits: 2, maximumFractionDigits: 2 })

export default function ReceiptsUploadModal({ open, onClose }: Props) {
  const [dragActive, setDragActive] = useState(false)
  const [rows, setRows] = useState<PreviewRow[] | null>(null)
  const [editedCategories, setEditedCategories] = useState<Record<number, string>>({})
  const [skippedRows, setSkippedRows] = useState<Set<number>>(new Set())
  const [error, setError] = useState('')
  const fileInputRef = useRef<HTMLInputElement>(null)
  const queryClient = useQueryClient()

  // Категории для выпадашки
  const { data: categories = [] } = useQuery<{ key: string; name: string }[]>({
    queryKey: ['journal-categories'],
    queryFn: () => api.get('/journal/categories').then((r) => r.data),
    enabled: open,
  })

  const uploadMutation = useMutation({
    mutationFn: async (files: File[]) => {
      const formData = new FormData()
      files.forEach((f) => formData.append('files', f))
      const resp = await api.post('/journal/upload-receipts', formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
      })
      const raw = resp.data
      const mapped: PreviewRow[] = (raw.entries || []).map((e: any, i: number) => ({
        row_index: i,
        filename: e.filename || '',
        date: e.date || '',
        amount: e.amount || 0,
        counterparty: e.counterparty || '',
        description: e.description || '',
        category: e.auto_category || 'other',
        auto_classified: !!(e.auto_category && e.auto_category !== 'other'),
        entry_type: e.entry_type || 'expense',
        bank: e.bank || 'unknown',
        error: e.error,
      }))
      return mapped
    },
    onSuccess: (data) => {
      setRows(data)
      setEditedCategories({})
      setSkippedRows(new Set())
      setError('')
    },
    onError: () => {
      setError('Ошибка распознавания. Проверьте качество изображений.')
    },
  })

  const confirmMutation = useMutation({
    mutationFn: async () => {
      if (!rows) return Promise.reject()
      const accountName = 'Чеки'
      const today = new Date().toISOString().slice(0, 10)
      const valid = rows.filter((row) => !skippedRows.has(row.row_index) && !row.error && row.amount > 0)
      const skippedNoAmount = rows.filter(r => !skippedRows.has(r.row_index) && !r.error && !(r.amount > 0)).length
      const entries = valid.map((row) => ({
        entry_type: row.entry_type || 'expense',
        amount: Math.abs(row.amount),
        date: row.date || today,
        category: (editedCategories[row.row_index] ?? row.category) || 'other',
        counterparty: row.counterparty,
        description: row.description,
        account_name: accountName,
      }))
      if (entries.length === 0) {
        return Promise.reject(new Error(
          skippedNoAmount > 0
            ? `Нет чеков с распознанной суммой (пропущено: ${skippedNoAmount})`
            : 'Нет валидных чеков для импорта'
        ))
      }
      return api.post('/journal/upload-confirm', { entries, account_name: accountName })
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['journal'] })
      handleClose()
    },
    onError: (err: any) => {
      const detail = err?.response?.data?.detail || err?.message || 'Ошибка импорта. Попробуйте снова.'
      setError(typeof detail === 'string' ? detail : JSON.stringify(detail))
    },
  })

  const handleClose = () => {
    setRows(null)
    setEditedCategories({})
    setSkippedRows(new Set())
    setError('')
    onClose()
  }

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault()
      setDragActive(false)
      const files = Array.from(e.dataTransfer.files)
      if (files.length > 0) {
        uploadMutation.mutate(files)
      }
    },
    [uploadMutation]
  )

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || [])
    if (files.length > 0) {
      uploadMutation.mutate(files)
    }
  }

  const updateCategory = (rowIndex: number, category: string) => {
    setEditedCategories((prev) => ({ ...prev, [rowIndex]: category }))
  }

  if (!open) return null

  const validCount = rows
    ? rows.filter((r) => !skippedRows.has(r.row_index) && !r.error && r.amount > 0).length
    : 0
  const autoCount = rows ? rows.filter((r) => r.auto_classified).length : 0

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-6xl max-h-[90vh] flex flex-col mx-4">
        {/* Header */}
        <div className="flex items-center justify-between px-6 pt-5 pb-3 shrink-0">
          <h2 className="text-lg font-bold text-gray-900">Загрузить чеки</h2>
          <button onClick={handleClose} className="text-gray-400 hover:text-gray-600">
            <X size={20} />
          </button>
        </div>

        <div className="px-6 pb-6 flex-1 overflow-y-auto">
          {/* Error */}
          {error && (
            <div className="flex items-center gap-2 text-sm text-red-600 bg-red-50 rounded-lg px-3 py-2 mb-4">
              <AlertCircle size={16} />
              {error}
            </div>
          )}

          {/* Upload zone */}
          {!rows && (
            <div
              onDragOver={(e) => {
                e.preventDefault()
                setDragActive(true)
              }}
              onDragLeave={() => setDragActive(false)}
              onDrop={handleDrop}
              onClick={() => fileInputRef.current?.click()}
              className={clsx(
                'border-2 border-dashed rounded-xl p-12 text-center cursor-pointer transition-colors',
                dragActive
                  ? 'border-blue-400 bg-blue-50'
                  : 'border-gray-300 bg-gray-50 hover:border-gray-400 hover:bg-gray-100'
              )}
            >
              <input
                ref={fileInputRef}
                type="file"
                accept=".png,.jpg,.jpeg,.heic,.webp"
                multiple
                onChange={handleFileSelect}
                className="hidden"
              />
              {uploadMutation.isPending ? (
                <div className="text-gray-500">
                  <Camera size={40} className="mx-auto mb-3 text-blue-400 animate-pulse" />
                  <p className="text-sm font-medium">Распознавание чеков...</p>
                </div>
              ) : (
                <div className="text-gray-500">
                  <Upload size={40} className="mx-auto mb-3 text-gray-400" />
                  <p className="text-sm font-medium">
                    Перетащите фото чеков сюда или нажмите для выбора
                  </p>
                  <p className="text-xs text-gray-400 mt-1">
                    Поддерживаются .png .jpg .jpeg .heic .webp (можно несколько)
                  </p>
                </div>
              )}
            </div>
          )}

          {/* Preview table */}
          {rows && (
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <div className="text-sm text-gray-600">
                  Загружено чеков: <span className="font-medium">{rows.length}</span>
                  {' | '}
                  Авто-распознано:{' '}
                  <span className="font-medium text-emerald-600">{autoCount}</span>
                </div>
              </div>

              <div className="border border-gray-200 rounded-xl overflow-hidden">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-gray-50">
                      <th className="text-center px-2 py-2 font-medium text-gray-600 w-12">
                        <span title="Пропустить">✕</span>
                      </th>
                      <th className="text-left px-3 py-2 font-medium text-gray-600">Файл</th>
                      <th className="text-left px-3 py-2 font-medium text-gray-600">Дата</th>
                      <th className="text-right px-3 py-2 font-medium text-gray-600">Сумма</th>
                      <th className="text-left px-3 py-2 font-medium text-gray-600">Контрагент</th>
                      <th className="text-left px-3 py-2 font-medium text-gray-600">Назначение</th>
                      <th className="text-left px-3 py-2 font-medium text-gray-600 min-w-[160px]">
                        Категория
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((row) => {
                      const currentCat = editedCategories[row.row_index] ?? row.category
                      const isAuto = row.auto_classified && !(row.row_index in editedCategories)
                      const isSkipped = skippedRows.has(row.row_index)
                      const hasError = !!row.error
                      return (
                        <tr
                          key={row.row_index}
                          className={clsx(
                            'border-t border-gray-100',
                            hasError
                              ? 'bg-red-50/60'
                              : isSkipped
                              ? 'bg-gray-100 opacity-40'
                              : isAuto
                              ? 'bg-emerald-50/50'
                              : 'bg-amber-50/50'
                          )}
                        >
                          <td className="px-2 py-2 text-center">
                            <input
                              type="checkbox"
                              checked={isSkipped}
                              onChange={() => {
                                setSkippedRows((prev) => {
                                  const next = new Set(prev)
                                  if (next.has(row.row_index)) next.delete(row.row_index)
                                  else next.add(row.row_index)
                                  return next
                                })
                              }}
                              disabled={hasError}
                              className="w-4 h-4 rounded border-gray-300 text-red-500 focus:ring-red-400"
                              title="Пропустить"
                            />
                          </td>
                          <td className="px-3 py-2 text-gray-500 text-xs max-w-[140px]" title={row.filename}>
                            <div className="truncate">{row.filename}</div>
                            {hasError && (
                              <div className="text-red-500 text-[10px] mt-0.5" title={row.error}>
                                {row.error}
                              </div>
                            )}
                          </td>
                          <td className="px-3 py-2 text-gray-700 whitespace-nowrap">
                            {row.date || '—'}
                          </td>
                          <td
                            className={clsx(
                              'px-3 py-2 text-right tabular-nums whitespace-nowrap font-medium',
                              row.entry_type === 'expense' ? 'text-red-600' : 'text-emerald-600'
                            )}
                          >
                            {row.amount > 0
                              ? `${row.entry_type === 'expense' ? '−' : '+'}${fmt(
                                  Math.abs(row.amount)
                                )} ₽`
                              : '—'}
                          </td>
                          <td
                            className="px-3 py-2 text-gray-700 max-w-[180px]"
                            title={row.counterparty}
                          >
                            <div className="line-clamp-2 text-xs">{row.counterparty}</div>
                          </td>
                          <td
                            className="px-3 py-2 text-gray-500 min-w-[200px]"
                            title={row.description}
                          >
                            <div className="line-clamp-2 text-xs leading-relaxed">
                              {row.description}
                            </div>
                          </td>
                          <td className="px-3 py-2">
                            <select
                              value={currentCat}
                              onChange={(e) => updateCategory(row.row_index, e.target.value)}
                              disabled={hasError}
                              className={clsx(
                                'w-full border rounded-lg px-2 py-1 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500/30 disabled:opacity-50',
                                isAuto ? 'border-emerald-300' : 'border-amber-300'
                              )}
                            >
                              <option value="">— Выберите —</option>
                              {groupCategoriesForSelect(categories).map((group) => (
                                <optgroup key={group.label} label={group.label}>
                                  {group.items.map((c) => (
                                    <option key={c.key} value={c.key}>
                                      {c.name}
                                    </option>
                                  ))}
                                </optgroup>
                              ))}
                            </select>
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        {rows && (
          <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-gray-200 shrink-0">
            <button
              onClick={handleClose}
              className="px-5 py-2 border border-gray-200 rounded-lg text-sm text-gray-600 hover:bg-gray-50 transition-colors"
            >
              Отмена
            </button>
            <button
              onClick={() => confirmMutation.mutate()}
              disabled={confirmMutation.isPending || validCount === 0}
              className="px-5 py-2 bg-emerald-600 text-white font-medium rounded-lg text-sm hover:bg-emerald-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
            >
              {confirmMutation.isPending
                ? 'Импорт...'
                : `Импортировать ${validCount} из ${rows.length}`}
            </button>
          </div>
        )}
      </div>
    </div>
  )
}
