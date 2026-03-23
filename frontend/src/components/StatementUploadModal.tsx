import { useState, useCallback, useRef } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/api/client'
import { X, Upload, FileSpreadsheet, AlertCircle } from 'lucide-react'
import clsx from 'clsx'

interface PreviewRow {
  row_index: number
  date: string
  amount: number
  counterparty: string
  description: string
  category: string
  auto_classified: boolean
  entry_type: string
}

interface PreviewData {
  filename: string
  rows: PreviewRow[]
  total_rows: number
  auto_classified_count: number
}

interface Props {
  open: boolean
  onClose: () => void
}

const fmt = (n: number) =>
  n.toLocaleString('ru-RU', { minimumFractionDigits: 2, maximumFractionDigits: 2 })

export default function StatementUploadModal({ open, onClose }: Props) {
  const [dragActive, setDragActive] = useState(false)
  const [file, setFile] = useState<File | null>(null)
  const [preview, setPreview] = useState<PreviewData | null>(null)
  const [editedCategories, setEditedCategories] = useState<Record<number, string>>({})
  const [error, setError] = useState('')
  const fileInputRef = useRef<HTMLInputElement>(null)
  const queryClient = useQueryClient()

  // Fetch categories for dropdown
  const { data: categories = [] } = useQuery<{ key: string; name: string }[]>({
    queryKey: ['journal-categories'],
    queryFn: () => api.get('/journal/categories').then((r) => r.data),
    enabled: open,
  })

  const uploadMutation = useMutation({
    mutationFn: async (f: File) => {
      const formData = new FormData()
      formData.append('file', f)
      const resp = await api.post('/journal/upload-statement', formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
      })
      const raw = resp.data
      // Map API response to PreviewData
      const rows: PreviewRow[] = (raw.entries || []).map((e: any, i: number) => ({
        row_index: i,
        date: e.date || '',
        amount: e.amount || 0,
        counterparty: e.counterparty || '',
        description: e.description || '',
        category: e.auto_category || '',
        auto_classified: !!(e.auto_category && e.confidence !== 'none'),
        entry_type: e.entry_type || 'expense',
      }))
      return {
        filename: raw.filename || f.name,
        rows,
        total_rows: raw.total_rows || rows.length,
        auto_classified_count: rows.filter((r: PreviewRow) => r.auto_classified).length,
      } as PreviewData
    },
    onSuccess: (data) => {
      setPreview(data)
      setEditedCategories({})
      setError('')
    },
    onError: () => {
      setError('Ошибка загрузки файла. Проверьте формат (.xlsx или .csv).')
    },
  })

  const confirmMutation = useMutation({
    mutationFn: async () => {
      if (!preview) return Promise.reject()
      const entries = preview.rows.map((row) => ({
        entry_type: row.entry_type || (row.amount >= 0 ? 'income' : 'expense'),
        amount: Math.abs(row.amount),
        scheduled_date: row.date,
        category: (editedCategories[row.row_index] ?? row.category) || 'other',
        counterparty: row.counterparty,
        description: row.description,
        account_name: preview.filename || 'Банковская выписка',
        is_recurring: false,
      }))
      return api.post('/journal/upload-confirm', { entries })
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['journal'] })
      handleClose()
    },
    onError: () => {
      setError('Ошибка импорта. Попробуйте снова.')
    },
  })

  const handleClose = () => {
    setFile(null)
    setPreview(null)
    setEditedCategories({})
    setError('')
    onClose()
  }

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault()
      setDragActive(false)
      const f = e.dataTransfer.files[0]
      if (f) {
        setFile(f)
        uploadMutation.mutate(f)
      }
    },
    [uploadMutation]
  )

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const f = e.target.files?.[0]
    if (f) {
      setFile(f)
      uploadMutation.mutate(f)
    }
  }

  const updateCategory = (rowIndex: number, category: string) => {
    setEditedCategories((prev) => ({ ...prev, [rowIndex]: category }))
  }

  if (!open) return null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-6xl max-h-[90vh] flex flex-col mx-4">
        {/* Header */}
        <div className="flex items-center justify-between px-6 pt-5 pb-3 shrink-0">
          <h2 className="text-lg font-bold text-gray-900">Загрузить банковскую выписку</h2>
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

          {/* Upload zone (no preview yet) */}
          {!preview && (
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
                accept=".xlsx,.csv"
                onChange={handleFileSelect}
                className="hidden"
              />
              {uploadMutation.isPending ? (
                <div className="text-gray-500">
                  <FileSpreadsheet size={40} className="mx-auto mb-3 text-blue-400 animate-pulse" />
                  <p className="text-sm font-medium">Загрузка и обработка...</p>
                </div>
              ) : (
                <div className="text-gray-500">
                  <Upload size={40} className="mx-auto mb-3 text-gray-400" />
                  <p className="text-sm font-medium">
                    Перетащите файл сюда или нажмите для выбора
                  </p>
                  <p className="text-xs text-gray-400 mt-1">Поддерживаются .xlsx и .csv</p>
                  {file && (
                    <p className="text-xs text-blue-600 mt-2">
                      Выбран: {file.name}
                    </p>
                  )}
                </div>
              )}
            </div>
          )}

          {/* Preview table */}
          {preview && (
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <div className="text-sm text-gray-600">
                  Найдено строк: <span className="font-medium">{preview.total_rows}</span>
                  {' | '}
                  Авто-распознано:{' '}
                  <span className="font-medium text-emerald-600">
                    {preview.auto_classified_count}
                  </span>
                </div>
              </div>

              <div className="border border-gray-200 rounded-xl overflow-hidden">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-gray-50">
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
                    {(preview.rows || []).map((row) => {
                      const currentCat = editedCategories[row.row_index] ?? row.category
                      const isAuto =
                        row.auto_classified && !(row.row_index in editedCategories)
                      return (
                        <tr
                          key={row.row_index}
                          className={clsx(
                            'border-t border-gray-100',
                            isAuto ? 'bg-emerald-50/50' : 'bg-amber-50/50'
                          )}
                        >
                          <td className="px-3 py-2 text-gray-700 whitespace-nowrap">{row.date}</td>
                          <td
                            className={clsx(
                              'px-3 py-2 text-right tabular-nums whitespace-nowrap font-medium',
                              row.entry_type === 'expense' ? 'text-red-600' : 'text-emerald-600'
                            )}
                          >
                            {row.entry_type === 'expense' ? '−' : '+'}{fmt(Math.abs(row.amount))} ₽
                          </td>
                          <td className="px-3 py-2 text-gray-700 max-w-[180px]" title={row.counterparty}>
                            <div className="line-clamp-2 text-xs">{row.counterparty}</div>
                          </td>
                          <td className="px-3 py-2 text-gray-500 min-w-[250px]" title={row.description}>
                            <div className="line-clamp-2 text-xs leading-relaxed">{row.description}</div>
                          </td>
                          <td className="px-3 py-2">
                            <select
                              value={currentCat}
                              onChange={(e) => updateCategory(row.row_index, e.target.value)}
                              className={clsx(
                                'w-full border rounded-lg px-2 py-1 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500/30',
                                isAuto ? 'border-emerald-300' : 'border-amber-300'
                              )}
                            >
                              <option value="">— Выберите —</option>
                              {categories.map((c) => (
                                <option key={c.key} value={c.key}>
                                  {c.name}
                                </option>
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

        {/* Footer buttons */}
        {preview && (
          <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-gray-200 shrink-0">
            <button
              onClick={handleClose}
              className="px-5 py-2 border border-gray-200 rounded-lg text-sm text-gray-600 hover:bg-gray-50 transition-colors"
            >
              Отмена
            </button>
            <button
              onClick={() => confirmMutation.mutate()}
              disabled={confirmMutation.isPending}
              className="px-5 py-2 bg-emerald-600 text-white font-medium rounded-lg text-sm hover:bg-emerald-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
            >
              {confirmMutation.isPending ? 'Импорт...' : 'Импортировать все'}
            </button>
          </div>
        )}
      </div>
    </div>
  )
}
