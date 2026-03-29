/**
 * Настройки — интеграции и токены
 */
import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { integrationsApi, analyticsApi } from '@/api/endpoints'
import { Settings, Key, RefreshCw, CheckCircle, AlertCircle, ExternalLink, SlidersHorizontal, RotateCcw } from 'lucide-react'
import type { AnalyticsThreshold } from '@/types'

export default function SettingsPage() {
  const qc = useQueryClient()

  const { data: integrations = [], isLoading } = useQuery({
    queryKey: ['integrations'],
    queryFn: () => integrationsApi.list().then((r) => r.data),
  })

  const wbIntegration = integrations.find((i) => i.type === 'wb')

  return (
    <div className="p-6 max-w-2xl mx-auto space-y-6">
      <div className="flex items-center gap-3">
        <Settings size={22} className="text-gray-500" />
        <h1 className="text-xl font-semibold text-gray-900">Настройки</h1>
      </div>

      {isLoading && <div className="text-sm text-gray-400">Загрузка...</div>}

      {wbIntegration && (
        <WbIntegrationCard integration={wbIntegration} onSaved={() => qc.invalidateQueries({ queryKey: ['integrations'] })} />
      )}

      {!isLoading && !wbIntegration && (
        <div className="rounded-xl border border-gray-200 p-6 text-sm text-gray-500">
          Нет активных WB интеграций.
        </div>
      )}

      {/* Пороги аналитики */}
      <ThresholdsSection />
    </div>
  )
}

interface Integration {
  id: number
  type: string
  name: string
  status: string
  last_sync_at: string | null
  last_error: string | null
}

function WbIntegrationCard({ integration, onSaved }: { integration: Integration; onSaved: () => void }) {
  const [adsToken, setAdsToken] = useState('')
  const [syncResult, setSyncResult] = useState<string | null>(null)

  const saveToken = useMutation({
    mutationFn: () => integrationsApi.setAdsToken(integration.id, adsToken),
    onSuccess: (res) => {
      const d = res.data
      if (d.warning) {
        setSyncResult('⚠️ ' + d.warning)
      } else {
        setSyncResult('✓ ' + (d.message ?? 'Токен сохранён'))
        setAdsToken('')
      }
      onSaved()
    },
    onError: () => setSyncResult('✗ Ошибка при сохранении'),
  })

  const syncAds = useMutation({
    mutationFn: () => integrationsApi.syncAds(integration.id, 14),
    onSuccess: (res) => {
      const d = res.data as Record<string, unknown>
      setSyncResult(JSON.stringify(d, null, 2))
    },
    onError: () => setSyncResult('✗ Ошибка при синхронизации'),
  })

  const statusColor = integration.status === 'active' ? 'text-green-600' : 'text-red-500'

  return (
    <div className="rounded-xl border border-gray-200 bg-white shadow-sm overflow-hidden">
      {/* Шапка */}
      <div className="flex items-center justify-between px-5 py-4 border-b border-gray-100 bg-gray-50">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-purple-100 flex items-center justify-center text-purple-700 font-bold text-sm">WB</div>
          <div>
            <div className="font-medium text-gray-900 text-sm">{integration.name}</div>
            <div className={`text-xs ${statusColor}`}>{integration.status === 'active' ? 'Активна' : 'Ошибка'}</div>
          </div>
        </div>
        {integration.last_sync_at && (
          <div className="text-xs text-gray-400">
            Синхр. {new Date(integration.last_sync_at).toLocaleString('ru-RU')}
          </div>
        )}
      </div>

      {/* Ошибка интеграции */}
      {integration.last_error && (
        <div className="mx-5 mt-4 flex gap-2 text-xs text-red-600 bg-red-50 rounded-lg p-3">
          <AlertCircle size={14} className="shrink-0 mt-0.5" />
          <span>{integration.last_error}</span>
        </div>
      )}

      {/* Рекламный токен */}
      <div className="p-5 space-y-4">
        <div>
          <div className="flex items-center gap-2 mb-1.5">
            <Key size={14} className="text-gray-400" />
            <span className="text-sm font-medium text-gray-700">Токен для рекламного API WB</span>
          </div>
          <p className="text-xs text-gray-500 mb-3 leading-relaxed">
            WB Advertising API требует <strong>новый единый токен</strong> (JWT-формат, начинается с <code className="bg-gray-100 px-1 rounded">eyJ…</code>).
            Получить:{' '}
            <a
              href="https://seller.wildberries.ru/supplier-settings/access-to-new-api"
              target="_blank"
              rel="noopener noreferrer"
              className="text-indigo-600 hover:underline inline-flex items-center gap-0.5"
            >
              WB Личный кабинет → Настройки → Доступ к новому API
              <ExternalLink size={10} />
            </a>
            {' '}→ создать токен со скопом <strong>«Реклама»</strong>.
          </p>
          <div className="flex gap-2">
            <input
              type="password"
              placeholder="eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9..."
              value={adsToken}
              onChange={(e) => setAdsToken(e.target.value)}
              className="flex-1 border border-gray-200 rounded-lg px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-indigo-400"
            />
            <button
              onClick={() => saveToken.mutate()}
              disabled={!adsToken.trim() || saveToken.isPending}
              className="px-4 py-2 bg-indigo-600 text-white text-xs rounded-lg hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors font-medium"
            >
              {saveToken.isPending ? 'Сохраняю...' : 'Сохранить'}
            </button>
          </div>
        </div>

        {/* Ручная синхронизация рекламы */}
        <div className="pt-3 border-t border-gray-100">
          <div className="flex items-center justify-between">
            <div>
              <div className="text-sm font-medium text-gray-700">Синхронизация рекламных кампаний</div>
              <div className="text-xs text-gray-400 mt-0.5">Обновить данные РК за последние 14 дней</div>
            </div>
            <button
              onClick={() => syncAds.mutate()}
              disabled={syncAds.isPending}
              className="flex items-center gap-2 px-4 py-2 bg-gray-100 text-gray-700 text-xs rounded-lg hover:bg-gray-200 disabled:opacity-50 transition-colors font-medium"
            >
              <RefreshCw size={13} className={syncAds.isPending ? 'animate-spin' : ''} />
              {syncAds.isPending ? 'Синхронизирую...' : 'Синхронизировать'}
            </button>
          </div>
        </div>

        {/* Результат операции */}
        {syncResult && (
          <div className={`text-xs rounded-lg p-3 font-mono whitespace-pre-wrap border ${
            syncResult.startsWith('✗') ? 'bg-red-50 border-red-200 text-red-700' :
            syncResult.startsWith('⚠') ? 'bg-yellow-50 border-yellow-200 text-yellow-700' :
            'bg-green-50 border-green-200 text-green-700'
          }`}>
            {syncResult}
          </div>
        )}
      </div>
    </div>
  )
}

function ThresholdsSection() {
  const qc = useQueryClient()
  const [editKey, setEditKey] = useState<string | null>(null)
  const [editVal, setEditVal] = useState('')

  const { data: thresholds = [], isLoading } = useQuery<AnalyticsThreshold[]>({
    queryKey: ['analytics-thresholds'],
    queryFn: () => analyticsApi.thresholds().then((r) => r.data),
  })

  const updateMut = useMutation({
    mutationFn: ({ key, value }: { key: string; value: number }) =>
      analyticsApi.updateThreshold(key, value),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['analytics-thresholds'] })
      setEditKey(null)
    },
  })

  const resetMut = useMutation({
    mutationFn: () => analyticsApi.resetThresholds(),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['analytics-thresholds'] }),
  })

  return (
    <div className="rounded-xl border border-gray-200 bg-white shadow-sm overflow-hidden">
      <div className="flex items-center justify-between px-5 py-4 border-b border-gray-100 bg-gray-50">
        <div className="flex items-center gap-3">
          <SlidersHorizontal size={18} className="text-indigo-600" />
          <div>
            <div className="font-medium text-gray-900 text-sm">Пороги аналитики РнП</div>
            <div className="text-xs text-gray-400">Настройка зон и триггеров рекомендаций</div>
          </div>
        </div>
        <button
          onClick={() => resetMut.mutate()}
          disabled={resetMut.isPending}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-gray-600 bg-gray-100 rounded-md hover:bg-gray-200 transition-colors"
        >
          <RotateCcw size={12} className={resetMut.isPending ? 'animate-spin' : ''} />
          По умолчанию
        </button>
      </div>

      {isLoading ? (
        <div className="p-5 text-sm text-gray-400">Загрузка...</div>
      ) : (
        <div className="divide-y divide-gray-100">
          {thresholds.map((t) => (
            <div key={t.key} className="flex items-center justify-between px-5 py-3 hover:bg-gray-50">
              <div className="flex-1 min-w-0">
                <div className="text-sm text-gray-700">{t.description}</div>
                <div className="text-[10px] text-gray-400 font-mono">{t.key}</div>
              </div>
              {editKey === t.key ? (
                <div className="flex items-center gap-2">
                  <input
                    type="number"
                    value={editVal}
                    onChange={(e) => setEditVal(e.target.value)}
                    className="w-20 px-2 py-1 text-sm border border-indigo-300 rounded focus:ring-1 focus:ring-indigo-500"
                    autoFocus
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        updateMut.mutate({ key: t.key, value: parseFloat(editVal) })
                      }
                      if (e.key === 'Escape') setEditKey(null)
                    }}
                  />
                  <button
                    onClick={() => updateMut.mutate({ key: t.key, value: parseFloat(editVal) })}
                    className="text-xs text-indigo-600 font-medium"
                  >
                    OK
                  </button>
                  <button onClick={() => setEditKey(null)} className="text-xs text-gray-400">
                    Отмена
                  </button>
                </div>
              ) : (
                <button
                  onClick={() => { setEditKey(t.key); setEditVal(String(t.value)) }}
                  className="text-sm font-semibold text-indigo-600 hover:text-indigo-800 min-w-[60px] text-right"
                >
                  {t.value}
                </button>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
