import { api } from './client'
import type {
  Channel,
  DashboardData,
  Order,
  OtsifrovkaData,
  RnPData,
  RnPPivotData,
  SalesDynamic,
  SalesSummary,
  SKUOut,
  User,
} from '@/types'

// Auth
export const authApi = {
  login: (email: string, password: string) =>
    api.post<{ access_token: string }>('/auth/login', { email, password }),
  me: () => api.get<User>('/auth/me'),
}

// Dashboard
export const dashboardApi = {
  owner: () => api.get<DashboardData>('/dashboard/owner'),
}

// РнП
export const rnpApi = {
  daily: (channelType?: string) =>
    api.get<RnPData>('/rnp/daily', { params: channelType ? { channel_type: channelType } : {} }),
  pivot: (params: { date_from?: string; date_to?: string; days?: number; channels?: string[]; article?: string } = { days: 7 }) =>
    api.get<RnPPivotData>('/rnp/pivot', { params }),
  updateSkuOverrides: (payload: {
    sku_id: number
    channel_id: number
    buyout_rate_pct?: number | null
    logistics_rub?: number | null
    commission_pct?: number | null
  }) => api.patch('/rnp/sku-overrides', payload),
}

// Продажи
export const salesApi = {
  orders: (params?: {
    channel_type?: string
    sku_id?: number
    date_from?: string
    date_to?: string
    limit?: number
    offset?: number
  }) => api.get<Order[]>('/sales/orders', { params }),
  summary: (params?: { date_from?: string; date_to?: string; channel_type?: string }) =>
    api.get<SalesSummary>('/sales/summary', { params }),
  dynamic: (params?: { date_from?: string; date_to?: string; channel_type?: string }) =>
    api.get<SalesDynamic[]>('/sales/dynamic', { params }),
}

// SKU
export const skuApi = {
  list: () => api.get<SKUOut[]>('/sku/'),
  detail: (id: number) => api.get(`/sku/${id}`),
}

// Channels
export const channelsApi = {
  list: () => api.get<Channel[]>('/channels/'),
}

// Integrations
export const integrationsApi = {
  syncNmReport: (daysBack = 14) =>
    api.post('/integrations/sync-nm-report-all', null, { params: { days_back: daysBack } }),
  list: () =>
    api.get<{ id: number; type: string; name: string; status: string; last_sync_at: string | null; last_error: string | null }[]>('/integrations/'),
  setAdsToken: (integrationId: number, adsApiKey: string) =>
    api.patch<{ ok: boolean; warning?: string; message?: string }>(`/integrations/${integrationId}/ads-token`, { ads_api_key: adsApiKey }),
  syncAds: (integrationId: number, daysBack = 14) =>
    api.post(`/integrations/${integrationId}/sync-ads`, null, { params: { days_back: daysBack } }),
}

// Оцифровка
export const otsifrovkaApi = {
  get: (params?: { date_from?: string; date_to?: string; days?: number; channels?: string[]; article?: string }) =>
    api.get<OtsifrovkaData>('/otsifrovka', { params }),
}

// Сверка поставок
export const sverkaApi = {
  run: (params: { date_from: string; date_to: string; channel: string; agent_name?: string; organization?: string }) =>
    api.get('/sverka', { params, timeout: 120000 }),
}

// Аналитика РнП
export const analyticsApi = {
  dashboard: (params?: { channels?: string[]; article?: string }) =>
    api.get('/rnp/analytics', { params }),
  thresholds: () => api.get('/analytics/thresholds'),
  updateThreshold: (key: string, value: number) =>
    api.patch(`/analytics/thresholds/${key}`, { value }),
  resetThresholds: () => api.post('/analytics/thresholds/reset'),
}

// Ценовая аналитика
export const elasticityApi = {
  dashboard: (channel = 'wb', limit = 20) =>
    api.get('/elasticity/dashboard', { params: { channel, limit } }),
  sku: (skuId: number, channel = 'wb', sppPct?: number) =>
    api.get(`/elasticity/sku/${skuId}`, { params: { channel, spp_pct: sppPct } }),
  forecast: (skuId: number, newPrice: number, channel = 'wb', sppPct?: number) =>
    api.get('/elasticity/forecast', { params: { sku_id: skuId, new_price: newPrice, channel, spp_pct: sppPct } }),
}
