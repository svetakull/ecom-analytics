import axios from 'axios'
import { useAuthStore } from '@/store/auth'

const API_BASE = import.meta.env.VITE_API_URL || ''

// FastAPI expects repeated params for arrays: ?channels=wb&channels=ozon
function serializeParams(params: Record<string, unknown>): string {
  const sp = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null) continue
    if (Array.isArray(value)) {
      value.forEach((v) => sp.append(key, String(v)))
    } else {
      sp.append(key, String(value))
    }
  }
  return sp.toString()
}

export const api = axios.create({
  baseURL: `${API_BASE}/api`,
  headers: { 'Content-Type': 'application/json' },
  paramsSerializer: (params) => serializeParams(params),
})

api.interceptors.request.use((config) => {
  const token = useAuthStore.getState().token
  if (token) {
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

api.interceptors.response.use(
  (res) => res,
  (err) => {
    if (err.response?.status === 401) {
      useAuthStore.getState().logout()
      window.location.href = '/login'
    }
    return Promise.reject(err)
  }
)
