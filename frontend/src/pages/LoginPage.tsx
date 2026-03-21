import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useAuthStore } from '@/store/auth'
import { authApi } from '@/api/endpoints'

export default function LoginPage() {
  const navigate = useNavigate()
  const setAuth = useAuthStore((s) => s.setAuth)
  const [email, setEmail] = useState('owner@ecom.ru')
  const [password, setPassword] = useState('demo1234')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError('')
    setLoading(true)
    try {
      const { data } = await authApi.login(email, password)
      const { data: user } = await import('@/api/client').then(({ api }) =>
        api.get('/auth/me', { headers: { Authorization: `Bearer ${data.access_token}` } })
      )
      setAuth(data.access_token, user)
      navigate('/')
    } catch {
      setError('Неверный email или пароль')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen bg-[#1e3a5f] flex items-center justify-center p-4">
      <div className="bg-white rounded-2xl shadow-xl w-full max-w-sm p-8">
        <div className="text-center mb-8">
          <div className="text-3xl font-bold text-[#1e3a5f] mb-1">РнП</div>
          <div className="text-sm text-gray-500">Управленческая аналитика</div>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Email</label>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className="w-full border border-gray-300 rounded-lg px-3 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              required
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Пароль</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full border border-gray-300 rounded-lg px-3 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              required
            />
          </div>

          {error && (
            <div className="text-sm text-red-600 bg-red-50 rounded-lg px-3 py-2">{error}</div>
          )}

          <button
            type="submit"
            disabled={loading}
            className="w-full bg-[#1e3a5f] text-white rounded-lg py-2.5 text-sm font-medium hover:bg-[#2a4f7f] transition-colors disabled:opacity-60"
          >
            {loading ? 'Вход...' : 'Войти'}
          </button>
        </form>

        <div className="mt-6 pt-4 border-t border-gray-100 text-xs text-gray-400 space-y-1">
          <div className="font-medium mb-1">Тестовые аккаунты:</div>
          {[
            ['owner@ecom.ru', 'Собственник'],
            ['finance@ecom.ru', 'Финансовый менеджер'],
            ['marketer@ecom.ru', 'Маркетолог'],
          ].map(([em, role]) => (
            <div
              key={em}
              className="cursor-pointer hover:text-blue-600 transition-colors"
              onClick={() => { setEmail(em); setPassword('demo1234') }}
            >
              {em} — {role}
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
