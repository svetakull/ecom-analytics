import { useAuthStore } from '@/store/auth'
import { format } from 'date-fns'
import { ru } from 'date-fns/locale'

const ROLE_LABELS: Record<string, string> = {
  owner: 'Собственник',
  finance_manager: 'Финансовый менеджер',
  marketer: 'Маркетолог',
  mp_manager: 'Менеджер МП',
  warehouse: 'Склад',
  assistant: 'Ассистент',
}

export default function Header() {
  const user = useAuthStore((s) => s.user)
  const today = format(new Date(), 'd MMMM yyyy', { locale: ru })

  return (
    <header className="bg-white border-b border-gray-200 px-6 py-3 flex items-center justify-between shrink-0">
      <div className="text-sm text-gray-500">{today}</div>
      {user && (
        <div className="flex items-center gap-3">
          <span className="text-xs bg-blue-100 text-blue-700 px-2 py-0.5 rounded-full">
            {ROLE_LABELS[user.role] || user.role}
          </span>
          <span className="text-sm font-medium text-gray-700">{user.name}</span>
        </div>
      )}
    </header>
  )
}
