import { NavLink, useLocation } from 'react-router-dom'
import { useAuthStore } from '@/store/auth'
import type { UserRole } from '@/types'
import { useState } from 'react'
import {
  BarChart3,
  TrendingUp,
  Calculator,
  TrendingDown,
  Wallet,
  FileText,
  Warehouse,
  ClipboardCheck,
  Settings,
  LogOut,
  ChevronDown,
  Calendar,
  Scale,
  BookOpen,
  Activity,
  Ruler,
  DollarSign,
} from 'lucide-react'
import clsx from 'clsx'

interface NavItem {
  path: string
  label: string
  icon: React.ReactNode
  roles: UserRole[]
}

interface NavGroup {
  key: string
  label: string
  icon: React.ReactNode
  roles: UserRole[]
  items: NavItem[]
}

const NAV_GROUPS: NavGroup[] = [
  {
    key: 'stats',
    label: 'Аналитика',
    icon: <BarChart3 size={18} />,
    roles: ['owner', 'mp_manager', 'marketer', 'finance_manager'],
    items: [
      {
        path: '/analytics',
        label: 'Аналитика',
        icon: <Activity size={16} />,
        roles: ['owner', 'mp_manager'],
      },
      {
        path: '/rnp',
        label: 'РнП',
        icon: <TrendingUp size={16} />,
        roles: ['owner', 'mp_manager', 'marketer'],
      },
      {
        path: '/otsifrovka',
        label: 'Оцифровка',
        icon: <Calculator size={16} />,
        roles: ['owner', 'finance_manager', 'mp_manager'],
      },
      {
        path: '/elasticity',
        label: 'Цены',
        icon: <TrendingDown size={16} />,
        roles: ['owner', 'mp_manager'],
      },
      {
        path: '/cost-prices',
        label: 'Себестоимость',
        icon: <DollarSign size={16} />,
        roles: ['owner', 'finance_manager', 'mp_manager'],
      },
    ],
  },
  {
    key: 'finance',
    label: 'Финансы',
    icon: <Wallet size={18} />,
    roles: ['owner', 'finance_manager'],
    items: [
      {
        path: '/journal',
        label: 'Журнал операций',
        icon: <BookOpen size={16} />,
        roles: ['owner', 'finance_manager'],
      },
      {
        path: '/opiu',
        label: 'ОПиУ',
        icon: <FileText size={16} />,
        roles: ['owner', 'finance_manager'],
      },
      {
        path: '/dds',
        label: 'ДДС',
        icon: <Wallet size={16} />,
        roles: ['owner', 'finance_manager'],
      },
      {
        path: '/payment-calendar',
        label: 'Плат. календарь',
        icon: <Calendar size={16} />,
        roles: ['owner', 'finance_manager'],
      },
      {
        path: '/balance-sheet',
        label: 'Упр. баланс',
        icon: <Scale size={16} />,
        roles: ['owner', 'finance_manager'],
      },
      {
        path: '/credits',
        label: 'Кредиты',
        icon: <DollarSign size={16} />,
        roles: ['owner', 'finance_manager'],
      },
    ],
  },
  {
    key: 'warehouse',
    label: 'Склад',
    icon: <Warehouse size={18} />,
    roles: ['owner', 'warehouse', 'mp_manager'],
    items: [
      {
        path: '/sverka',
        label: 'Сверка поставок',
        icon: <ClipboardCheck size={16} />,
        roles: ['owner', 'warehouse', 'mp_manager'],
      },
      {
        path: '/logistics',
        label: 'Габариты',
        icon: <Ruler size={16} />,
        roles: ['owner', 'mp_manager', 'warehouse'],
      },
    ],
  },
]

const STANDALONE_ITEMS: NavItem[] = [
  {
    path: '/settings',
    label: 'Настройки',
    icon: <Settings size={18} />,
    roles: ['owner'],
  },
]

export default function Sidebar() {
  const { user, logout } = useAuthStore()
  const location = useLocation()

  // Auto-open group that contains the current route
  const activeGroupKey = NAV_GROUPS.find((g) =>
    g.items.some((item) => location.pathname.startsWith(item.path))
  )?.key

  const [openGroups, setOpenGroups] = useState<Set<string>>(
    new Set(activeGroupKey ? [activeGroupKey] : ['stats'])
  )

  const toggleGroup = (key: string) => {
    setOpenGroups((prev) => {
      const next = new Set(prev)
      if (next.has(key)) {
        next.delete(key)
      } else {
        next.add(key)
      }
      return next
    })
  }

  const hasRole = (roles: UserRole[]) => !user || roles.includes(user.role)

  return (
    <aside className="w-56 bg-[#1e3a5f] text-white flex flex-col shrink-0">
      <div className="px-4 py-5 border-b border-white/10">
        <div className="font-bold text-lg leading-tight">Ecom Analytics</div>
        <div className="text-xs text-blue-200 mt-0.5">Рука на пульсе</div>
      </div>

      <nav className="flex-1 py-3 overflow-y-auto">
        {NAV_GROUPS.filter((g) => hasRole(g.roles)).map((group) => {
          const isOpen = openGroups.has(group.key)
          const visibleItems = group.items.filter((item) => hasRole(item.roles))
          if (!visibleItems.length) return null

          const isActiveGroup = visibleItems.some((item) =>
            location.pathname.startsWith(item.path)
          )

          return (
            <div key={group.key} className="mb-1">
              {/* Group header */}
              <button
                onClick={() => toggleGroup(group.key)}
                className={clsx(
                  'w-full flex items-center gap-3 px-4 py-2.5 text-sm transition-colors',
                  isActiveGroup
                    ? 'text-white font-medium'
                    : 'text-blue-200 hover:text-white'
                )}
              >
                {group.icon}
                <span className="flex-1 text-left">{group.label}</span>
                <ChevronDown
                  size={14}
                  className={clsx(
                    'transition-transform duration-200',
                    isOpen ? 'rotate-0' : '-rotate-90'
                  )}
                />
              </button>

              {/* Sub-items */}
              <div
                className={clsx(
                  'overflow-hidden transition-all duration-200',
                  isOpen ? 'max-h-96 opacity-100' : 'max-h-0 opacity-0'
                )}
              >
                {visibleItems.map((item) => (
                  <NavLink
                    key={item.path}
                    to={item.path}
                    className={({ isActive }) =>
                      clsx(
                        'flex items-center gap-3 pl-11 pr-4 py-2 text-sm transition-colors',
                        isActive
                          ? 'bg-white/15 text-white font-medium'
                          : 'text-blue-100 hover:bg-white/10 hover:text-white'
                      )
                    }
                  >
                    {item.icon}
                    {item.label}
                  </NavLink>
                ))}
              </div>
            </div>
          )
        })}

        {/* Standalone items (Настройки) */}
        <div className="mt-2 pt-2 border-t border-white/10">
          {STANDALONE_ITEMS.filter((item) => hasRole(item.roles)).map((item) => (
            <NavLink
              key={item.path}
              to={item.path}
              className={({ isActive }) =>
                clsx(
                  'flex items-center gap-3 px-4 py-2.5 text-sm transition-colors',
                  isActive
                    ? 'bg-white/15 text-white font-medium'
                    : 'text-blue-100 hover:bg-white/10 hover:text-white'
                )
              }
            >
              {item.icon}
              {item.label}
            </NavLink>
          ))}
        </div>
      </nav>

      <div className="border-t border-white/10 p-4">
        <div className="text-xs text-blue-200 mb-1 truncate">{user?.name}</div>
        <button
          onClick={logout}
          className="flex items-center gap-2 text-sm text-blue-100 hover:text-white transition-colors"
        >
          <LogOut size={15} />
          Выйти
        </button>
      </div>
    </aside>
  )
}
