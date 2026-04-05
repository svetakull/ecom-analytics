/**
 * Группировка DDS-категорий для выпадающих списков.
 * Верхний уровень — по секции (доходы/расходы/налоги/...).
 * Внутри расходов — подразделы как в ДДС (Сайт, Продвижение, ПВЗ и т.д.).
 */

export interface Category {
  key: string
  name: string
  section?: string
}

export interface GroupedOption {
  label: string
  items: Category[]
}

const SECTION_LABELS: Record<string, string> = {
  income: 'Доходы',
  expenses: 'Расходы',
  taxes: 'Налоги',
  advances: 'Авансы (закупка)',
  credits: 'Кредиты и удержания',
  dividends: 'Дивиденды',
}

const SECTION_ORDER = ['income', 'expenses', 'taxes', 'advances', 'credits', 'dividends']

// Подразделы внутри расходов: key категории → название подраздела
const EXPENSE_SUBGROUPS: Record<string, string> = {
  // Сайт
  external_ads_site: 'Сайт',
  site_delivery: 'Сайт',
  // Продвижение внешнее
  external_ads: 'Продвижение внешнее',
  external_ads_smm_strategy: 'Продвижение внешнее',
  external_ads_personal_brand: 'Продвижение внешнее',
  external_ads_smm_brand: 'Продвижение внешнее',
  external_ads_shootings_brand: 'Продвижение внешнее',
  content: 'Продвижение внешнее',
  // ФОТ
  salary: 'ФОТ',
  salary_manager: 'ФОТ',
  salary_employee: 'ФОТ',
  salary_smm: 'ФОТ',
  salary_reels: 'ФОТ',
  // Аутсорс
  outsource: 'Аутсорс',
  outsource_accountant: 'Аутсорс',
  outsource_it: 'Аутсорс',
  outsource_other: 'Аутсорс',
  // Склад
  warehouse: 'Склад',
  warehouse_kalmykia: 'Склад',
  // ПВЗ
  pvz: 'ПВЗ',
  pvz_tko: 'ПВЗ',
  pvz_internet: 'ПВЗ',
  pvz_video: 'ПВЗ',
  rent_pvz: 'ПВЗ',
  salary_pvz: 'ПВЗ',
  // Выкупы
  buyout_services: 'Выкупы',
  buyout_goods: 'Выкупы',
  // Прочие (по умолчанию)
}

const EXPENSE_SUBGROUP_ORDER = [
  'Сайт',
  'Продвижение внешнее',
  'Выкупы',
  'ФОТ',
  'Аутсорс',
  'Склад',
  'ПВЗ',
  'Прочее',
]

export function groupCategoriesForSelect(categories: Category[]): GroupedOption[] {
  const groups: GroupedOption[] = []

  for (const section of SECTION_ORDER) {
    const items = categories.filter(c => c.section === section)
    if (items.length === 0) continue

    if (section !== 'expenses') {
      groups.push({ label: SECTION_LABELS[section] || section, items })
      continue
    }

    // Расходы: группируем по подразделам
    const bySubgroup: Record<string, Category[]> = {}
    for (const c of items) {
      const sub = EXPENSE_SUBGROUPS[c.key] || 'Прочее'
      if (!bySubgroup[sub]) bySubgroup[sub] = []
      bySubgroup[sub].push(c)
    }
    for (const sub of EXPENSE_SUBGROUP_ORDER) {
      if (!bySubgroup[sub]) continue
      groups.push({ label: `Расходы — ${sub}`, items: bySubgroup[sub] })
    }
  }

  return groups
}
