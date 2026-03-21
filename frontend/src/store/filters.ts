import { create } from 'zustand'

interface FiltersState {
  channelType: string | null
  dateFrom: string | null
  dateTo: string | null
  setChannelType: (v: string | null) => void
  setDateRange: (from: string | null, to: string | null) => void
  reset: () => void
}

export const useFiltersStore = create<FiltersState>((set) => ({
  channelType: null,
  dateFrom: null,
  dateTo: null,
  setChannelType: (v) => set({ channelType: v }),
  setDateRange: (from, to) => set({ dateFrom: from, dateTo: to }),
  reset: () => set({ channelType: null, dateFrom: null, dateTo: null }),
}))
