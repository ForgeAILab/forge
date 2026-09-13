import { create } from 'zustand'
import { persist } from 'zustand/middleware'

export type ThemeMode = 'light' | 'dark'

type LayoutState = {
  theme: ThemeMode
  panelSizes: [number, number]
  selectedProjectId: string | undefined
  setTheme: (theme: ThemeMode) => void
  setPanelSizes: (sizes: [number, number]) => void
  setSelectedProjectId: (id: string | undefined) => void
}

const STORAGE_KEY = 'forge-web-layout'

function applyTheme(theme: ThemeMode): void {
  document.documentElement.classList.toggle('dark', theme === 'dark')
}

export const useLayoutStore = create<LayoutState>()(
  persist(
    (set) => ({
      theme: 'light',
      panelSizes: [35, 65],
      selectedProjectId: undefined,
      setTheme: (theme) => {
        applyTheme(theme)
        set({ theme })
      },
      setPanelSizes: (panelSizes) => set({ panelSizes }),
      setSelectedProjectId: (selectedProjectId) => set({ selectedProjectId }),
    }),
    {
      name: STORAGE_KEY,
      partialize: (s) => ({
        theme: s.theme,
        panelSizes: s.panelSizes,
        selectedProjectId: s.selectedProjectId,
      }),
    },
  ),
)

export function applyThemeFromStorage(): void {
  const raw = localStorage.getItem(STORAGE_KEY)
  if (!raw) {
    applyTheme('light')
    return
  }
  try {
    const parsed = JSON.parse(raw) as { state?: Partial<LayoutState> }
    const theme = parsed.state?.theme === 'dark' ? 'dark' : 'light'
    applyTheme(theme)
  } catch {
    applyTheme('light')
  }
}
