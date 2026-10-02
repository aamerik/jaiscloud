import { createContext, useCallback, useContext, useMemo, useState } from 'react'

export interface SplitPanelState {
  header: string
  content: React.ReactNode
}

interface SplitPanelApi {
  state: SplitPanelState | null
  open: boolean
  show: (state: SplitPanelState) => void
  hide: () => void
  setOpen: (open: boolean) => void
}

const SplitPanelContext = createContext<SplitPanelApi>({
  state: null,
  open: false,
  show: () => {},
  hide: () => {},
  setOpen: () => {},
})

/** Access the AppLayout side panel to show resource details without leaving a list. */
export function useSplitPanel() {
  return useContext(SplitPanelContext)
}

export function SplitPanelProvider({ children }: { children: React.ReactNode }) {
  const [state, setState] = useState<SplitPanelState | null>(null)
  const [open, setOpen] = useState(false)

  const show = useCallback((next: SplitPanelState) => {
    setState(next)
    setOpen(true)
  }, [])

  const hide = useCallback(() => setOpen(false), [])

  const value = useMemo(
    () => ({ state, open, show, hide, setOpen }),
    [state, open, show, hide],
  )

  return <SplitPanelContext.Provider value={value}>{children}</SplitPanelContext.Provider>
}
