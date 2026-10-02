import { createContext, useCallback, useContext, useMemo, useRef, useState } from 'react'
import type { FlashbarProps } from '@cloudscape-design/components'

export type FlashType = 'success' | 'error' | 'warning' | 'info'

export interface NotifyInput {
  type: FlashType
  header: string
  content?: string
}

interface NotificationsApi {
  items: FlashbarProps.MessageDefinition[]
  notify: (input: NotifyInput) => void
}

const NotificationsContext = createContext<NotificationsApi>({ items: [], notify: () => {} })

/** Access the global flash notifications (rendered in the AppLayout). */
export function useNotifications() {
  return useContext(NotificationsContext)
}

/**
 * Holds transient success/error notifications and auto-dismisses them. The
 * Layout renders `items` into the AppLayout notification area as a Flashbar.
 */
export function NotificationsProvider({ children }: { children: React.ReactNode }) {
  const [items, setItems] = useState<FlashbarProps.MessageDefinition[]>([])
  const counter = useRef(0)

  const dismiss = useCallback((id: string) => {
    setItems((prev) => prev.filter((item) => item.id !== id))
  }, [])

  const notify = useCallback(
    ({ type, header, content }: NotifyInput) => {
      const id = `flash-${Date.now()}-${counter.current++}`
      setItems((prev) => [
        ...prev,
        { type, header, content, id, dismissible: true, onDismiss: () => dismiss(id) },
      ])
      window.setTimeout(() => dismiss(id), 6000)
    },
    [dismiss],
  )

  const value = useMemo(() => ({ items, notify }), [items, notify])

  return <NotificationsContext.Provider value={value}>{children}</NotificationsContext.Provider>
}
