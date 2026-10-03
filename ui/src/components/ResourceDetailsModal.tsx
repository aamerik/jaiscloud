import type { ReactNode } from 'react'
import { Box, Button, KeyValuePairs, Modal } from '@cloudscape-design/components'

export interface ResourceDetailItem {
  label: string
  value: ReactNode
}

export interface ResourceDetailsModalProps {
  visible: boolean
  onDismiss: () => void
  /** Modal title, typically the resource name or identifier. */
  header: string
  items: ResourceDetailItem[]
  /** KeyValuePairs column count. Defaults to 1. */
  columns?: 1 | 2
}

/**
 * Read-only details dialog for list-only resources. Keeps the "View details"
 * affordance consistent across services; use it with a selected item.
 */
export function ResourceDetailsModal({
  visible,
  onDismiss,
  header,
  items,
  columns = 1,
}: ResourceDetailsModalProps) {
  return (
    <Modal
      visible={visible}
      onDismiss={onDismiss}
      header={header}
      size="medium"
      footer={
        <Box float="right">
          <Button variant="link" onClick={onDismiss}>
            Close
          </Button>
        </Box>
      }
    >
      <KeyValuePairs columns={columns} items={items} />
    </Modal>
  )
}
