import { Badge, Popover } from '@cloudscape-design/components'
import type { ServiceDescriptor } from '../api/services'
import { tierDescription, tierLabel } from '../lib/tier'

/** Badge marking a service as metadata-only or preview; renders nothing for full services. */
export function ServiceTierBadge({ service }: { service: ServiceDescriptor }) {
  const label = tierLabel(service)
  if (!label) return null
  return (
    <Popover
      triggerType="custom"
      position="right"
      size="medium"
      header={label}
      dismissButton
      content={tierDescription(service)}
    >
      <Badge color="grey">{label}</Badge>
    </Popover>
  )
}
