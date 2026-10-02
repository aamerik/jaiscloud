import { Badge } from '@cloudscape-design/components'
import type { ServiceDescriptor } from '../api/services'
import { tierLabel } from '../lib/tier'

/** Badge marking a service as metadata-only or preview; renders nothing for full services. */
export function ServiceTierBadge({ service }: { service: ServiceDescriptor }) {
  const label = tierLabel(service)
  if (!label) return null
  return <Badge color="grey">{label}</Badge>
}
