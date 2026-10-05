import type { ServiceDescriptor } from '../api/services'

/** Short label for a non-full service tier, or undefined for full services. */
export function tierLabel(service: ServiceDescriptor): string | undefined {
  if (service.tier === 'metadata') return 'Metadata only'
  if (service.tier === 'stub') return 'Shape only'
  return undefined
}

/** Longer explanation for the page-level notice on non-full services. */
export function tierDescription(service: ServiceDescriptor): string | undefined {
  if (service.tier === 'metadata') {
    return `Serves the ${service.label} API and stores resource records, but nothing ever runs here. Fine for control-plane/IaC round-trips, not for behaviour.${service.note ? ` (${service.note})` : ''}`
  }
  if (service.tier === 'stub') {
    return `Serves the ${service.label} API and runs a partial version of the real behaviour. Coverage and semantics are incomplete — verify against the real cloud service.${service.note ? ` (${service.note})` : ''}`
  }
  return undefined
}
