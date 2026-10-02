import type { ServiceDescriptor } from '../api/services'

/** Short label for a non-full service tier, or undefined for full services. */
export function tierLabel(service: ServiceDescriptor): string | undefined {
  if (service.tier === 'metadata') return 'Metadata only'
  if (service.tier === 'stub') return 'Preview'
  return undefined
}

/** Longer explanation for the page-level notice on non-full services. */
export function tierDescription(service: ServiceDescriptor): string | undefined {
  if (service.tier === 'metadata') {
    return `JaisCloud emulates the ${service.label} API and persists resources, but does not provision or run anything.${service.note ? ` (${service.note})` : ''}`
  }
  if (service.tier === 'stub') {
    return `JaisCloud returns plausible ${service.label} responses with limited operation coverage.${service.note ? ` (${service.note})` : ''}`
  }
  return undefined
}
