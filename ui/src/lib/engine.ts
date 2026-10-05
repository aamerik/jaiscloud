import type { EngineMode, ServiceDescriptor } from '../api/services'

/**
 * Backend tag for an engine-capable service with a real engine configured
 * ('docker' / 'k8s' / 'native'), or undefined when no engine runs. This is the
 * implementation axis, never a fidelity tier.
 */
export function engineTag(service: ServiceDescriptor): string | undefined {
  return service.engine?.active ? service.engine.mode : undefined
}

/** All backends and their support state, for the availability matrix. */
export function engineModes(service: ServiceDescriptor): EngineMode[] {
  return service.engine?.modes ?? []
}
