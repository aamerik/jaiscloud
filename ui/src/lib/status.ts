import type { StatusIndicatorProps } from '@cloudscape-design/components'

export type ResourceStatus = StatusIndicatorProps.Type

const SUCCESS = new Set([
  'running', 'active', 'available', 'succeeded', 'success', 'complete', 'completed',
  'deployed', 'enabled', 'live', 'healthy', 'in-service', 'issued', 'ready', 'ok',
])
const ERROR = new Set([
  'failed', 'failure', 'error', 'terminated', 'deleting', 'deleted', 'unhealthy',
  'rollback_complete', 'rollback-complete', 'rollback', 'stopping',
])
const STOPPED = new Set(['stopped', 'inactive', 'disabled', 'suspended', 'suspended'])
const PENDING = new Set([
  'pending', 'creating', 'starting', 'updating', 'modifying', 'in-progress', 'in_progress',
  'provisioning', 'initializing',
])
const WARNING = new Set(['warning', 'degraded', 'impaired', 'insufficient_data'])

/** Map a service resource state string onto a Cloudscape StatusIndicator type. */
export function resourceStatus(state?: string | null): ResourceStatus {
  const s = (state ?? '').trim().toLowerCase()
  if (SUCCESS.has(s)) return 'success'
  if (ERROR.has(s)) return 'error'
  if (STOPPED.has(s)) return 'stopped'
  if (PENDING.has(s)) return 'in-progress'
  if (WARNING.has(s)) return 'warning'
  return 'info'
}
