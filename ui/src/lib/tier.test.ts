import { describe, expect, it } from 'vitest'
import type { ServiceDescriptor } from '../api/services'
import { tierDescription, tierLabel } from './tier'

const service = (over: Partial<ServiceDescriptor> = {}): ServiceDescriptor => ({
  id: 'compute',
  label: 'Compute Engine',
  category: 'Compute',
  rootPath: '/gcp/compute/instances',
  children: [],
  tier: 'full',
  ...over,
})

describe('tierLabel', () => {
  it('returns undefined for full services', () => {
    expect(tierLabel(service({ tier: 'full' }))).toBeUndefined()
  })

  it('labels metadata-only and shape-only services', () => {
    expect(tierLabel(service({ tier: 'metadata' }))).toBe('Metadata only')
    expect(tierLabel(service({ tier: 'stub' }))).toBe('Shape only')
  })
})

describe('tierDescription', () => {
  it('returns undefined for full services', () => {
    expect(tierDescription(service({ tier: 'full' }))).toBeUndefined()
  })

  it('explains the tier and appends the note', () => {
    const metadata = tierDescription(service({ tier: 'metadata', note: 'no data plane' }))
    expect(metadata).toContain('nothing ever runs here')
    expect(metadata).toContain('(no data plane)')

    const stub = tierDescription(service({ tier: 'stub', note: 'shape only' }))
    expect(stub).toContain('partial version of the real behaviour')
    expect(stub).toContain('(shape only)')
  })
})
