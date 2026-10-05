import { describe, expect, it } from 'vitest'
import type { ServiceDescriptor } from '../api/services'
import { engineModes, engineTag } from './engine'

const service = (over: Partial<ServiceDescriptor> = {}): ServiceDescriptor => ({
  id: 'dataproc',
  label: 'Dataproc',
  category: 'Analytics',
  rootPath: '/gcp/dataproc/clusters',
  children: [],
  tier: 'stub',
  ...over,
})

describe('engineTag', () => {
  it('is undefined without an engine or when inactive', () => {
    expect(engineTag(service())).toBeUndefined()
    expect(engineTag(service({ engine: { active: false, modes: [] } }))).toBeUndefined()
  })

  it('returns the active backend mode', () => {
    const tagged = service({ tier: 'full', engine: { active: true, mode: 'k8s', modes: [] } })
    expect(engineTag(tagged)).toBe('k8s')
  })
})

describe('engineModes', () => {
  it('is empty without an engine and lists every backend otherwise', () => {
    expect(engineModes(service())).toEqual([])
    const modes = [
      { name: 'mock', supported: true },
      { name: 'docker', supported: false, note: 'not wired' },
    ]
    expect(engineModes(service({ engine: { active: false, modes } }))).toEqual(modes)
  })
})
