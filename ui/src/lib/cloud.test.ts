import { describe, expect, it } from 'vitest'
import { cloudTheme, getActiveCloud, setActiveCloud } from './cloud'

describe('cloudTheme', () => {
  it('selects the GCP theme for gcp', () => {
    expect(cloudTheme('gcp')).toBe('gcp')
  })

  it('falls back to the AWS base for aws, azure and unknown clouds', () => {
    expect(cloudTheme('aws')).toBe('aws')
    expect(cloudTheme('azure')).toBe('aws')
    expect(cloudTheme(undefined)).toBe('aws')
  })
})

describe('active cloud', () => {
  it('defaults to aws and records the bootstrapped cloud', () => {
    expect(getActiveCloud()).toBe('aws')

    setActiveCloud('gcp')
    expect(getActiveCloud()).toBe('gcp')

    setActiveCloud('')
    expect(getActiveCloud()).toBe('aws')
  })
})
