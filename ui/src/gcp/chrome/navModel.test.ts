import { describe, expect, it } from 'vitest'
import type { ServiceDescriptor } from '../../api/services'
import {
  breadcrumbsFor,
  buildSearchGroups,
  pageTitleFor,
  pushRecent,
  readRecent,
  type StorageLike,
} from './navModel'

function fakeStorage(seed: Record<string, string> = {}): StorageLike {
  const data = new Map(Object.entries(seed))
  return {
    getItem: (key) => data.get(key) ?? null,
    setItem: (key, value) => {
      data.set(key, value)
    },
  }
}

const services: ServiceDescriptor[] = [
  {
    id: 'storage',
    label: 'Cloud Storage',
    category: 'Storage',
    rootPath: '/gcp/storage/buckets',
    children: [{ label: 'Buckets', path: '/gcp/storage/buckets' }],
    tier: 'full',
  },
  {
    id: 'pubsub',
    label: 'Pub/Sub',
    category: 'Integration',
    rootPath: '/gcp/pubsub/topics',
    children: [{ label: 'Topics', path: '/gcp/pubsub/topics' }],
    tier: 'full',
  },
  {
    id: 'iam',
    label: 'IAM',
    category: 'Security',
    rootPath: '/gcp/iam/service-accounts',
    children: [{ label: 'Service accounts', path: '/gcp/iam/service-accounts' }],
    tier: 'full',
  },
]

describe('recent list storage', () => {
  it('reads a de-duplicated, capped list and survives malformed JSON', () => {
    const storage = fakeStorage({ recent: '["a","b"]' })
    expect(readRecent('recent', storage)).toEqual(['a', 'b'])
    expect(pushRecent('recent', 'b', storage)).toEqual(['b', 'a'])
    expect(readRecent('recent', storage)).toEqual(['b', 'a'])

    const bad = fakeStorage({ recent: 'not json' })
    expect(readRecent('recent', bad)).toEqual([])

    const long = fakeStorage({ recent: '["a","b","c"]' })
    expect(pushRecent('recent', 'd', long, 3)).toEqual(['d', 'a', 'b'])
  })

  it('returns an empty list without storage', () => {
    expect(readRecent('missing', fakeStorage())).toEqual([])
  })
})

describe('buildSearchGroups', () => {
  it('orders favorites and recents before category groups', () => {
    const groups = buildSearchGroups(services, ['iam'], ['pubsub'], '')
    expect(groups.map((g) => g.label)).toEqual(['Favorites', 'Recent', 'Storage', 'Integration', 'Security'])
    expect(groups[0]?.options[0]?.id).toBe('iam')
    expect(groups[1]?.options[0]?.id).toBe('pubsub')
  })

  it('filters by label and category', () => {
    const byLabel = buildSearchGroups(services, [], [], 'storage')
    expect(byLabel.flatMap((g) => g.options).map((o) => o.id)).toEqual(['storage'])

    const byCategory = buildSearchGroups(services, [], [], 'security')
    expect(byCategory.flatMap((g) => g.options).map((o) => o.id)).toEqual(['iam'])

    expect(buildSearchGroups(services, [], [], 'nope')).toEqual([])
  })

  it('drops favorites that do not match the query', () => {
    const groups = buildSearchGroups(services, ['iam'], [], 'storage')
    expect(groups.map((g) => g.label)).not.toContain('Favorites')
  })
})

describe('breadcrumbsFor', () => {
  it('builds the root, service and admin trails', () => {
    expect(breadcrumbsFor(services, '/gcp')).toEqual([{ label: 'JaisCloud', to: '/gcp' }])
    expect(breadcrumbsFor(services, '/gcp/storage/buckets/b1')).toEqual([
      { label: 'JaisCloud', to: '/gcp' },
      { label: 'Cloud Storage', to: '/gcp/storage/buckets' },
    ])
    expect(breadcrumbsFor(services, '/gcp/admin')).toEqual([
      { label: 'JaisCloud', to: '/gcp' },
      { label: 'Admin', to: '/gcp/admin' },
    ])
  })
})

describe('pageTitleFor', () => {
  it('titles the console, a service and admin', () => {
    expect(pageTitleFor(services, '/gcp')).toBe('JaisCloud Console')
    expect(pageTitleFor(services, '/gcp/pubsub/topics/t1')).toBe('Pub/Sub · JaisCloud')
    expect(pageTitleFor(services, '/gcp/admin')).toBe('Admin · JaisCloud')
  })
})
