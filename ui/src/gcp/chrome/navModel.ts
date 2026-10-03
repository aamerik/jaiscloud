import type { ServiceDescriptor } from '../../api/services'
import { groupByCategory, serviceForPath } from '../../components/nav'

/** Minimal Storage surface so the recent-list helpers are testable in Node. */
export interface StorageLike {
  getItem(key: string): string | null
  setItem(key: string, value: string): void
}

/** Recent services, shared with the Cloudscape shell so both consoles agree. */
export const RECENT_SERVICES_KEY = 'jaiscloud-recent'
/** Recent GCP projects for the "Select a project" dialog. */
export const RECENT_PROJECTS_KEY = 'jaiscloud-recent-projects'

function defaultStorage(): StorageLike | undefined {
  if (typeof window === 'undefined') return undefined
  try {
    return window.localStorage
  } catch {
    return undefined
  }
}

/** Read a JSON string-array from storage, tolerating malformed or absent data. */
export function readRecent(key: string, storage?: StorageLike): string[] {
  const store = storage ?? defaultStorage()
  if (!store) return []
  try {
    const raw: unknown = JSON.parse(store.getItem(key) ?? '[]')
    return Array.isArray(raw) ? raw.filter((id): id is string => typeof id === 'string') : []
  } catch {
    return []
  }
}

/** Push an id to the front of a recent list, de-duplicated and capped. */
export function pushRecent(
  key: string,
  id: string,
  storage?: StorageLike,
  limit = 6,
): string[] {
  const store = storage ?? defaultStorage()
  if (!id) return readRecent(key, store)
  const next = [id, ...readRecent(key, store).filter((item) => item !== id)].slice(0, limit)
  if (store) {
    try {
      store.setItem(key, JSON.stringify(next))
    } catch {
      /* ignore storage errors */
    }
  }
  return next
}

export interface SearchOption {
  id: string
  label: string
  description: string
  path: string
}

export interface SearchGroup {
  label: string
  options: SearchOption[]
}

function matchesQuery(service: ServiceDescriptor, query: string): boolean {
  const q = query.trim().toLowerCase()
  if (!q) return true
  return (
    service.label.toLowerCase().includes(q) || service.category.toLowerCase().includes(q)
  )
}

function toOption(service: ServiceDescriptor): SearchOption {
  return {
    id: service.id,
    label: service.label,
    description: service.category,
    path: service.rootPath,
  }
}

function pick(services: ServiceDescriptor[], ids: string[]): SearchOption[] {
  const byId = new Map(services.map((service) => [service.id, service]))
  return ids
    .map((id) => byId.get(id))
    .filter((service): service is ServiceDescriptor => service != null)
    .map(toOption)
}

/**
 * Build the grouped options for the GCP global search: matching favorites and
 * recents first, then every matching service grouped by category.
 */
export function buildSearchGroups(
  services: ServiceDescriptor[],
  favorites: string[],
  recent: string[],
  query: string,
): SearchGroup[] {
  const matching = services.filter((service) => matchesQuery(service, query))
  const matchingIds = new Set(matching.map((service) => service.id))
  const groups: SearchGroup[] = []

  const favoriteServices = pick(services, favorites).filter((option) => matchingIds.has(option.id))
  if (favoriteServices.length > 0) groups.push({ label: 'Favorites', options: favoriteServices })

  const recentServices = pick(services, recent).filter((option) => matchingIds.has(option.id))
  if (recentServices.length > 0) groups.push({ label: 'Recent', options: recentServices })

  for (const group of groupByCategory(matching)) {
    groups.push({
      label: group.category,
      options: group.services.map(toOption),
    })
  }
  return groups
}

export interface Crumb {
  label: string
  to?: string
}

/** Breadcrumb trail for a GCP router pathname (root, then service or Admin). */
export function breadcrumbsFor(services: ServiceDescriptor[], pathname: string): Crumb[] {
  const root: Crumb = { label: 'JaisCloud', to: '/gcp' }
  const rest = pathname.replace(/^\/gcp/, '').split('/').filter(Boolean)
  if (rest[0] === 'admin') return [root, { label: 'Admin', to: '/gcp/admin' }]
  const service = serviceForPath(services, pathname)
  if (service) return [root, { label: service.label, to: service.rootPath }]
  return [root]
}

/** `document.title` for a GCP router pathname. */
export function pageTitleFor(services: ServiceDescriptor[], pathname: string): string {
  const rest = pathname.replace(/^\/gcp/, '').split('/').filter(Boolean)
  if (rest[0] === 'admin') return 'Admin · JaisCloud'
  const service = serviceForPath(services, pathname)
  return service ? `${service.label} · JaisCloud` : 'JaisCloud Console'
}
