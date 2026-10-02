import { api } from './client'

export interface ServiceChild {
  label: string
  path: string
}

export interface ServiceDescriptor {
  id: string
  label: string
  category: string
  rootPath: string
  children: ServiceChild[]
  tier: 'full' | 'metadata' | 'stub'
  note?: string
}

export interface ServicesResponse {
  services: ServiceDescriptor[]
}

/** Services the running JaisCloud binary actually supports. */
export const listServices = () => api.get<ServicesResponse>('/api/ui/v1/services')
