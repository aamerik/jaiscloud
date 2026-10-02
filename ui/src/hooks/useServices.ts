import { useQuery } from '@tanstack/react-query'
import { listServices } from '../api/services'

/** The set of services currently supported by the running server. */
export function useServices() {
  return useQuery({
    queryKey: ['services'],
    queryFn: listServices,
    staleTime: Infinity,
  })
}
