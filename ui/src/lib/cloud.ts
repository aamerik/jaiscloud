/** The console theme family a cloud uses. Azure reuses the AWS/OneTheme base. */
export type CloudTheme = 'gcp' | 'aws'

/** Pick the console theme family for a cloud (unknown clouds fall back to AWS). */
export function cloudTheme(cloud: string | undefined): CloudTheme {
  return cloud === 'gcp' ? 'gcp' : 'aws'
}

/**
 * Cloud the SPA was bootstrapped against. Set once from `/api/ui/v1/meta`
 * before the first render (see main.tsx) so cloud-dependent defaults such as
 * density can be resolved synchronously during the initial render.
 */
let activeCloud = 'aws'

export function setActiveCloud(cloud: string): void {
  activeCloud = cloud || 'aws'
}

export function getActiveCloud(): string {
  return activeCloud
}
