import { cloudTheme } from '../lib/cloud'
import { applyAwsConsoleTheme } from './aws/theme'
import { applyGcpConsoleTheme } from './gcp/theme'

/** Apply the console theme family for a cloud. */
export function applyCloudTheme(cloud: string | undefined): void {
  if (cloudTheme(cloud) === 'gcp') {
    applyGcpConsoleTheme()
  } else {
    applyAwsConsoleTheme()
  }
}
