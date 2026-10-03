import { applyTheme } from '@cloudscape-design/components/theming'

/**
 * GCP Console palette layered on top of Cloudscape.
 *
 * Overrides the same tokens as the AWS theme (see ../aws/theme.ts) so it fully
 * supersedes the AWS orange instead of layering over it. Google Blue
 * (#1a73e8) is the primary action colour; the side navigation uses the
 * console's blue active highlight, mode-aware so labels stay readable in dark
 * mode.
 */
export function applyGcpConsoleTheme() {
  return applyTheme({
    theme: {
      tokens: {
        colorBackgroundButtonPrimaryDefault: '#1a73e8',
        colorBackgroundButtonPrimaryHover: '#1b66c9',
        colorBackgroundButtonPrimaryActive: '#174ea6',
        colorTextButtonPrimaryDefault: '#ffffff',
        colorTextButtonPrimaryHover: '#ffffff',
        colorTextButtonPrimaryActive: '#ffffff',
        // Side navigation: near-black labels in light mode with the
        // console's light-blue active highlight.
        colorTextSideNavigationItemDefault: { light: '#202124', dark: '#e8eaed' },
        colorTextSideNavigationItemActive: { light: '#1a73e8', dark: '#8ab4f8' },
        colorBackgroundSideNavigationItemActive: { light: '#e8f0fe', dark: '#1b3a5c' },
      },
    },
  })
}
