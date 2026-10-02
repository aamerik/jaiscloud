import { applyTheme } from '@cloudscape-design/components/theming'

/**
 * AWS Console palette layered on top of Cloudscape.
 *
 * Cloudscape's default primary action is blue (#0972d3). The AWS Console
 * uses the signature AWS orange (#ff9900) for primary buttons, so override
 * the primary button background and label tokens to match. Link, focus and
 * status colors keep Cloudscape's defaults, which already match the console.
 */
export function applyAwsConsoleTheme() {
  return applyTheme({
    theme: {
      tokens: {
        colorBackgroundButtonPrimaryDefault: '#ff9900',
        colorBackgroundButtonPrimaryHover: '#ec7211',
        colorBackgroundButtonPrimaryActive: '#eb5f07',
        colorTextButtonPrimaryDefault: '#ffffff',
        colorTextButtonPrimaryHover: '#ffffff',
        colorTextButtonPrimaryActive: '#ffffff',
        // Side navigation: near-black labels in light mode with the
        // console's light-blue active highlight; mode-aware so labels stay
        // readable in dark mode.
        colorTextSideNavigationItemDefault: { light: '#16191f', dark: '#c6cbd1' },
        colorTextSideNavigationItemActive: { light: '#0972d3', dark: '#539fe5' },
        colorBackgroundSideNavigationItemActive: { light: '#d1e4fa', dark: '#1b3a5c' },
      },
    },
  })
}
