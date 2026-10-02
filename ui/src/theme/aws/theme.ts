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
      },
    },
  })
}
