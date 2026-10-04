/** Shared code-editor configuration for the GCP console.
 *
 * The console's JSON/structured-text inputs used to be plain monospace MUI
 * TextFields. This module holds the pure mapping from a console language and
 * MUI palette mode to the ace mode/theme and editor options, so the lazy ace
 * component stays a thin mount and the mapping is unit-testable.
 */

/** Languages the shared editor understands. */
export type GcpCodeLanguage = 'json' | 'yaml' | 'text'

/** Effective light/dark appearance, matching `GcpThemeMode`. */
export type GcpCodeThemeMode = 'light' | 'dark'

/** ace mode path for a console language. */
export function aceModeFor(language: GcpCodeLanguage): string {
  switch (language) {
    case 'yaml':
      return 'ace/mode/yaml'
    case 'text':
      return 'ace/mode/text'
    default:
      return 'ace/mode/json'
  }
}

/** ace theme name matched to the MUI palette mode. Mirrors the AWS editor's
 * dawn/tomorrow_night pair so the two consoles stay visually consistent. */
export function aceThemeFor(mode: GcpCodeThemeMode): string {
  return mode === 'dark' ? 'ace/theme/tomorrow_night' : 'ace/theme/dawn'
}

export interface AceEditorOptions {
  mode: string
  theme: string
  readOnly: boolean
  wrap: boolean
  showFoldWidgets: boolean
  displayIndentGuides: boolean
  showPrintMargin: boolean
  showGutter: boolean
  highlightActiveLine: boolean
  highlightGutterLine: boolean
  tabSize: number
  fontSize: number
  minLines: number
  maxLines: number
  /** The JSON linter runs in a web worker; disabled so the ace bundle stays
   * bundler-friendly (inline lint is tracked separately from this migration). */
  useWorker: boolean
}

export interface AceEditorOptionInput {
  language: GcpCodeLanguage
  theme: GcpCodeThemeMode
  readOnly: boolean
  minLines?: number
  maxLines?: number
}

export const DEFAULT_MIN_LINES = 8
export const DEFAULT_MAX_LINES = 40

/** ace editor options shared by every mount: folding, bracket matching (on by
 * default), soft wrap and gutter styling. */
export function aceEditorOptions({
  language,
  theme,
  readOnly,
  minLines = DEFAULT_MIN_LINES,
  maxLines = DEFAULT_MAX_LINES,
}: AceEditorOptionInput): AceEditorOptions {
  const min = Math.max(1, minLines)
  return {
    mode: aceModeFor(language),
    theme: aceThemeFor(theme),
    readOnly,
    wrap: true,
    showFoldWidgets: true,
    displayIndentGuides: true,
    showPrintMargin: false,
    showGutter: true,
    highlightActiveLine: !readOnly,
    highlightGutterLine: !readOnly,
    tabSize: 2,
    fontSize: 13,
    minLines: min,
    maxLines: Math.max(min, maxLines),
    useWorker: false,
  }
}
