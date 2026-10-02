import { useState } from 'react'
import ace from 'ace-builds'
import 'ace-builds/src-noconflict/mode-json'
import 'ace-builds/src-noconflict/theme-dawn'
import 'ace-builds/src-noconflict/theme-tomorrow_night'
import { CodeEditor } from '@cloudscape-design/components'
import type { CodeEditorProps } from '@cloudscape-design/components'

export interface JsonEditorProps {
  value: string
  onChange: (value: string) => void
  height?: number
  ariaLabel?: string
}

/**
 * Cloudscape CodeEditor preconfigured for JSON with the ace instance, json
 * mode and light/dark themes. Loaded lazily (see JsonEditor.tsx) so the ace
 * bundle is only fetched when an editor is actually rendered.
 */
export function JsonEditorImpl({
  value,
  onChange,
  height = 240,
  ariaLabel = 'JSON editor',
}: JsonEditorProps) {
  const [preferences, setPreferences] = useState<CodeEditorProps.Preferences>({
    wrapLines: true,
    theme: 'dawn',
  })

  return (
    <CodeEditor
      ace={ace}
      language="json"
      value={value}
      onChange={({ detail }) => onChange(detail.value)}
      onPreferencesChange={({ detail }) => setPreferences(detail)}
      preferences={preferences}
      themes={{ light: ['dawn'], dark: ['tomorrow_night'] }}
      editorContentHeight={height}
      ariaLabel={ariaLabel}
    />
  )
}
