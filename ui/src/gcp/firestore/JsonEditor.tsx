import { TextField } from '@mui/material'

export interface JsonEditorProps {
  label: string
  value: string
  onChange: (value: string) => void
  error?: string | null
  minRows?: number
  helperText?: string
  disabled?: boolean
}

/** MUI-native JSON editor: a monospace multiline field. Kept free of the
 * Cloudscape design system so the GCP console stays MUI-only. */
export function JsonEditor({
  label,
  value,
  onChange,
  error,
  minRows = 12,
  helperText,
  disabled,
}: JsonEditorProps) {
  return (
    <TextField
      label={label}
      value={value}
      onChange={(e) => onChange(e.target.value)}
      error={Boolean(error)}
      helperText={error || helperText}
      disabled={disabled}
      fullWidth
      multiline
      minRows={minRows}
      spellCheck={false}
      slotProps={{
        input: {
          sx: { fontFamily: 'monospace', fontSize: 13, lineHeight: 1.5, alignItems: 'flex-start' },
        },
        htmlInput: { 'aria-label': label },
      }}
    />
  )
}
