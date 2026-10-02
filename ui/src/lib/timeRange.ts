import type { DateRangePickerProps } from '@cloudscape-design/components'

export const RELATIVE_OPTIONS: DateRangePickerProps.RelativeOption[] = [
  { key: '15m', amount: 15, unit: 'minute', type: 'relative' },
  { key: '1h', amount: 1, unit: 'hour', type: 'relative' },
  { key: '3h', amount: 3, unit: 'hour', type: 'relative' },
  { key: '12h', amount: 12, unit: 'hour', type: 'relative' },
  { key: '1d', amount: 1, unit: 'day', type: 'relative' },
  { key: '1w', amount: 1, unit: 'week', type: 'relative' },
]

const UNIT_MS: Record<string, number> = {
  second: 1000,
  minute: 60_000,
  hour: 3_600_000,
  day: 86_400_000,
  week: 604_800_000,
  month: 2_592_000_000,
  year: 31_536_000_000,
}

/** Resolve a DateRangePicker value (relative or absolute) to a start/end window. */
export function rangeToWindow(value: DateRangePickerProps.Value | null): {
  start: Date
  end: Date
} {
  const now = new Date()
  if (!value) return { start: new Date(now.getTime() - 3 * 3_600_000), end: now }
  if (value.type === 'absolute') {
    return { start: new Date(value.startDate), end: new Date(value.endDate) }
  }
  const ms = (UNIT_MS[value.unit] ?? 3_600_000) * value.amount
  return { start: new Date(now.getTime() - ms), end: now }
}
