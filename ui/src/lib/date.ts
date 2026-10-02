/**
 * Format a timestamp coming from a JaisCloud API handler for display.
 *
 * Handlers are inconsistent: some return RFC3339 strings (CloudFormation,
 * EKS, S3, Lambda), some return Unix seconds (SQS CreatedTimestamp /
 * SentTimestamp, Glue job runs), and some return Unix milliseconds
 * (CloudWatch Logs). Accept all of them and fall back to the raw value
 * when it cannot be parsed, so nothing renders as "Invalid Date".
 */
export function formatDate(value: string | number | null | undefined): string {
  if (value === null || value === undefined || value === '') return '—'

  let ms: number

  if (typeof value === 'number') {
    ms = value
  } else {
    const trimmed = value.trim()
    // Numeric strings are epoch-based; everything else is parsed as a date
    // string (RFC3339 etc).
    if (/^-?\d+(\.\d+)?$/.test(trimmed)) {
      ms = Number(trimmed)
    } else {
      const parsed = Date.parse(trimmed)
      return Number.isNaN(parsed) ? value : new Date(parsed).toLocaleString()
    }
  }

  if (!Number.isFinite(ms)) return String(value)
  // Below 1e12 the value is seconds (1e12 ms ≈ 2001); otherwise milliseconds.
  if (Math.abs(ms) < 1e12) ms *= 1000

  const d = new Date(ms)
  return Number.isNaN(d.getTime()) ? String(value) : d.toLocaleString()
}
