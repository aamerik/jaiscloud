/** shortDate renders an RFC3339 timestamp for display, or em dash when absent. */
export function shortDate(value?: string): string {
  if (!value) return '—'
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString()
}

type ChipColor = 'success' | 'default' | 'warning' | 'error' | 'info'

/** clusterStateColor maps a Dataproc cluster state onto an MUI Chip color. */
export function clusterStateColor(state: string): ChipColor {
  switch (state) {
    case 'RUNNING':
      return 'success'
    case 'STOPPED':
    case 'DELETED':
      return 'default'
    case 'ERROR':
      return 'error'
    default:
      // CREATING / UPDATING / STARTING / STOPPING / DELETING
      return 'warning'
  }
}

/** jobStateColor maps a Dataproc job state onto an MUI Chip color. */
export function jobStateColor(state: string): ChipColor {
  switch (state) {
    case 'DONE':
      return 'success'
    case 'RUNNING':
      return 'info'
    case 'ERROR':
    case 'ATTEMPT_FAILURE':
      return 'error'
    case 'CANCELLED':
      return 'default'
    default:
      // PENDING / SETUP_DONE / CANCEL_PENDING / CANCEL_STARTED
      return 'warning'
  }
}

/** jobTypeLabel renders the job type (sparkJob -> Spark) for the list column. */
export function jobTypeLabel(type?: string): string {
  switch (type) {
    case 'sparkJob':
      return 'Spark'
    case 'pysparkJob':
      return 'PySpark'
    case 'sparkSqlJob':
      return 'Spark SQL'
    case 'sparkRJob':
      return 'SparkR'
    case 'hadoopJob':
      return 'Hadoop'
    case 'hiveJob':
      return 'Hive'
    case 'pigJob':
      return 'Pig'
    case undefined:
    case '':
      return '—'
    default:
      return type
  }
}
