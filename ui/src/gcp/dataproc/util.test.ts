import { describe, expect, it } from 'vitest'
import { clusterStateColor, jobStateColor, jobTypeLabel, shortDate } from './util'

describe('shortDate', () => {
  it('renders an em dash when absent', () => {
    expect(shortDate()).toBe('—')
  })

  it('passes through an unparseable value', () => {
    expect(shortDate('not-a-date')).toBe('not-a-date')
  })
})

describe('clusterStateColor', () => {
  it('maps known states', () => {
    expect(clusterStateColor('RUNNING')).toBe('success')
    expect(clusterStateColor('STOPPED')).toBe('default')
    expect(clusterStateColor('ERROR')).toBe('error')
  })

  it('warns on a transitional state', () => {
    expect(clusterStateColor('CREATING')).toBe('warning')
    expect(clusterStateColor('STARTING')).toBe('warning')
  })
})

describe('jobStateColor', () => {
  it('maps known states', () => {
    expect(jobStateColor('DONE')).toBe('success')
    expect(jobStateColor('RUNNING')).toBe('info')
    expect(jobStateColor('ERROR')).toBe('error')
    expect(jobStateColor('CANCELLED')).toBe('default')
  })

  it('warns on a pending state', () => {
    expect(jobStateColor('PENDING')).toBe('warning')
  })
})

describe('jobTypeLabel', () => {
  it('humanizes the oneof field name', () => {
    expect(jobTypeLabel('sparkJob')).toBe('Spark')
    expect(jobTypeLabel('pysparkJob')).toBe('PySpark')
    expect(jobTypeLabel('sparkSqlJob')).toBe('Spark SQL')
    expect(jobTypeLabel('hiveJob')).toBe('Hive')
  })

  it('renders an em dash when absent', () => {
    expect(jobTypeLabel()).toBe('—')
  })
})
