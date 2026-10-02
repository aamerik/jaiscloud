import { useState, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  Alert,
  Autosuggest,
  Box,
  Button,
  ContentLayout,
  DateRangePicker,
  Form,
  FormField,
  Header,
  SpaceBetween,
  StatusIndicator,
  Table,
  Textarea,
} from '@cloudscape-design/components'
import type { DateRangePickerProps, StatusIndicatorProps } from '@cloudscape-design/components'
import { listLogGroups, startQuery, getQueryResults, stopQuery, type QueryResult } from '../../../api/logs'
import { RELATIVE_OPTIONS, rangeToWindow } from '../../../lib/timeRange'

function queryStatusType(status: string): StatusIndicatorProps.Type {
  switch (status) {
    case 'Complete':
      return 'success'
    case 'Failed':
      return 'error'
    case 'Cancelled':
      return 'stopped'
    case 'Running':
      return 'in-progress'
    default:
      return 'info'
  }
}

interface ResultRow {
  id: string
  cells: Array<{ field: string; value: string }>
}

export function LogInsights() {
  const [queryString, setQueryString] = useState('fields @timestamp, @message\n| sort @timestamp desc\n| limit 20')
  const [logGroupName, setLogGroupName] = useState('')
  const [result, setResult] = useState<QueryResult | null>(null)
  const [running, setRunning] = useState(false)
  const [error, setError] = useState('')
  const [range, setRange] = useState<DateRangePickerProps.Value | null>({
    type: 'relative',
    amount: 3,
    unit: 'hour',
  })
  const queryIdRef = useRef<string | null>(null)

  const { data: logGroups } = useQuery({
    queryKey: ['logs', 'groups'],
    queryFn: () => listLogGroups(),
  })
  const groupOptions = (logGroups?.items ?? []).map((group) => ({ value: group.name }))

  async function run() {
    if (!queryString.trim()) return
    setRunning(true)
    setError('')
    setResult(null)
    try {
      const { start, end } = rangeToWindow(range)
      const { queryId } = await startQuery({
        queryString,
        logGroupName: logGroupName || undefined,
        startTime: Math.floor(start.getTime() / 1000),
        endTime: Math.floor(end.getTime() / 1000),
      })
      queryIdRef.current = queryId
      // Poll for results
      let attempts = 0
      const poll = async () => {
        const res = await getQueryResults(queryId)
        if (res.status === 'Complete' || res.status === 'Failed' || res.status === 'Cancelled' || attempts >= 20) {
          setResult(res)
          setRunning(false)
        } else {
          attempts++
          setTimeout(poll, 500)
        }
      }
      await poll()
    } catch (e) {
      setError((e as Error).message)
      setRunning(false)
    }
  }

  async function stop() {
    if (queryIdRef.current) {
      try { await stopQuery(queryIdRef.current) } catch { /* ignore */ }
    }
    setRunning(false)
  }

  const fields = result?.results[0]?.map((f) => f.field) ?? []
  const rows: ResultRow[] = (result?.results ?? []).map((cells, index) => ({
    id: String(index),
    cells,
  }))

  return (
    <ContentLayout header={<Header variant="h1">Log Insights</Header>}>
      <SpaceBetween size="l">
        <Form
          header={<Header variant="h2">Query</Header>}
          actions={
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="primary" loading={running} disabled={!queryString.trim()} onClick={run}>
                Run query
              </Button>
              {running && <Button onClick={stop}>Stop</Button>}
            </SpaceBetween>
          }
        >
          <SpaceBetween size="m">
            <FormField label="Log group" description="Leave blank to query all log groups in the account.">
              <Autosuggest
                value={logGroupName}
                onChange={({ detail }) => setLogGroupName(detail.value)}
                options={groupOptions}
                filteringType="auto"
                placeholder="/aws/lambda/my-function"
                empty="No log groups"
                enteredTextLabel={(value) => `Use "${value}"`}
                ariaLabel="Log group"
              />
            </FormField>
            <FormField label="Time range">
              <DateRangePicker
                value={range}
                onChange={({ detail }) => setRange(detail.value)}
                relativeOptions={RELATIVE_OPTIONS}
                isValidRange={() => ({ valid: true })}
                placeholder="Filter by a date and time range"
                ariaLabel="Time range"
              />
            </FormField>
            <FormField label="Query">
              <Textarea
                value={queryString}
                onChange={({ detail }) => setQueryString(detail.value)}
                rows={6}
              />
            </FormField>
          </SpaceBetween>
        </Form>

        {error && (
          <Alert type="error" header="Query failed">
            {error}
          </Alert>
        )}

        {result && (
          <Table
            items={rows}
            trackBy={(row) => row.id}
            header={
              <Header
                variant="h2"
                description={
                  <SpaceBetween direction="horizontal" size="m">
                    <StatusIndicator type={queryStatusType(result.status)}>
                      {result.status}
                    </StatusIndicator>
                    <Box color="text-body-secondary" display="inline">
                      {result.statistics.recordsScanned.toFixed(0)} records scanned,{' '}
                      {result.statistics.recordsMatched.toFixed(0)} matched
                    </Box>
                  </SpaceBetween>
                }
              >
                Results
              </Header>
            }
            columnDefinitions={fields.map((field, index) => ({
              id: field,
              header: field,
              cell: (row: ResultRow) => row.cells[index]?.value ?? '',
            }))}
            empty={
              <Box textAlign="center" color="inherit">
                <b>No results</b>
                <Box variant="p" color="inherit">
                  No records matched the query.
                </Box>
              </Box>
            }
          />
        )}
      </SpaceBetween>
    </ContentLayout>
  )
}
