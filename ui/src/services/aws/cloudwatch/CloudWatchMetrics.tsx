import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  Alert,
  Container,
  ContentLayout,
  Header,
  LineChart,
  Select,
  SpaceBetween,
} from '@cloudscape-design/components'
import { listMetrics, getMetricStatistics, type CWMetric } from '../../../api/cloudwatch'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'

const RANGES = [
  { value: '1', label: 'Last 1 hour' },
  { value: '3', label: 'Last 3 hours' },
  { value: '12', label: 'Last 12 hours' },
  { value: '24', label: 'Last 24 hours' },
]

export function CloudWatchMetrics() {
  const [selected, setSelected] = useState<CWMetric | null>(null)
  const [range, setRange] = useState('3')

  const { data, isLoading, error } = useQuery({
    queryKey: ['cloudwatch', 'metrics'],
    queryFn: () => listMetrics(),
  })

  const stats = useQuery({
    queryKey: ['cloudwatch', 'stats', selected?.namespace, selected?.metricName, range],
    enabled: selected != null,
    queryFn: () => {
      const now = new Date()
      const start = new Date(now.getTime() - Number(range) * 60 * 60 * 1000)
      return getMetricStatistics({
        namespace: selected!.namespace,
        metricName: selected!.metricName,
        startTime: start.toISOString(),
        endTime: now.toISOString(),
        period: 300,
        statistics: ['Sum', 'Average', 'Maximum', 'SampleCount'],
      })
    },
  })

  const metrics = data?.items ?? []

  const columns: ResourceColumn<CWMetric>[] = [
    { id: 'namespace', header: 'Namespace', filterLabel: 'Namespace', filterValue: (m) => m.namespace, cell: (m) => m.namespace },
    {
      id: 'metricName',
      header: 'Metric name',
      filterLabel: 'Metric name',
      filterValue: (m) => m.metricName,
      cell: (m) => m.metricName,
    },
  ]

  const datapoints = stats.data?.datapoints ?? []
  const series = [
    {
      title: 'Average',
      type: 'line' as const,
      data: datapoints
        .filter((d) => d.average != null)
        .map((d) => ({ x: new Date(d.timestamp), y: d.average as number })),
    },
    {
      title: 'Maximum',
      type: 'line' as const,
      data: datapoints
        .filter((d) => d.maximum != null)
        .map((d) => ({ x: new Date(d.timestamp), y: d.maximum as number })),
    },
    {
      title: 'Sum',
      type: 'line' as const,
      data: datapoints
        .filter((d) => d.sum != null)
        .map((d) => ({ x: new Date(d.timestamp), y: d.sum as number })),
    },
  ].filter((s) => s.data.length > 0)

  return (
    <ContentLayout header={<Header variant="h1">CloudWatch metrics</Header>}>
      {error ? (
        <Alert type="error" header="Failed to load metrics">
          {(error as Error).message}
        </Alert>
      ) : (
        <SpaceBetween size="l">
          <ResourceTable
            items={metrics}
            columns={columns}
            trackBy={(m) => `${m.namespace}/${m.metricName}`}
            title="Metrics"
            loading={isLoading}
            selectionType="single"
            selectedItems={selected ? [selected] : []}
            onSelectionChange={(items) => setSelected(items[0] ?? null)}
            onRowClick={(m) => setSelected(m)}
            emptyTitle="No metrics"
            emptyBody="Publish metric data via PutMetricData to see metrics here."
          />

          {selected && (
            <Container
              header={
                <Header
                  variant="h2"
                  description={`${selected.namespace} · last ${range}h · 5-minute periods`}
                  actions={
                    <Select
                      selectedOption={RANGES.find((r) => r.value === range) ?? RANGES[1]!}
                      onChange={({ detail }) => setRange(detail.selectedOption.value ?? '3')}
                      options={RANGES}
                      ariaLabel="Time range"
                    />
                  }
                >
                  {selected.metricName}
                </Header>
              }
            >
              <LineChart
                series={series}
                xScaleType="time"
                yScaleType="linear"
                xTitle="Time"
                yTitle="Value"
                height={320}
                statusType={stats.isLoading ? 'loading' : 'finished'}
                loadingText="Loading statistics"
                errorText="Failed to load statistics"
                empty={
                  <span>No datapoints in the selected range.</span>
                }
                ariaLabel={`Statistics for ${selected.metricName}`}
                i18nStrings={{
                  xTickFormatter: (value) =>
                    new Date(value).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
                  yTickFormatter: (value) => value.toLocaleString(),
                }}
              />
            </Container>
          )}
        </SpaceBetween>
      )}
    </ContentLayout>
  )
}
