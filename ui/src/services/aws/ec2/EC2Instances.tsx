import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Badge,
  Box,
  Button,
  ContentLayout,
  Header,
  Modal,
  SpaceBetween,
  Table,
} from '@cloudscape-design/components'
import type { TableProps } from '@cloudscape-design/components'
import {
  listInstances,
  terminateInstance,
  startInstance,
  stopInstance,
  type Instance,
} from '../../../api/ec2'

function stateBadge(state: string): 'green' | 'blue' | 'red' | 'grey' {
  if (state === 'running') return 'green'
  if (state === 'terminated') return 'red'
  if (state === 'stopped') return 'grey'
  return 'blue'
}

export function EC2Instances() {
  const qc = useQueryClient()
  const [selected, setSelected] = useState<Instance | null>(null)

  const { data, isLoading, error } = useQuery({
    queryKey: ['ec2', 'instances'],
    queryFn: () => listInstances(),
  })

  const invalidate = () => qc.invalidateQueries({ queryKey: ['ec2', 'instances'] })
  const terminate = useMutation({
    mutationFn: (id: string) => terminateInstance(id),
    onSuccess: () => {
      invalidate()
      setSelected(null)
    },
  })
  const start = useMutation({ mutationFn: (id: string) => startInstance(id), onSuccess: invalidate })
  const stop = useMutation({ mutationFn: (id: string) => stopInstance(id), onSuccess: invalidate })

  const items = data?.items ?? []

  const columnDefinitions: TableProps.ColumnDefinition<Instance>[] = [
    { id: 'id', header: 'Instance ID', cell: (i) => <Box variant="code">{i.id}</Box> },
    { id: 'state', header: 'State', cell: (i) => <Badge color={stateBadge(i.state)}>{i.state}</Badge> },
    { id: 'type', header: 'Instance type', cell: (i) => i.instanceType },
    { id: 'image', header: 'AMI ID', cell: (i) => <Box variant="code">{i.imageId}</Box> },
    { id: 'private', header: 'Private IP', cell: (i) => <Box variant="code">{i.privateIp || '—'}</Box> },
    { id: 'public', header: 'Public IP', cell: (i) => <Box variant="code">{i.publicIp || '—'}</Box> },
    {
      id: 'actions',
      header: '',
      width: 220,
      cell: (i) => (
        <SpaceBetween direction="horizontal" size="xs">
          {i.state === 'stopped' && (
            <Button onClick={() => start.mutate(i.id)}>Start</Button>
          )}
          {i.state === 'running' && <Button onClick={() => stop.mutate(i.id)}>Stop</Button>}
          {i.state !== 'terminated' && (
            <Button onClick={() => setSelected(i)}>Terminate</Button>
          )}
        </SpaceBetween>
      ),
    },
  ]

  return (
    <ContentLayout
      header={
        <Header
          variant="h1"
          description="EC2 instances (metadata only)"
        >
          Instances
        </Header>
      }
    >
      {error ? (
        <Alert type="error" header="Failed to load instances">
          {(error as Error).message}
        </Alert>
      ) : (
        <Table
          variant="container"
          columnDefinitions={columnDefinitions}
          items={items}
          loading={isLoading}
          loadingText="Loading instances"
          trackBy="id"
          empty={
            <Box textAlign="center" color="inherit">
              <SpaceBetween size="m">
                <b>No instances</b>
                <Box variant="p" color="inherit">
                  No EC2 instances found in this region.
                </Box>
              </SpaceBetween>
            </Box>
          }
        />
      )}

      <Modal
        visible={selected != null}
        onDismiss={() => setSelected(null)}
        header="Terminate instance"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setSelected(null)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={terminate.isPending}
                onClick={() => selected && terminate.mutate(selected.id)}
              >
                Terminate
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        {terminate.error && (
          <Box color="text-status-error" margin={{ bottom: 's' }}>
            {(terminate.error as Error).message}
          </Box>
        )}
        Terminate <b>{selected?.id}</b>? This cannot be undone.
      </Modal>
    </ContentLayout>
  )
}
