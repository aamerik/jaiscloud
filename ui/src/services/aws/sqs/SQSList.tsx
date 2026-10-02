import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  Alert,
  Badge,
  Box,
  Button,
  ContentLayout,
  Header,
  Link,
  Modal,
  SpaceBetween,
  Table,
} from '@cloudscape-design/components'
import type { TableProps } from '@cloudscape-design/components'
import { listQueues, deleteQueue, type Queue } from '../../../api/sqs'
import { formatDate } from '../../../lib/date'
import { SQSCreate } from './SQSCreate'

export function SQSList() {
  const [createOpen, setCreateOpen] = useState(false)
  const [confirmDelete, setConfirmDelete] = useState<Queue | null>(null)
  const qc = useQueryClient()
  const navigate = useNavigate()

  const { data, isLoading, error } = useQuery({
    queryKey: ['sqs', 'queues'],
    queryFn: () => listQueues(),
  })

  const deleteMut = useMutation({
    mutationFn: (url: string) => deleteQueue(url),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['sqs', 'queues'] })
      setConfirmDelete(null)
    },
  })

  const queues = data?.items ?? []

  const columnDefinitions: TableProps.ColumnDefinition<Queue>[] = [
    {
      id: 'name',
      header: 'Name',
      cell: (q) => (
        <Link
          href={`/ui/aws/sqs/${encodeURIComponent(q.url)}`}
          onFollow={(event) => {
            event.preventDefault()
            navigate(`/aws/sqs/${encodeURIComponent(q.url)}`)
          }}
        >
          {q.name}
        </Link>
      ),
    },
    {
      id: 'type',
      header: 'Type',
      cell: (q) => <Badge color={q.type === 'FIFO' ? 'blue' : 'grey'}>{q.type}</Badge>,
    },
    {
      id: 'available',
      header: 'Messages available',
      cell: (q) => q.messagesAvailable.toLocaleString(),
    },
    {
      id: 'inflight',
      header: 'Messages in flight',
      cell: (q) => q.messagesInFlight.toLocaleString(),
    },
    {
      id: 'created',
      header: 'Created',
      cell: (q) => formatDate(q.createdAt),
    },
    {
      id: 'actions',
      header: '',
      width: 150,
      minWidth: 150,
      cell: (q) => (
        <div onClick={(event) => event.stopPropagation()}>
          <Button onClick={() => setConfirmDelete(q)}>Delete</Button>
        </div>
      ),
    },
  ]

  return (
    <ContentLayout
      header={
        <Header
          variant="h1"
          description={data?.total != null ? `${data.total} queue${data.total !== 1 ? 's' : ''}` : undefined}
          actions={
            <Button variant="primary" onClick={() => setCreateOpen(true)}>
              Create queue
            </Button>
          }
        >
          SQS queues
        </Header>
      }
    >
      {error ? (
        <Alert type="error" header="Failed to load queues">
          {(error as Error).message}
        </Alert>
      ) : (
        <Table
          variant="container"
          columnDefinitions={columnDefinitions}
          items={queues}
          loading={isLoading}
          loadingText="Loading queues"
          trackBy="url"
          onRowClick={({ detail }) =>
            navigate(`/aws/sqs/${encodeURIComponent(detail.item.url)}`)
          }
          empty={
            <Box textAlign="center" color="inherit">
              <SpaceBetween size="m">
                <b>No queues</b>
                <Box variant="p" color="inherit">
                  SQS queues let your applications communicate asynchronously.
                </Box>
                <Button variant="primary" onClick={() => setCreateOpen(true)}>
                  Create queue
                </Button>
              </SpaceBetween>
            </Box>
          }
        />
      )}

      {createOpen && (
        <SQSCreate
          onClose={() => setCreateOpen(false)}
          onCreated={() => {
            setCreateOpen(false)
            void qc.invalidateQueries({ queryKey: ['sqs', 'queues'] })
          }}
        />
      )}

      <Modal
        visible={confirmDelete != null}
        onDismiss={() => setConfirmDelete(null)}
        header="Delete queue"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setConfirmDelete(null)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={deleteMut.isPending}
                onClick={() => confirmDelete && deleteMut.mutate(confirmDelete.url)}
              >
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        {deleteMut.error && (
          <Box color="text-status-error" margin={{ bottom: 's' }}>
            {(deleteMut.error as Error).message}
          </Box>
        )}
        Permanently delete <b>{confirmDelete?.name}</b>? All messages will be lost and cannot
        be recovered.
      </Modal>
    </ContentLayout>
  )
}
