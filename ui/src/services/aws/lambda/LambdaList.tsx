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
import { listFunctions, deleteFunction, type LambdaFunction } from '../../../api/lambda'
import { formatDate } from '../../../lib/date'

function stateBadge(state: string): 'green' | 'blue' | 'red' | 'grey' {
  if (state === 'Active') return 'green'
  if (state === 'Pending') return 'blue'
  if (state === 'Inactive' || state === 'Failed') return 'red'
  return 'grey'
}

export function LambdaList() {
  const [confirmDelete, setConfirmDelete] = useState<LambdaFunction | null>(null)
  const qc = useQueryClient()
  const navigate = useNavigate()

  const { data, isLoading, error } = useQuery({
    queryKey: ['lambda', 'functions'],
    queryFn: () => listFunctions(),
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteFunction(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['lambda', 'functions'] })
      setConfirmDelete(null)
    },
  })

  const functions = data?.items ?? []

  const columnDefinitions: TableProps.ColumnDefinition<LambdaFunction>[] = [
    {
      id: 'name',
      header: 'Name',
      cell: (fn) => (
        <Link
          href={`/ui/aws/lambda/${encodeURIComponent(fn.name)}`}
          onFollow={(event) => {
            event.preventDefault()
            navigate(`/aws/lambda/${encodeURIComponent(fn.name)}`)
          }}
        >
          {fn.name}
        </Link>
      ),
    },
    { id: 'runtime', header: 'Runtime', cell: (fn) => <code>{fn.runtime}</code> },
    {
      id: 'handler',
      header: 'Handler',
      cell: (fn) => <Box variant="code">{fn.handler}</Box>,
    },
    { id: 'modified', header: 'Last modified', cell: (fn) => formatDate(fn.lastModified) },
    {
      id: 'state',
      header: 'State',
      cell: (fn) => <Badge color={stateBadge(fn.state)}>{fn.state || '—'}</Badge>,
    },
    {
      id: 'actions',
      header: '',
      width: 150,
      minWidth: 150,
      cell: (fn) => (
        <div onClick={(event) => event.stopPropagation()}>
          <Button onClick={() => setConfirmDelete(fn)}>Delete</Button>
        </div>
      ),
    },
  ]

  return (
    <ContentLayout
      header={
        <Header
          variant="h1"
          description={`${functions.length} function${functions.length !== 1 ? 's' : ''}`}
        >
          Lambda functions
        </Header>
      }
    >
      {error ? (
        <Alert type="error" header="Failed to load functions">
          {(error as Error).message}
        </Alert>
      ) : (
        <Table
          variant="container"
          columnDefinitions={columnDefinitions}
          items={functions}
          loading={isLoading}
          loadingText="Loading functions"
          trackBy="arn"
          onRowClick={({ detail }) =>
            navigate(`/aws/lambda/${encodeURIComponent(detail.item.name)}`)
          }
          empty={
            <Box textAlign="center" color="inherit">
              <SpaceBetween size="m">
                <b>No functions</b>
                <Box variant="p" color="inherit">
                  Lambda functions let you run code without managing infrastructure.
                </Box>
              </SpaceBetween>
            </Box>
          }
        />
      )}

      <Modal
        visible={confirmDelete != null}
        onDismiss={() => setConfirmDelete(null)}
        header="Delete function"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setConfirmDelete(null)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={deleteMut.isPending}
                onClick={() => confirmDelete && deleteMut.mutate(confirmDelete.name)}
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
        Permanently delete <b>{confirmDelete?.name}</b>?
      </Modal>
    </ContentLayout>
  )
}
