import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  Alert,
  Badge,
  Box,
  Button,
  ContentLayout,
  Form,
  FormField,
  Header,
  Input,
  Link,
  Modal,
  SpaceBetween,
  Table,
} from '@cloudscape-design/components'
import type { TableProps } from '@cloudscape-design/components'
import { listBuckets, createBucket, deleteBucket, type Bucket } from '../../../api/s3'

export function S3List() {
  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [confirmDelete, setConfirmDelete] = useState<Bucket | null>(null)
  const qc = useQueryClient()
  const navigate = useNavigate()

  const { data, isLoading, error } = useQuery({
    queryKey: ['s3', 'buckets'],
    queryFn: () => listBuckets(),
  })

  const createMut = useMutation({
    mutationFn: () => createBucket({ name: newName }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['s3', 'buckets'] })
      setCreateOpen(false)
      setNewName('')
    },
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteBucket(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['s3', 'buckets'] })
      setConfirmDelete(null)
    },
  })

  const buckets = data?.items ?? []

  const columnDefinitions: TableProps.ColumnDefinition<Bucket>[] = [
    {
      id: 'name',
      header: 'Name',
      cell: (b) => (
        <Link
          href={`/ui/aws/s3/${encodeURIComponent(b.name)}`}
          onFollow={(event) => {
            event.preventDefault()
            navigate(`/aws/s3/${encodeURIComponent(b.name)}`)
          }}
        >
          {b.name}
        </Link>
      ),
    },
    { id: 'region', header: 'Region', cell: (b) => b.region },
    {
      id: 'versioning',
      header: 'Versioning',
      cell: (b) => (
        <Badge color={b.versioning === 'Enabled' ? 'green' : 'grey'}>{b.versioning}</Badge>
      ),
    },
    {
      id: 'actions',
      header: '',
      width: 150,
      minWidth: 150,
      cell: (b) => (
        <div onClick={(event) => event.stopPropagation()}>
          <Button onClick={() => setConfirmDelete(b)}>Delete</Button>
        </div>
      ),
    },
  ]

  return (
    <ContentLayout
      header={
        <Header
          variant="h1"
          description={`${buckets.length} bucket${buckets.length !== 1 ? 's' : ''}`}
          actions={
            <Button variant="primary" onClick={() => setCreateOpen(true)}>
              Create bucket
            </Button>
          }
        >
          S3 buckets
        </Header>
      }
    >
      {error ? (
        <Alert type="error" header="Failed to load buckets">
          {(error as Error).message}
        </Alert>
      ) : (
        <Table
          variant="container"
          columnDefinitions={columnDefinitions}
          items={buckets}
          loading={isLoading}
          loadingText="Loading buckets"
          trackBy="name"
          onRowClick={({ detail }) =>
            navigate(`/aws/s3/${encodeURIComponent(detail.item.name)}`)
          }
          empty={
            <Box textAlign="center" color="inherit">
              <SpaceBetween size="m">
                <b>No buckets</b>
                <Box variant="p" color="inherit">
                  S3 buckets store your objects and files.
                </Box>
                <Button variant="primary" onClick={() => setCreateOpen(true)}>
                  Create bucket
                </Button>
              </SpaceBetween>
            </Box>
          }
        />
      )}

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create bucket"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={createMut.isPending}
                disabled={!newName}
                onClick={() => createMut.mutate()}
              >
                Create bucket
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          {createMut.error && (
            <Alert type="error" header="Could not create bucket">
              {(createMut.error as Error).message}
            </Alert>
          )}
          <FormField label="Bucket name" description="Bucket names must be globally unique.">
            <Input
              autoFocus
              value={newName}
              onChange={({ detail }) => setNewName(detail.value)}
              placeholder="my-bucket"
            />
          </FormField>
        </Form>
      </Modal>

      <Modal
        visible={confirmDelete != null}
        onDismiss={() => setConfirmDelete(null)}
        header="Delete bucket"
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
        Permanently delete <b>{confirmDelete?.name}</b>? The bucket must be empty.
      </Modal>
    </ContentLayout>
  )
}
