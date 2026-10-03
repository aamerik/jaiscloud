import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Button, IconButton, Link, Stack, Tooltip } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink, useParams } from 'react-router-dom'
import { deleteDocument, listDocuments, type FirestoreDocument } from '../../api/gcp/firestore'
import { useAccount } from '../../context/AccountContext'
import { CreateDocumentDialog } from './CreateDocumentDialog'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

function shortDate(value?: string): string {
  if (!value) return '—'
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString()
}

/** Documents in a Firestore collection. */
export function DocumentsPage() {
  const { collection = '' } = useParams()
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])

  const documents = useQuery({
    queryKey: ['gcp', 'firestore', 'documents', collection, accountId],
    queryFn: () => listDocuments(collection),
    enabled: Boolean(collection),
  })

  const remove = useMutation({
    mutationFn: (document: string) => deleteDocument(collection, document),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'firestore'] }),
  })

  const rows = filterRows(documents.data?.documents ?? [], filter, (document) =>
    `${document.id} ${document.updateTime ?? ''}`,
  )

  const columns: GcpColumn<FirestoreDocument>[] = [
    {
      key: 'id',
      header: 'Document ID',
      sortable: true,
      sortValue: (document) => document.id,
      render: (document) => (
        <Link
          component={RouterLink}
          to={`/gcp/firestore/collections/${encodeURIComponent(collection)}/documents/${encodeURIComponent(document.id)}`}
        >
          {document.id}
        </Link>
      ),
    },
    {
      key: 'updated',
      header: 'Updated',
      sortable: true,
      sortValue: (document) => document.updateTime ?? '',
      render: (document) => shortDate(document.updateTime),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (document) => (
        <Tooltip title="Delete document">
          <span>
            <IconButton
              size="small"
              disabled={remove.isPending}
              onClick={() => remove.mutate(document.id)}
              aria-label={`Delete ${document.id}`}
            >
              <DeleteOutlineIcon fontSize="small" />
            </IconButton>
          </span>
        </Tooltip>
      ),
    },
  ]

  return (
    <Stack>
      <GcpPageHeader
        id="firestore"
        title={collection}
        subtitle={`Firestore collection · project ${accountId || '—'}`}
        backTo="/gcp/firestore/collections"
        backAriaLabel="Back to collections"
        actions={
          <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
            Create document
          </Button>
        }
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter documents"
        onRefresh={() => void documents.refetch()}
        refreshing={documents.isFetching}
      />

      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <GcpDataTable
        aria-label="Documents"
        columns={columns}
        rows={rows}
        getRowKey={(document) => document.id}
        loading={documents.isLoading}
        error={documents.isError ? 'Failed to load documents.' : null}
        emptyMessage={filter ? 'No documents match the filter.' : 'No documents in this collection.'}
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(document) => <GcpRowDetail row={document} />}
        detailTitle={(document) => document.id}
      />

      <CreateDocumentDialog
        open={createOpen}
        onClose={() => setCreateOpen(false)}
        collection={collection}
      />
    </Stack>
  )
}
