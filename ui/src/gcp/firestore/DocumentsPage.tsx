import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  IconButton,
  Link,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tooltip,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink, useParams } from 'react-router-dom'
import { deleteDocument, listDocuments } from '../../api/gcp/firestore'
import { useAccount } from '../../context/AccountContext'
import { CreateDocumentDialog } from './CreateDocumentDialog'

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

  const documents = useQuery({
    queryKey: ['gcp', 'firestore', 'documents', collection, accountId],
    queryFn: () => listDocuments(collection),
    enabled: Boolean(collection),
  })

  const remove = useMutation({
    mutationFn: (document: string) => deleteDocument(collection, document),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'firestore'] }),
  })

  return (
    <Box>
      <Stack
        direction="row"
        spacing={1}
        sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <IconButton
          component={RouterLink}
          to="/gcp/firestore/collections"
          aria-label="Back to collections"
        >
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <Typography variant="h5" sx={{ overflowWrap: 'anywhere' }}>
            {collection}
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Firestore collection · project {accountId || '—'}
          </Typography>
        </Box>
        <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
          Create document
        </Button>
      </Stack>

      {documents.isError && <Alert severity="error">Failed to load documents.</Alert>}
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Document ID</TableCell>
              <TableCell>Updated</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {documents.isLoading && (
              <TableRow>
                <TableCell colSpan={3} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!documents.isLoading && (documents.data?.documents.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={3} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No documents in this collection.
                </TableCell>
              </TableRow>
            )}
            {documents.data?.documents.map((doc) => (
              <TableRow key={doc.id} hover>
                <TableCell>
                  <Link
                    component={RouterLink}
                    to={`/gcp/firestore/collections/${encodeURIComponent(collection)}/documents/${encodeURIComponent(doc.id)}`}
                  >
                    {doc.id}
                  </Link>
                </TableCell>
                <TableCell>{shortDate(doc.updateTime)}</TableCell>
                <TableCell align="right">
                  <Tooltip title="Delete document">
                    <span>
                      <IconButton
                        size="small"
                        disabled={remove.isPending}
                        onClick={() => remove.mutate(doc.id)}
                        aria-label={`Delete ${doc.id}`}
                      >
                        <DeleteOutlineIcon fontSize="small" />
                      </IconButton>
                    </span>
                  </Tooltip>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      <CreateDocumentDialog
        open={createOpen}
        onClose={() => setCreateOpen(false)}
        collection={collection}
      />
    </Box>
  )
}
