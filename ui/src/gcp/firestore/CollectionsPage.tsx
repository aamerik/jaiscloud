import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Link,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import { Link as RouterLink } from 'react-router-dom'
import { listCollections } from '../../api/gcp/firestore'
import { useAccount } from '../../context/AccountContext'
import { CreateDocumentDialog } from './CreateDocumentDialog'
import { GcpPageTitle } from '../common/PageTitle'

/** Firestore root collections. Clicking a collection browses its documents. */
export function CollectionsPage() {
  const { accountId } = useAccount()
  const [createOpen, setCreateOpen] = useState(false)

  const collections = useQuery({
    queryKey: ['gcp', 'firestore', 'collections', accountId],
    queryFn: listCollections,
  })

  return (
    <Box>
      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <Box>
          <GcpPageTitle id="firestore">Firestore</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Collections · project {accountId || '—'}
          </Typography>
        </Box>
        <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
          Create document
        </Button>
      </Stack>

      {collections.isError && <Alert severity="error">Failed to load collections.</Alert>}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Collection ID</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {collections.isLoading && (
              <TableRow>
                <TableCell align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!collections.isLoading && (collections.data?.collections.length ?? 0) === 0 && (
              <TableRow>
                <TableCell align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No collections in this project. Create a document to add one.
                </TableCell>
              </TableRow>
            )}
            {collections.data?.collections.map((collection) => (
              <TableRow key={collection.id} hover>
                <TableCell>
                  <Link
                    component={RouterLink}
                    to={`/gcp/firestore/collections/${encodeURIComponent(collection.id)}`}
                  >
                    {collection.id}
                  </Link>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      <CreateDocumentDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Box>
  )
}
