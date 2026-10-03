import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Chip,
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
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink } from 'react-router-dom'
import { deleteFunction, listFunctions, type GcpFunction } from '../../api/gcp/functions'
import { useAccount } from '../../context/AccountContext'
import { FunctionDialog } from './FunctionDialog'
import { functionStateColor, memoryLabel, shortDate, triggerSummary } from './util'

/** Cloud Functions across every location, with create and delete. */
export function FunctionsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'functions'] })

  const functions = useQuery({
    queryKey: ['gcp', 'functions', 'list', accountId],
    queryFn: listFunctions,
  })

  const remove = useMutation({
    mutationFn: (fn: GcpFunction) => deleteFunction(fn.location, fn.id),
    onSuccess: invalidate,
  })

  return (
    <Box>
      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <Box>
          <Typography variant="h5">Cloud Functions</Typography>
          <Typography variant="body2" color="text.secondary">
            Functions · project {accountId || '—'}
          </Typography>
        </Box>
        <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
          Create function
        </Button>
      </Stack>

      {functions.isError && <Alert severity="error">Failed to load functions.</Alert>}
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {(remove.error as Error).message}
        </Alert>
      )}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell>Location</TableCell>
              <TableCell>Runtime</TableCell>
              <TableCell>Trigger</TableCell>
              <TableCell>Memory</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Updated</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {functions.isLoading && (
              <TableRow>
                <TableCell colSpan={8} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!functions.isLoading && (functions.data?.functions.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={8} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No functions in this project.
                </TableCell>
              </TableRow>
            )}
            {functions.data?.functions.map((fn) => (
              <TableRow key={`${fn.location}/${fn.id}`} hover>
                <TableCell>
                  <Link
                    component={RouterLink}
                    to={`/gcp/functions/${encodeURIComponent(fn.location)}/${encodeURIComponent(fn.id)}`}
                  >
                    {fn.id}
                  </Link>
                </TableCell>
                <TableCell>{fn.location || '—'}</TableCell>
                <TableCell>{fn.runtime || '—'}</TableCell>
                <TableCell sx={{ maxWidth: 320, overflowWrap: 'anywhere' }}>{triggerSummary(fn)}</TableCell>
                <TableCell>{memoryLabel(fn.availableMemoryMB)}</TableCell>
                <TableCell>
                  <Chip size="small" label={fn.status || '—'} color={functionStateColor(fn.status)} />
                </TableCell>
                <TableCell>{shortDate(fn.updateTime)}</TableCell>
                <TableCell align="right">
                  <Tooltip title="Delete function">
                    <span>
                      <IconButton
                        size="small"
                        disabled={remove.isPending}
                        onClick={() => remove.mutate(fn)}
                        aria-label={`Delete ${fn.id}`}
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

      <FunctionDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Box>
  )
}
