import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  IconButton,
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
import EditOutlinedIcon from '@mui/icons-material/EditOutlined'
import { deleteSink, listSinks, type LogSink } from '../../api/gcp/logging'
import { useAccount } from '../../context/AccountContext'
import { SinkDialog } from './SinkDialog'
import { GcpPageTitle } from '../common/PageTitle'

/** Log router sinks: list, create, edit and delete. */
export function SinksPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editing, setEditing] = useState<LogSink | undefined>(undefined)

  const sinks = useQuery({
    queryKey: ['gcp', 'logging', 'sinks', accountId],
    queryFn: listSinks,
  })

  const remove = useMutation({
    mutationFn: (name: string) => deleteSink(name),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'logging', 'sinks'] }),
  })

  const rows = sinks.data?.sinks ?? []

  return (
    <Box>
      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <Box>
          <GcpPageTitle id="logging">Log router</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Sinks route log entries to a destination · project {accountId || '—'}
          </Typography>
        </Box>
        <Button
          variant="contained"
          startIcon={<AddIcon />}
          onClick={() => {
            setEditing(undefined)
            setDialogOpen(true)
          }}
        >
          Create sink
        </Button>
      </Stack>

      {sinks.isError && <Alert severity="error">Failed to load sinks.</Alert>}
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell>Destination</TableCell>
              <TableCell>Filter</TableCell>
              <TableCell>Status</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {sinks.isLoading && (
              <TableRow>
                <TableCell colSpan={5} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!sinks.isLoading && rows.length === 0 && (
              <TableRow>
                <TableCell colSpan={5} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No sinks in this project.
                </TableCell>
              </TableRow>
            )}
            {rows.map((sink) => (
              <TableRow key={sink.name} hover>
                <TableCell>{sink.name}</TableCell>
                <TableCell
                  sx={{ maxWidth: 320, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                >
                  {sink.destination}
                </TableCell>
                <TableCell
                  sx={{ maxWidth: 320, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                >
                  {sink.filter || '—'}
                </TableCell>
                <TableCell>
                  <Chip
                    size="small"
                    label={sink.disabled ? 'DISABLED' : 'ENABLED'}
                    color={sink.disabled ? 'default' : 'success'}
                  />
                </TableCell>
                <TableCell align="right">
                  <Tooltip title="Edit sink">
                    <IconButton
                      size="small"
                      onClick={() => {
                        setEditing(sink)
                        setDialogOpen(true)
                      }}
                    >
                      <EditOutlinedIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title="Delete sink">
                    <IconButton size="small" onClick={() => remove.mutate(sink.name)} disabled={remove.isPending}>
                      <DeleteOutlineIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      <SinkDialog open={dialogOpen} onClose={() => setDialogOpen(false)} initial={editing} />
    </Box>
  )
}
