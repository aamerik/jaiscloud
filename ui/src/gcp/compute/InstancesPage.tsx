import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
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
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import StopIcon from '@mui/icons-material/Stop'
import { Link as RouterLink } from 'react-router-dom'
import {
  deleteInstance,
  listInstances,
  startInstance,
  stopInstance,
  type Instance,
} from '../../api/gcp/compute'
import { useAccount } from '../../context/AccountContext'
import { shortDate, statusColor } from './util'

function target(instance: Instance): { zone: string; instance: string } {
  return { zone: instance.zone, instance: instance.name }
}

/** Compute Engine instances across every zone, with start / stop / delete. */
export function InstancesPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'compute'] })

  const instances = useQuery({
    queryKey: ['gcp', 'compute', 'instances', accountId],
    queryFn: listInstances,
  })

  const start = useMutation({
    mutationFn: ({ zone, instance }: { zone: string; instance: string }) =>
      startInstance(zone, instance),
    onSuccess: invalidate,
  })
  const stop = useMutation({
    mutationFn: ({ zone, instance }: { zone: string; instance: string }) =>
      stopInstance(zone, instance),
    onSuccess: invalidate,
  })
  const remove = useMutation({
    mutationFn: ({ zone, instance }: { zone: string; instance: string }) =>
      deleteInstance(zone, instance),
    onSuccess: invalidate,
  })

  const busy = start.isPending || stop.isPending || remove.isPending

  return (
    <Box>
      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <Box>
          <Typography variant="h5">Compute Engine</Typography>
          <Typography variant="body2" color="text.secondary">
            Instances · project {accountId || '—'}
          </Typography>
        </Box>
      </Stack>

      {instances.isError && <Alert severity="error">Failed to load instances.</Alert>}
      {start.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Start failed.
        </Alert>
      )}
      {stop.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Stop failed.
        </Alert>
      )}
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
              <TableCell>Zone</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Machine type</TableCell>
              <TableCell>Internal IP</TableCell>
              <TableCell>External IP</TableCell>
              <TableCell>Created</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {instances.isLoading && (
              <TableRow>
                <TableCell colSpan={8} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!instances.isLoading && (instances.data?.instances.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={8} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No instances in this project.
                </TableCell>
              </TableRow>
            )}
            {instances.data?.instances.map((instance) => (
              <TableRow key={`${instance.zone}/${instance.name}`} hover>
                <TableCell>
                  <Link
                    component={RouterLink}
                    to={`/gcp/compute/instances/${encodeURIComponent(instance.zone)}/${encodeURIComponent(instance.name)}`}
                  >
                    {instance.name}
                  </Link>
                </TableCell>
                <TableCell>{instance.zone || '—'}</TableCell>
                <TableCell>
                  <Chip size="small" label={instance.status || 'UNKNOWN'} color={statusColor(instance.status)} />
                </TableCell>
                <TableCell>{instance.machineType || '—'}</TableCell>
                <TableCell>{instance.internalIp || '—'}</TableCell>
                <TableCell>{instance.externalIp || '—'}</TableCell>
                <TableCell>{shortDate(instance.creationTimestamp)}</TableCell>
                <TableCell align="right">
                  <Tooltip title="Start instance">
                    <span>
                      <IconButton
                        size="small"
                        disabled={busy || instance.status === 'RUNNING'}
                        onClick={() => start.mutate(target(instance))}
                        aria-label={`Start ${instance.name}`}
                      >
                        <PlayArrowIcon fontSize="small" />
                      </IconButton>
                    </span>
                  </Tooltip>
                  <Tooltip title="Stop instance">
                    <span>
                      <IconButton
                        size="small"
                        disabled={busy || instance.status !== 'RUNNING'}
                        onClick={() => stop.mutate(target(instance))}
                        aria-label={`Stop ${instance.name}`}
                      >
                        <StopIcon fontSize="small" />
                      </IconButton>
                    </span>
                  </Tooltip>
                  <Tooltip title="Delete instance">
                    <span>
                      <IconButton
                        size="small"
                        disabled={busy}
                        onClick={() => remove.mutate(target(instance))}
                        aria-label={`Delete ${instance.name}`}
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
    </Box>
  )
}
