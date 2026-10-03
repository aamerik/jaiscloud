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
  deleteCluster,
  listClusters,
  startCluster,
  stopCluster,
  type DataprocCluster,
} from '../../api/gcp/dataproc'
import { useAccount } from '../../context/AccountContext'
import { clusterStateColor, shortDate } from './util'
import { GcpPageTitle } from '../common/PageTitle'

function target(cluster: DataprocCluster) {
  return { region: cluster.region, cluster: cluster.id }
}

/** Dataproc clusters across every region, with start/stop/delete. */
export function ClustersPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'dataproc'] })

  const clusters = useQuery({
    queryKey: ['gcp', 'dataproc', 'clusters', accountId],
    queryFn: listClusters,
  })

  const remove = useMutation({
    mutationFn: ({ region, cluster }: { region: string; cluster: string }) =>
      deleteCluster(region, cluster),
    onSuccess: invalidate,
  })
  const start = useMutation({
    mutationFn: ({ region, cluster }: { region: string; cluster: string }) =>
      startCluster(region, cluster),
    onSuccess: invalidate,
  })
  const stop = useMutation({
    mutationFn: ({ region, cluster }: { region: string; cluster: string }) =>
      stopCluster(region, cluster),
    onSuccess: invalidate,
  })

  const busy = remove.isPending || start.isPending || stop.isPending
  const actionError = remove.error ?? start.error ?? stop.error

  return (
    <Box>
      <Stack sx={{ mb: 2 }}>
        <GcpPageTitle id="dataproc">Dataproc</GcpPageTitle>
        <Typography variant="body2" color="text.secondary">
          Clusters · project {accountId || '—'}
        </Typography>
      </Stack>

      {clusters.isError && <Alert severity="error">Failed to load clusters.</Alert>}
      {actionError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {(actionError as Error).message}
        </Alert>
      )}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell>Region</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Cluster UUID</TableCell>
              <TableCell>Created</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {clusters.isLoading && (
              <TableRow>
                <TableCell colSpan={6} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!clusters.isLoading && (clusters.data?.clusters.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={6} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No Dataproc clusters in this project.
                </TableCell>
              </TableRow>
            )}
            {clusters.data?.clusters.map((cluster) => (
              <TableRow key={`${cluster.region}/${cluster.id}`} hover>
                <TableCell>
                  <Link
                    component={RouterLink}
                    to={`/gcp/dataproc/clusters/${encodeURIComponent(cluster.region)}/${encodeURIComponent(cluster.id)}`}
                  >
                    {cluster.id}
                  </Link>
                </TableCell>
                <TableCell>{cluster.region || '—'}</TableCell>
                <TableCell>
                  <Chip size="small" label={cluster.status || '—'} color={clusterStateColor(cluster.status)} />
                </TableCell>
                <TableCell sx={{ maxWidth: 180, overflow: 'hidden', textOverflow: 'ellipsis' }}>
                  {cluster.clusterUuid || '—'}
                </TableCell>
                <TableCell>{shortDate(cluster.createTime)}</TableCell>
                <TableCell align="right">
                  {cluster.status === 'STOPPED' ? (
                    <Tooltip title="Start">
                      <span>
                        <IconButton
                          size="small"
                          disabled={busy || cluster.status !== 'STOPPED'}
                          onClick={() => start.mutate(target(cluster))}
                          aria-label={`Start ${cluster.id}`}
                        >
                          <PlayArrowIcon fontSize="small" />
                        </IconButton>
                      </span>
                    </Tooltip>
                  ) : (
                    <Tooltip title="Stop">
                      <span>
                        <IconButton
                          size="small"
                          disabled={busy || cluster.status !== 'RUNNING'}
                          onClick={() => stop.mutate(target(cluster))}
                          aria-label={`Stop ${cluster.id}`}
                        >
                          <StopIcon fontSize="small" />
                        </IconButton>
                      </span>
                    </Tooltip>
                  )}
                  <Tooltip title="Delete cluster">
                    <span>
                      <IconButton
                        size="small"
                        disabled={busy}
                        onClick={() => remove.mutate(target(cluster))}
                        aria-label={`Delete ${cluster.id}`}
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
