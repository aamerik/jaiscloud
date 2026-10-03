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
import CancelOutlinedIcon from '@mui/icons-material/CancelOutlined'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink } from 'react-router-dom'
import { cancelJob, deleteJob, listJobs, type BigQueryJob } from '../../api/gcp/bigquery'
import { useAccount } from '../../context/AccountContext'

function querySummary(job: BigQueryJob): string {
  const q = (job.query ?? '').replace(/\s+/g, ' ').trim()
  if (!q) return '—'
  return q.length > 80 ? `${q.slice(0, 80)}…` : q
}

/** BigQuery jobs in the project, with cancel and delete. */
export function JobsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'bigquery'] })

  const jobs = useQuery({
    queryKey: ['gcp', 'bigquery', 'jobs', accountId],
    queryFn: listJobs,
  })

  const cancel = useMutation({ mutationFn: (job: string) => cancelJob(job), onSuccess: invalidate })
  const remove = useMutation({ mutationFn: (job: string) => deleteJob(job), onSuccess: invalidate })

  return (
    <Box>
      <Stack sx={{ mb: 2 }}>
        <Typography variant="h5">BigQuery</Typography>
        <Typography variant="body2" color="text.secondary">
          Jobs · project {accountId || '—'}
        </Typography>
      </Stack>

      {jobs.isError && <Alert severity="error">Failed to load jobs.</Alert>}
      {(cancel.isError || remove.isError) && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Job action failed.
        </Alert>
      )}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Job ID</TableCell>
              <TableCell>Statement</TableCell>
              <TableCell>Query</TableCell>
              <TableCell>State</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {jobs.isLoading && (
              <TableRow>
                <TableCell colSpan={5} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!jobs.isLoading && (jobs.data?.jobs.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={5} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No jobs in this project.
                </TableCell>
              </TableRow>
            )}
            {jobs.data?.jobs.map((job) => (
              <TableRow key={job.jobId} hover>
                <TableCell>
                  <Link component={RouterLink} to={`/gcp/bigquery/jobs/${encodeURIComponent(job.jobId)}`}>
                    {job.jobId}
                  </Link>
                </TableCell>
                <TableCell>{job.statementType || '—'}</TableCell>
                <TableCell sx={{ fontFamily: 'monospace', maxWidth: 360, overflowWrap: 'anywhere' }}>
                  {querySummary(job)}
                </TableCell>
                <TableCell>
                  <Chip
                    size="small"
                    label={job.state || 'UNKNOWN'}
                    color={job.state === 'DONE' ? 'success' : 'warning'}
                  />
                </TableCell>
                <TableCell align="right">
                  <Tooltip title="Cancel job">
                    <span>
                      <IconButton
                        size="small"
                        disabled={cancel.isPending}
                        onClick={() => cancel.mutate(job.jobId)}
                        aria-label={`Cancel ${job.jobId}`}
                      >
                        <CancelOutlinedIcon fontSize="small" />
                      </IconButton>
                    </span>
                  </Tooltip>
                  <Tooltip title="Delete job">
                    <span>
                      <IconButton
                        size="small"
                        disabled={remove.isPending}
                        onClick={() => remove.mutate(job.jobId)}
                        aria-label={`Delete ${job.jobId}`}
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
