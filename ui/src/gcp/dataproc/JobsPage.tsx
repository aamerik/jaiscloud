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
import { Link as RouterLink } from 'react-router-dom'
import { cancelJob, listJobs, type DataprocJob } from '../../api/gcp/dataproc'
import { useAccount } from '../../context/AccountContext'
import { jobStateColor, jobTypeLabel, shortDate } from './util'

/** True for a job that has reached a terminal state and cannot be cancelled. */
function isTerminal(state: string): boolean {
  return ['DONE', 'ERROR', 'CANCELLED', 'ATTEMPT_FAILURE'].includes(state)
}

/** Dataproc jobs across every region, with cancel for active jobs. */
export function JobsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'dataproc'] })

  const jobs = useQuery({
    queryKey: ['gcp', 'dataproc', 'jobs', accountId],
    queryFn: listJobs,
  })

  const cancel = useMutation({
    mutationFn: ({ region, job }: { region: string; job: string }) => cancelJob(region, job),
    onSuccess: invalidate,
  })

  return (
    <Box>
      <Stack sx={{ mb: 2 }}>
        <Typography variant="h5">Dataproc</Typography>
        <Typography variant="body2" color="text.secondary">
          Jobs · project {accountId || '—'}
        </Typography>
      </Stack>

      {jobs.isError && <Alert severity="error">Failed to load jobs.</Alert>}
      {cancel.error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {(cancel.error as Error).message}
        </Alert>
      )}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Job ID</TableCell>
              <TableCell>Region</TableCell>
              <TableCell>Cluster</TableCell>
              <TableCell>Type</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Created</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {jobs.isLoading && (
              <TableRow>
                <TableCell colSpan={7} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!jobs.isLoading && (jobs.data?.jobs.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={7} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No Dataproc jobs in this project.
                </TableCell>
              </TableRow>
            )}
            {jobs.data?.jobs.map((job: DataprocJob) => (
              <TableRow key={`${job.region}/${job.id}`} hover>
                <TableCell>
                  <Link
                    component={RouterLink}
                    to={`/gcp/dataproc/jobs/${encodeURIComponent(job.region)}/${encodeURIComponent(job.id)}`}
                  >
                    {job.id}
                  </Link>
                </TableCell>
                <TableCell>{job.region || '—'}</TableCell>
                <TableCell>{job.clusterName || '—'}</TableCell>
                <TableCell>{jobTypeLabel(job.type)}</TableCell>
                <TableCell>
                  <Chip size="small" label={job.status || '—'} color={jobStateColor(job.status)} />
                </TableCell>
                <TableCell>{shortDate(job.createTime)}</TableCell>
                <TableCell align="right">
                  <Tooltip title="Cancel job">
                    <span>
                      <IconButton
                        size="small"
                        disabled={cancel.isPending || isTerminal(job.status)}
                        onClick={() => cancel.mutate({ region: job.region, job: job.id })}
                        aria-label={`Cancel ${job.id}`}
                      >
                        <CancelOutlinedIcon fontSize="small" />
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
