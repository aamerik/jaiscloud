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
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import PauseIcon from '@mui/icons-material/Pause'
import PlayCircleOutlineIcon from '@mui/icons-material/PlayCircleOutlineOutlined'
import { Link as RouterLink } from 'react-router-dom'
import {
  deleteJob,
  listJobs,
  pauseJob,
  resumeJob,
  runJob,
  type SchedulerJob,
} from '../../api/gcp/scheduler'
import { useAccount } from '../../context/AccountContext'
import { JobDialog } from './JobDialog'
import { shortDate, stateColor, targetLabel } from './util'

function target(job: SchedulerJob) {
  return { location: job.location, job: job.name }
}

/** Cloud Scheduler jobs across every location, with pause/resume/run/delete. */
export function JobsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'scheduler'] })

  const jobs = useQuery({
    queryKey: ['gcp', 'scheduler', 'jobs', accountId],
    queryFn: listJobs,
  })

  const remove = useMutation({
    mutationFn: ({ location, job }: { location: string; job: string }) => deleteJob(location, job),
    onSuccess: invalidate,
  })
  const pause = useMutation({
    mutationFn: ({ location, job }: { location: string; job: string }) => pauseJob(location, job),
    onSuccess: invalidate,
  })
  const resume = useMutation({
    mutationFn: ({ location, job }: { location: string; job: string }) => resumeJob(location, job),
    onSuccess: invalidate,
  })
  const run = useMutation({
    mutationFn: ({ location, job }: { location: string; job: string }) => runJob(location, job),
    onSuccess: invalidate,
  })

  const busy = remove.isPending || pause.isPending || resume.isPending || run.isPending
  const actionError = remove.error ?? pause.error ?? resume.error ?? run.error

  return (
    <Box>
      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <Box>
          <Typography variant="h5">Cloud Scheduler</Typography>
          <Typography variant="body2" color="text.secondary">
            Jobs · project {accountId || '—'}
          </Typography>
        </Box>
        <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
          Create job
        </Button>
      </Stack>

      {jobs.isError && <Alert severity="error">Failed to load jobs.</Alert>}
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
              <TableCell>Location</TableCell>
              <TableCell>Schedule</TableCell>
              <TableCell>Target</TableCell>
              <TableCell>State</TableCell>
              <TableCell>Last attempt</TableCell>
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
                  No Cloud Scheduler jobs in this project.
                </TableCell>
              </TableRow>
            )}
            {jobs.data?.jobs.map((job) => (
              <TableRow key={`${job.location}/${job.name}`} hover>
                <TableCell>
                  <Link
                    component={RouterLink}
                    to={`/gcp/scheduler/jobs/${encodeURIComponent(job.location)}/${encodeURIComponent(job.name)}`}
                  >
                    {job.name}
                  </Link>
                </TableCell>
                <TableCell>{job.location || '—'}</TableCell>
                <TableCell>{job.schedule || '—'}</TableCell>
                <TableCell sx={{ maxWidth: 260, overflow: 'hidden', textOverflow: 'ellipsis' }}>
                  {targetLabel(job)}
                </TableCell>
                <TableCell>
                  <Chip size="small" label={job.state || '—'} color={stateColor(job.state)} />
                </TableCell>
                <TableCell>{shortDate(job.lastAttemptTime)}</TableCell>
                <TableCell align="right">
                  <Tooltip title="Run now">
                    <span>
                      <IconButton
                        size="small"
                        disabled={busy}
                        onClick={() => run.mutate(target(job))}
                        aria-label={`Run ${job.name}`}
                      >
                        <PlayCircleOutlineIcon fontSize="small" />
                      </IconButton>
                    </span>
                  </Tooltip>
                  {job.state === 'PAUSED' ? (
                    <Tooltip title="Resume">
                      <span>
                        <IconButton
                          size="small"
                          disabled={busy}
                          onClick={() => resume.mutate(target(job))}
                          aria-label={`Resume ${job.name}`}
                        >
                          <PlayArrowIcon fontSize="small" />
                        </IconButton>
                      </span>
                    </Tooltip>
                  ) : (
                    <Tooltip title="Pause">
                      <span>
                        <IconButton
                          size="small"
                          disabled={busy || job.state !== 'ENABLED'}
                          onClick={() => pause.mutate(target(job))}
                          aria-label={`Pause ${job.name}`}
                        >
                          <PauseIcon fontSize="small" />
                        </IconButton>
                      </span>
                    </Tooltip>
                  )}
                  <Tooltip title="Delete job">
                    <span>
                      <IconButton
                        size="small"
                        disabled={busy}
                        onClick={() => remove.mutate(target(job))}
                        aria-label={`Delete ${job.name}`}
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

      <JobDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Box>
  )
}
