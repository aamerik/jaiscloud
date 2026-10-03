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
import PauseIcon from '@mui/icons-material/Pause'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import CleaningServicesOutlinedIcon from '@mui/icons-material/CleaningServicesOutlined'
import { Link as RouterLink } from 'react-router-dom'
import {
  deleteQueue,
  listQueues,
  pauseQueue,
  purgeQueue,
  resumeQueue,
  type TaskQueue,
} from '../../api/gcp/tasks'
import { useAccount } from '../../context/AccountContext'
import { QueueDialog } from './QueueDialog'
import { queueStateColor, rateLabel, shortDate } from './util'

function target(queue: TaskQueue) {
  return { location: queue.location, queue: queue.name }
}

/** Cloud Tasks queues across every location, with pause/resume/purge/delete. */
export function QueuesPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'tasks'] })

  const queues = useQuery({
    queryKey: ['gcp', 'tasks', 'queues', accountId],
    queryFn: listQueues,
  })

  const remove = useMutation({
    mutationFn: ({ location, queue }: { location: string; queue: string }) =>
      deleteQueue(location, queue),
    onSuccess: invalidate,
  })
  const pause = useMutation({
    mutationFn: ({ location, queue }: { location: string; queue: string }) =>
      pauseQueue(location, queue),
    onSuccess: invalidate,
  })
  const resume = useMutation({
    mutationFn: ({ location, queue }: { location: string; queue: string }) =>
      resumeQueue(location, queue),
    onSuccess: invalidate,
  })
  const purge = useMutation({
    mutationFn: ({ location, queue }: { location: string; queue: string }) =>
      purgeQueue(location, queue),
    onSuccess: invalidate,
  })

  const busy = remove.isPending || pause.isPending || resume.isPending || purge.isPending
  const actionError = remove.error ?? pause.error ?? resume.error ?? purge.error

  return (
    <Box>
      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <Box>
          <Typography variant="h5">Cloud Tasks</Typography>
          <Typography variant="body2" color="text.secondary">
            Queues · project {accountId || '—'}
          </Typography>
        </Box>
        <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
          Create queue
        </Button>
      </Stack>

      {queues.isError && <Alert severity="error">Failed to load queues.</Alert>}
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
              <TableCell>State</TableCell>
              <TableCell>Dispatch rate</TableCell>
              <TableCell>Retry attempts</TableCell>
              <TableCell>Last purged</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {queues.isLoading && (
              <TableRow>
                <TableCell colSpan={7} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!queues.isLoading && (queues.data?.queues.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={7} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No Cloud Tasks queues in this project.
                </TableCell>
              </TableRow>
            )}
            {queues.data?.queues.map((queue) => (
              <TableRow key={`${queue.location}/${queue.name}`} hover>
                <TableCell>
                  <Link
                    component={RouterLink}
                    to={`/gcp/tasks/queues/${encodeURIComponent(queue.location)}/${encodeURIComponent(queue.name)}`}
                  >
                    {queue.name}
                  </Link>
                </TableCell>
                <TableCell>{queue.location || '—'}</TableCell>
                <TableCell>
                  <Chip size="small" label={queue.state || '—'} color={queueStateColor(queue.state)} />
                </TableCell>
                <TableCell>{rateLabel(queue)}</TableCell>
                <TableCell>{queue.maxAttempts ?? '—'}</TableCell>
                <TableCell>{shortDate(queue.purgeTime)}</TableCell>
                <TableCell align="right">
                  {queue.state === 'PAUSED' ? (
                    <Tooltip title="Resume">
                      <span>
                        <IconButton
                          size="small"
                          disabled={busy}
                          onClick={() => resume.mutate(target(queue))}
                          aria-label={`Resume ${queue.name}`}
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
                          disabled={busy || queue.state !== 'RUNNING'}
                          onClick={() => pause.mutate(target(queue))}
                          aria-label={`Pause ${queue.name}`}
                        >
                          <PauseIcon fontSize="small" />
                        </IconButton>
                      </span>
                    </Tooltip>
                  )}
                  <Tooltip title="Purge tasks">
                    <span>
                      <IconButton
                        size="small"
                        disabled={busy}
                        onClick={() => purge.mutate(target(queue))}
                        aria-label={`Purge ${queue.name}`}
                      >
                        <CleaningServicesOutlinedIcon fontSize="small" />
                      </IconButton>
                    </span>
                  </Tooltip>
                  <Tooltip title="Delete queue">
                    <span>
                      <IconButton
                        size="small"
                        disabled={busy}
                        onClick={() => remove.mutate(target(queue))}
                        aria-label={`Delete ${queue.name}`}
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

      <QueueDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Box>
  )
}
