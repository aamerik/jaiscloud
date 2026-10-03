import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  IconButton,
  MenuItem,
  Paper,
  Snackbar,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material'
import RefreshIcon from '@mui/icons-material/Refresh'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import {
  createSnapshot,
  deleteSnapshot,
  getAdminStatus,
  getClock,
  listSnapshots,
  resetState,
  revertSnapshot,
  setClock,
  type ClockState,
  type Snapshot,
} from '../../api/admin'

type Feedback = { severity: 'success' | 'error'; message: string }

const MODE_OPTIONS: { value: ClockState['mode']; label: string }[] = [
  { value: 'real', label: 'real (wall clock)' },
  { value: 'fixed', label: 'fixed (frozen time)' },
  { value: 'offset', label: 'offset (shifted time)' },
]

function shortDate(value?: string): string {
  if (!value) return '—'
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString()
}

/** Cloud-neutral admin panel (status, clock, reset, export, snapshots) in MUI. */
export function GcpAdminPage() {
  const [feedback, setFeedback] = useState<Feedback | null>(null)

  return (
    <Box>
      <Typography variant="h5">Admin</Typography>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
        Status, clock, state reset and named snapshots for this emulator instance.
      </Typography>

      <Stack spacing={3}>
        <Box
          sx={{
            display: 'grid',
            gridTemplateColumns: { xs: '1fr', md: 'repeat(2, 1fr)' },
            gap: 3,
          }}
        >
          <StatusCard />
          <ClockCard notify={setFeedback} />
          <ResetCard notify={setFeedback} />
          <ExportCard />
        </Box>
        <SnapshotsSection notify={setFeedback} />
      </Stack>

      <Snackbar
        open={feedback != null}
        autoHideDuration={6000}
        onClose={() => setFeedback(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert severity={feedback?.severity ?? 'success'} onClose={() => setFeedback(null)}>
          {feedback?.message}
        </Alert>
      </Snackbar>
    </Box>
  )
}

function Panel({
  title,
  action,
  children,
}: {
  title: string
  action?: React.ReactNode
  children: React.ReactNode
}) {
  return (
    <Paper variant="outlined" sx={{ p: 2 }}>
      <Stack direction="row" sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
        <Typography variant="h6">{title}</Typography>
        {action}
      </Stack>
      {children}
    </Paper>
  )
}

function Field({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <Stack direction="row" spacing={2} sx={{ py: 0.5 }}>
      <Typography variant="body2" color="text.secondary" sx={{ minWidth: 120 }}>
        {label}
      </Typography>
      <Typography variant="body2" sx={{ wordBreak: 'break-all' }}>
        {value}
      </Typography>
    </Stack>
  )
}

function StatusCard() {
  const { data, isLoading, isError, refetch, isFetching } = useQuery({
    queryKey: ['admin', 'status'],
    queryFn: getAdminStatus,
    refetchInterval: 10000,
  })

  return (
    <Panel
      title="Status"
      action={
        <Tooltip title="Refresh">
          <IconButton size="small" onClick={() => void refetch()} disabled={isFetching}>
            <RefreshIcon fontSize="small" />
          </IconButton>
        </Tooltip>
      }
    >
      {isError && <Alert severity="error" sx={{ mb: 1 }}>Failed to load status.</Alert>}
      {isLoading && <CircularProgress size={20} />}
      {data && (
        <Box>
          <Field
            label="Status"
            value={
              <Chip
                size="small"
                color={data.status === 'ok' ? 'success' : 'error'}
                label={data.status}
              />
            }
          />
          <Field label="Cloud" value={data.cloud || '—'} />
          {data.backend && <Field label="Backend" value={data.backend} />}
          {data.data_dir && <Field label="Data dir" value={data.data_dir} />}
          {data.kek_fingerprint && <Field label="KEK" value={data.kek_fingerprint} />}
          <Field
            label="Snapshotters"
            value={data.snapshotters?.length ? data.snapshotters.join(', ') : '—'}
          />
        </Box>
      )}
    </Panel>
  )
}

function ClockCard({ notify }: { notify: (f: Feedback) => void }) {
  const queryClient = useQueryClient()
  const { data: clock, isLoading } = useQuery({
    queryKey: ['admin', 'clock'],
    queryFn: getClock,
    refetchInterval: 5000,
  })

  const [open, setOpen] = useState(false)
  const [mode, setMode] = useState<ClockState['mode']>('real')
  const [time, setTime] = useState('')

  const update = useMutation({
    mutationFn: setClock,
    onSuccess: (result, req) => {
      void queryClient.invalidateQueries({ queryKey: ['admin', 'clock'] })
      setOpen(false)
      notify({ severity: 'success', message: `Clock set to ${result.mode ?? req.mode}` })
    },
    onError: (err) => notify({ severity: 'error', message: `Set clock failed: ${(err as Error).message}` }),
  })

  const openDialog = () => {
    setMode(clock?.mode ?? 'real')
    setTime(clock?.time ?? '')
    setOpen(true)
  }

  return (
    <Panel
      title="Clock"
      action={
        <Button size="small" onClick={openDialog}>
          Set clock
        </Button>
      }
    >
      {isLoading && <CircularProgress size={20} />}
      {clock && (
        <Box>
          <Field
            label="Mode"
            value={<Chip size="small" label={clock.mode} color={clock.mode === 'real' ? 'default' : 'primary'} />}
          />
          {clock.time && <Field label="Time" value={<code>{clock.time}</code>} />}
        </Box>
      )}

      <Dialog open={open} onClose={() => setOpen(false)} fullWidth maxWidth="xs">
        <DialogTitle>Set clock mode</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            <TextField
              select
              label="Mode"
              value={mode}
              onChange={(e) => setMode(e.target.value as ClockState['mode'])}
              fullWidth
            >
              {MODE_OPTIONS.map((option) => (
                <MenuItem key={option.value} value={option.value}>
                  {option.label}
                </MenuItem>
              ))}
            </TextField>
            {mode !== 'real' && (
              <TextField
                label="Time"
                helperText="RFC3339, e.g. 2025-01-01T00:00:00Z"
                value={time}
                onChange={(e) => setTime(e.target.value)}
                fullWidth
              />
            )}
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpen(false)}>Cancel</Button>
          <Button
            variant="contained"
            disabled={update.isPending}
            onClick={() => update.mutate(mode === 'real' ? { mode } : { mode, time })}
          >
            Set
          </Button>
        </DialogActions>
      </Dialog>
    </Panel>
  )
}

function ResetCard({ notify }: { notify: (f: Feedback) => void }) {
  const queryClient = useQueryClient()
  const [confirm, setConfirm] = useState(false)

  const reset = useMutation({
    mutationFn: resetState,
    onSuccess: () => {
      void queryClient.invalidateQueries()
      setConfirm(false)
      notify({ severity: 'success', message: 'State reset.' })
    },
    onError: (err) => notify({ severity: 'error', message: `Reset failed: ${(err as Error).message}` }),
  })

  return (
    <Panel title="Reset state">
      <Stack spacing={2}>
        <Typography variant="body2" color="text.secondary">
          Wipe all emulator state. This is irreversible — all resources will be deleted.
        </Typography>
        <Button color="error" variant="outlined" onClick={() => setConfirm(true)}>
          Reset all state…
        </Button>
      </Stack>

      <Dialog open={confirm} onClose={() => setConfirm(false)}>
        <DialogTitle>Reset all state</DialogTitle>
        <DialogContent>Are you sure? This will delete ALL resources.</DialogContent>
        <DialogActions>
          <Button onClick={() => setConfirm(false)}>Cancel</Button>
          <Button
            color="error"
            variant="contained"
            disabled={reset.isPending}
            onClick={() => reset.mutate()}
          >
            Reset
          </Button>
        </DialogActions>
      </Dialog>
    </Panel>
  )
}

function ExportCard() {
  return (
    <Panel title="Export / import">
      <Stack spacing={2}>
        <Typography variant="body2" color="text.secondary">
          Export state as a gzip tarball, or import a snapshot via the CLI.
        </Typography>
        <Box>
          <Button variant="outlined" href="/_jaiscloud/export" download="jaiscloud-state.tar.gz">
            Download export
          </Button>
        </Box>
      </Stack>
    </Panel>
  )
}

function SnapshotsSection({ notify }: { notify: (f: Feedback) => void }) {
  const queryClient = useQueryClient()
  const { data, isLoading, isError } = useQuery({
    queryKey: ['admin', 'snapshots'],
    queryFn: listSnapshots,
  })
  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['admin', 'snapshots'] })

  const [createOpen, setCreateOpen] = useState(false)
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [revertTarget, setRevertTarget] = useState<Snapshot | null>(null)
  const [deleteTarget, setDeleteTarget] = useState<Snapshot | null>(null)

  const create = useMutation({
    mutationFn: () => createSnapshot({ name, description }),
    onSuccess: () => {
      invalidate()
      setCreateOpen(false)
      notify({ severity: 'success', message: `Snapshot "${name}" created.` })
      setName('')
      setDescription('')
    },
    onError: (err) => notify({ severity: 'error', message: `Create failed: ${(err as Error).message}` }),
  })

  const revert = useMutation({
    mutationFn: (snapshot: string) => revertSnapshot(snapshot),
    onSuccess: (_result, snapshot) => {
      invalidate()
      setRevertTarget(null)
      notify({ severity: 'success', message: `Reverted to "${snapshot}".` })
    },
    onError: (err) => notify({ severity: 'error', message: `Revert failed: ${(err as Error).message}` }),
  })

  const remove = useMutation({
    mutationFn: (snapshot: string) => deleteSnapshot(snapshot),
    onSuccess: (_result, snapshot) => {
      invalidate()
      setDeleteTarget(null)
      notify({ severity: 'success', message: `Snapshot "${snapshot}" deleted.` })
    },
    onError: (err) => notify({ severity: 'error', message: `Delete failed: ${(err as Error).message}` }),
  })

  const snapshots = data?.snapshots ?? []

  return (
    <Paper variant="outlined" sx={{ p: 2 }}>
      <Stack direction="row" sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
        <Typography variant="h6">Snapshots</Typography>
        <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
          Create snapshot
        </Button>
      </Stack>

      {isError && <Alert severity="error" sx={{ mb: 2 }}>Failed to load snapshots.</Alert>}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 1 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell>Description</TableCell>
              <TableCell>Created</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {isLoading && (
              <TableRow>
                <TableCell colSpan={4} align="center" sx={{ py: 3 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!isLoading && snapshots.length === 0 && (
              <TableRow>
                <TableCell colSpan={4} align="center" sx={{ py: 3, color: 'text.secondary' }}>
                  No snapshots. Create one to save the current state.
                </TableCell>
              </TableRow>
            )}
            {snapshots.map((snapshot) => (
              <TableRow key={snapshot.name} hover>
                <TableCell>{snapshot.name}</TableCell>
                <TableCell>{snapshot.description || '—'}</TableCell>
                <TableCell>{shortDate(snapshot.createdAt)}</TableCell>
                <TableCell align="right">
                  <Stack direction="row" spacing={1} sx={{ justifyContent: 'flex-end' }}>
                    <Button size="small" onClick={() => setRevertTarget(snapshot)}>
                      Revert
                    </Button>
                    <Tooltip title="Delete snapshot">
                      <IconButton
                        size="small"
                        disabled={remove.isPending}
                        onClick={() => setDeleteTarget(snapshot)}
                        aria-label={`Delete ${snapshot.name}`}
                      >
                        <DeleteOutlineIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </Stack>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      <Dialog open={createOpen} onClose={() => setCreateOpen(false)} fullWidth maxWidth="xs">
        <DialogTitle>Create snapshot</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            <TextField
              autoFocus
              label="Name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="my-snapshot"
              fullWidth
            />
            <TextField
              label="Description"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="Before the big test…"
              fullWidth
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setCreateOpen(false)}>Cancel</Button>
          <Button
            variant="contained"
            disabled={!name || create.isPending}
            onClick={() => create.mutate()}
          >
            Create
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={revertTarget != null} onClose={() => setRevertTarget(null)}>
        <DialogTitle>Revert to snapshot</DialogTitle>
        <DialogContent>
          Current state will be replaced with <b>{revertTarget?.name}</b>. This cannot be undone.
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setRevertTarget(null)}>Cancel</Button>
          <Button
            variant="contained"
            disabled={revert.isPending}
            onClick={() => revertTarget && revert.mutate(revertTarget.name)}
          >
            Revert
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={deleteTarget != null} onClose={() => setDeleteTarget(null)}>
        <DialogTitle>Delete snapshot</DialogTitle>
        <DialogContent>
          Permanently delete snapshot <b>{deleteTarget?.name}</b>?
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteTarget(null)}>Cancel</Button>
          <Button
            color="error"
            variant="contained"
            disabled={remove.isPending}
            onClick={() => deleteTarget && remove.mutate(deleteTarget.name)}
          >
            Delete
          </Button>
        </DialogActions>
      </Dialog>
    </Paper>
  )
}
