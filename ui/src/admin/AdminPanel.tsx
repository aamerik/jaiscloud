import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Badge,
  Box,
  Button,
  ColumnLayout,
  Container,
  ContentLayout,
  Form,
  FormField,
  Header,
  Input,
  KeyValuePairs,
  Modal,
  Select,
  SpaceBetween,
  StatusIndicator,
} from '@cloudscape-design/components'
import {
  getAdminStatus,
  resetState,
  getClock,
  setClock,
  listSnapshots,
  createSnapshot,
  revertSnapshot,
  deleteSnapshot,
  type ClockState,
  type Snapshot,
} from '../api/admin'
import { formatDate } from '../lib/date'
import { ResourceTable, type ResourceColumn } from '../components/ResourceTable'
import { useNotifications } from '../components/notifications'

const MODE_OPTIONS = [
  { value: 'real', label: 'real (wall clock)' },
  { value: 'fixed', label: 'fixed (frozen time)' },
  { value: 'offset', label: 'offset (shifted time)' },
]

export function AdminPanel() {
  const qc = useQueryClient()

  const { data: status } = useQuery({
    queryKey: ['admin', 'status'],
    queryFn: getAdminStatus,
    refetchInterval: 10000,
  })

  const { data: clock } = useQuery({
    queryKey: ['admin', 'clock'],
    queryFn: getClock,
    refetchInterval: 5000,
  })

  const { data: snapshotsData } = useQuery({
    queryKey: ['admin', 'snapshots'],
    queryFn: listSnapshots,
  })

  return (
    <ContentLayout
      header={
        <Header variant="h1" description="Status, clock, state reset and named snapshots.">
          Admin
        </Header>
      }
    >
      <SpaceBetween size="l">
        <ColumnLayout columns={2}>
          <StatusCard
            status={status}
            onRefresh={() => void qc.invalidateQueries({ queryKey: ['admin'] })}
          />
          <ClockCard
            clock={clock}
            onChanged={() => void qc.invalidateQueries({ queryKey: ['admin', 'clock'] })}
          />
          <ResetCard />
          <ExportCard />
        </ColumnLayout>
        <SnapshotsSection
          snapshots={snapshotsData?.snapshots ?? []}
          onChanged={() => void qc.invalidateQueries({ queryKey: ['admin', 'snapshots'] })}
        />
      </SpaceBetween>
    </ContentLayout>
  )
}

function StatusCard({
  status,
  onRefresh,
}: {
  status: Awaited<ReturnType<typeof getAdminStatus>> | undefined
  onRefresh: () => void
}) {
  return (
    <Container
      header={
        <Header
          variant="h2"
          actions={
            <Button iconName="refresh" onClick={onRefresh}>
              Refresh
            </Button>
          }
        >
          Status
        </Header>
      }
    >
      {status ? (
        <KeyValuePairs
          columns={1}
          items={[
            {
              label: 'Status',
              value: (
                <StatusIndicator type={status.status === 'ok' ? 'success' : 'error'}>
                  {status.status}
                </StatusIndicator>
              ),
            },
            { label: 'Cloud', value: status.cloud },
            ...(status.backend ? [{ label: 'Backend', value: status.backend }] : []),
            {
              label: 'Snapshotters',
              value: status.snapshotters?.length ? status.snapshotters.join(', ') : '—',
            },
          ]}
        />
      ) : (
        <Box color="text-body-secondary">Loading…</Box>
      )}
    </Container>
  )
}

function ClockCard({ clock, onChanged }: { clock: ClockState | undefined; onChanged: () => void }) {
  const [mode, setMode] = useState<ClockState['mode']>('real')
  const [timeStr, setTimeStr] = useState('')
  const [open, setOpen] = useState(false)
  const { notify } = useNotifications()

  const setMut = useMutation({
    mutationFn: (req: ClockState) => setClock(req),
    onSuccess: (_result, req) => {
      onChanged()
      setOpen(false)
      notify({ type: 'success', header: 'Clock updated', content: req.mode })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Failed to set clock', content: (err as Error).message }),
  })

  const handleSet = () => {
    const req: ClockState = { mode }
    if (mode !== 'real') req.time = timeStr
    setMut.mutate(req)
  }

  return (
    <Container
      header={
        <Header
          variant="h2"
          actions={
            <Button
              onClick={() => {
                setOpen(true)
                setMode(clock?.mode ?? 'real')
                setTimeStr(clock?.time ?? '')
              }}
            >
              Set clock
            </Button>
          }
        >
          Clock
        </Header>
      }
    >
      {clock ? (
        <KeyValuePairs
          columns={1}
          items={[
            {
              label: 'Mode',
              value: <Badge color={clock.mode === 'real' ? 'green' : 'grey'}>{clock.mode}</Badge>,
            },
            ...(clock.time
              ? [{ label: 'Time', value: <Box variant="code">{clock.time}</Box> }]
              : []),
          ]}
        />
      ) : (
        <Box color="text-body-secondary">Loading…</Box>
      )}

      <Modal
        visible={open}
        onDismiss={() => setOpen(false)}
        header="Set clock mode"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setOpen(false)}>
                Cancel
              </Button>
              <Button variant="primary" loading={setMut.isPending} onClick={handleSet}>
                Set
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="m">
            <FormField label="Mode">
              <Select
                selectedOption={MODE_OPTIONS.find((option) => option.value === mode) ?? MODE_OPTIONS[0]!}
                onChange={({ detail }) => setMode((detail.selectedOption.value ?? 'real') as ClockState['mode'])}
                options={MODE_OPTIONS}
              />
            </FormField>
            {mode !== 'real' && (
              <FormField label="Time" description="RFC3339, e.g. 2025-01-01T00:00:00Z">
                <Input
                  autoFocus
                  value={timeStr}
                  onChange={({ detail }) => setTimeStr(detail.value)}
                  placeholder="2025-01-01T00:00:00Z"
                />
              </FormField>
            )}
          </SpaceBetween>
        </Form>
      </Modal>
    </Container>
  )
}

function ResetCard() {
  const [confirm, setConfirm] = useState(false)
  const qc = useQueryClient()
  const { notify } = useNotifications()

  const resetMut = useMutation({
    mutationFn: resetState,
    onSuccess: () => {
      void qc.invalidateQueries()
      setConfirm(false)
      notify({ type: 'success', header: 'State reset' })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Reset failed', content: (err as Error).message }),
  })

  return (
    <Container header={<Header variant="h2">Reset state</Header>}>
      <SpaceBetween size="m">
        <Box color="text-body-secondary">
          Wipe all emulator state. This is irreversible — all resources will be deleted.
        </Box>
        <Button onClick={() => setConfirm(true)}>Reset all state…</Button>
      </SpaceBetween>

      <Modal
        visible={confirm}
        onDismiss={() => setConfirm(false)}
        header="Reset all state"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setConfirm(false)}>
                Cancel
              </Button>
              <Button variant="primary" loading={resetMut.isPending} onClick={() => resetMut.mutate()}>
                Reset
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Are you sure? This will delete ALL resources.
      </Modal>
    </Container>
  )
}

function ExportCard() {
  return (
    <Container header={<Header variant="h2">Export / import</Header>}>
      <SpaceBetween size="m">
        <Box color="text-body-secondary">
          Export state as a gzip tarball, or import a snapshot via the CLI.
        </Box>
        <Button href="/_jaiscloud/export" download="jaiscloud-state.tar.gz">
          Download export
        </Button>
      </SpaceBetween>
    </Container>
  )
}

function SnapshotsSection({
  snapshots,
  onChanged,
}: {
  snapshots: Snapshot[]
  onChanged: () => void
}) {
  const [createOpen, setCreateOpen] = useState(false)
  const [name, setName] = useState('')
  const [desc, setDesc] = useState('')
  const [confirmRevert, setConfirmRevert] = useState<Snapshot | null>(null)
  const [confirmDel, setConfirmDel] = useState<Snapshot | null>(null)
  const { notify } = useNotifications()

  const createMut = useMutation({
    mutationFn: () => createSnapshot({ name, description: desc }),
    onSuccess: () => {
      onChanged()
      setCreateOpen(false)
      setName('')
      setDesc('')
      notify({ type: 'success', header: 'Snapshot created', content: name })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Create failed', content: (err as Error).message }),
  })

  const revertMut = useMutation({
    mutationFn: (n: string) => revertSnapshot(n),
    onSuccess: (_r, n) => {
      onChanged()
      setConfirmRevert(null)
      notify({ type: 'success', header: 'Snapshot reverted', content: n })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Revert failed', content: (err as Error).message }),
  })

  const deleteMut = useMutation({
    mutationFn: (n: string) => deleteSnapshot(n),
    onSuccess: (_r, n) => {
      onChanged()
      setConfirmDel(null)
      notify({ type: 'success', header: 'Snapshot deleted', content: n })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const columns: ResourceColumn<Snapshot>[] = [
    {
      id: 'name',
      header: 'Name',
      filterLabel: 'Name',
      filterValue: (s) => s.name,
      cell: (s) => s.name,
    },
    { id: 'description', header: 'Description', cell: (s) => s.description ?? '—' },
    { id: 'created', header: 'Created', cell: (s) => formatDate(s.createdAt) },
    {
      id: 'actions',
      header: '',
      width: 180,
      cell: (s) => (
        <SpaceBetween direction="horizontal" size="xs">
          <Button onClick={() => setConfirmRevert(s)}>Revert</Button>
          <Button onClick={() => setConfirmDel(s)}>Delete</Button>
        </SpaceBetween>
      ),
    },
  ]

  return (
    <>
      <ResourceTable
        items={snapshots}
        columns={columns}
        trackBy={(s) => s.name}
        title="Snapshots"
        actions={
          <Button variant="primary" onClick={() => setCreateOpen(true)}>
            Create snapshot
          </Button>
        }
        emptyTitle="No snapshots"
        emptyBody="Create one to save the current state."
      />

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create snapshot"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={createMut.isPending}
                disabled={!name}
                onClick={() => createMut.mutate()}
              >
                Create
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="m">
            <FormField label="Name">
              <Input
                autoFocus
                value={name}
                onChange={({ detail }) => setName(detail.value)}
                placeholder="my-snapshot"
              />
            </FormField>
            <FormField label="Description" description="Optional">
              <Input
                value={desc}
                onChange={({ detail }) => setDesc(detail.value)}
                placeholder="Before the big test…"
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={confirmRevert != null}
        onDismiss={() => setConfirmRevert(null)}
        header="Revert to snapshot"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setConfirmRevert(null)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={revertMut.isPending}
                onClick={() => confirmRevert && revertMut.mutate(confirmRevert.name)}
              >
                Revert
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Current state will be replaced with <b>{confirmRevert?.name}</b>. This cannot be undone.
      </Modal>

      <Modal
        visible={confirmDel != null}
        onDismiss={() => setConfirmDel(null)}
        header="Delete snapshot"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setConfirmDel(null)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={deleteMut.isPending}
                onClick={() => confirmDel && deleteMut.mutate(confirmDel.name)}
              >
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Permanently delete snapshot <b>{confirmDel?.name}</b>?
      </Modal>
    </>
  )
}
