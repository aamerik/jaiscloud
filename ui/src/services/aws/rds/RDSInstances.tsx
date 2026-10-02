import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  ButtonDropdown,
  ContentLayout,
  Form,
  FormField,
  Header,
  Input,
  KeyValuePairs,
  Modal,
  SpaceBetween,
  StatusIndicator,
} from '@cloudscape-design/components'
import {
  listInstances,
  createInstance,
  deleteInstance,
  startInstance,
  stopInstance,
  type DBInstance,
} from '../../../api/rds'
import { resourceStatus } from '../../../lib/status'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { useNotifications } from '../../../components/notifications'
import { useSplitPanel } from '../../../components/splitPanel'

export function RDSInstances() {
  const qc = useQueryClient()
  const [selected, setSelected] = useState<DBInstance[]>([])
  const [createOpen, setCreateOpen] = useState(false)
  const [confirmDelete, setConfirmDelete] = useState(false)
  const [form, setForm] = useState({
    id: '',
    engine: 'mysql',
    class: 'db.t3.micro',
    username: 'admin',
    password: '',
  })
  const { notify } = useNotifications()
  const splitPanel = useSplitPanel()

  const showDetails = (instance: DBInstance) => {
    splitPanel.show({
      header: instance.id,
      content: (
        <KeyValuePairs
          columns={1}
          items={[
            { label: 'Identifier', value: instance.id },
            {
              label: 'Status',
              value: (
                <StatusIndicator type={resourceStatus(instance.status)}>
                  {instance.status}
                </StatusIndicator>
              ),
            },
            { label: 'Engine', value: instance.engine },
            { label: 'Class', value: instance.class },
            {
              label: 'Endpoint',
              value: <Box variant="code">{instance.endpoint ? `${instance.endpoint}:${instance.port}` : '—'}</Box>,
            },
          ]}
        />
      ),
    })
  }

  const { data, isLoading, error } = useQuery({
    queryKey: ['rds', 'instances'],
    queryFn: listInstances,
  })

  const invalidate = () => qc.invalidateQueries({ queryKey: ['rds', 'instances'] })

  const create = useMutation({
    mutationFn: () => createInstance(form),
    onSuccess: () => {
      void invalidate()
      notify({ type: 'success', header: 'DB instance created', content: form.id })
      setCreateOpen(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Create failed', content: (err as Error).message }),
  })

  const del = useMutation({
    mutationFn: async (instances: DBInstance[]) => {
      for (const instance of instances) await deleteInstance(instance.id)
    },
    onSuccess: (_r, instances) => {
      void invalidate()
      notify({ type: 'success', header: `Deleted ${instances.length} instance(s)` })
      setSelected([])
      setConfirmDelete(false)
    },
    onError: (err) => notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const start = useMutation({
    mutationFn: async (instances: DBInstance[]) => {
      for (const instance of instances) await startInstance(instance.id)
    },
    onSuccess: (_r, instances) => {
      void invalidate()
      notify({ type: 'success', header: `Starting ${instances.length} instance(s)` })
      setSelected([])
    },
    onError: (err) => notify({ type: 'error', header: 'Start failed', content: (err as Error).message }),
  })

  const stop = useMutation({
    mutationFn: async (instances: DBInstance[]) => {
      for (const instance of instances) await stopInstance(instance.id)
    },
    onSuccess: (_r, instances) => {
      void invalidate()
      notify({ type: 'success', header: `Stopping ${instances.length} instance(s)` })
      setSelected([])
    },
    onError: (err) => notify({ type: 'error', header: 'Stop failed', content: (err as Error).message }),
  })

  const items = data?.items ?? []

  const columns: ResourceColumn<DBInstance>[] = [
    { id: 'id', header: 'Identifier', filterLabel: 'Identifier', filterValue: (i) => i.id, cell: (i) => i.id },
    {
      id: 'status',
      header: 'Status',
      filterLabel: 'Status',
      filterValue: (i) => i.status,
      cell: (i) => <StatusIndicator type={resourceStatus(i.status)}>{i.status}</StatusIndicator>,
    },
    { id: 'engine', header: 'Engine', filterLabel: 'Engine', filterValue: (i) => i.engine, cell: (i) => i.engine },
    { id: 'class', header: 'Class', cell: (i) => i.class },
    {
      id: 'endpoint',
      header: 'Endpoint',
      cell: (i) => <Box variant="code">{i.endpoint ? `${i.endpoint}:${i.port}` : '—'}</Box>,
    },
  ]

  const hasStopped = selected.some((i) => i.status === 'stopped')
  const hasAvailable = selected.some((i) => i.status === 'available')

  return (
    <ContentLayout header={<Header variant="h1">RDS instances</Header>}>
      {error ? (
        <Alert type="error" header="Failed to load RDS instances">
          {(error as Error).message}
        </Alert>
      ) : (
        <ResourceTable
          items={items}
          columns={columns}
          trackBy={(i) => i.id}
          title="Databases"
          description="metadata only"
          loading={isLoading}
          onRowClick={showDetails}
          selectionType="multi"
          selectedItems={selected}
          onSelectionChange={(items) => {
            setSelected(items)
            if (items.length === 1) showDetails(items[0]!)
          }}
          actions={
            <SpaceBetween direction="horizontal" size="xs">
              <ButtonDropdown
                items={[
                  { id: 'start', text: 'Start', disabled: !hasStopped },
                  { id: 'stop', text: 'Stop', disabled: !hasAvailable },
                  { id: 'delete', text: 'Delete', disabled: selected.length === 0 },
                ]}
                onItemClick={({ detail }) => {
                  if (detail.id === 'start') start.mutate(selected)
                  else if (detail.id === 'stop') stop.mutate(selected)
                  else setConfirmDelete(true)
                }}
                disabled={selected.length === 0}
              >
                Actions
              </ButtonDropdown>
              <Button variant="primary" onClick={() => setCreateOpen(true)}>
                Create database
              </Button>
            </SpaceBetween>
          }
          emptyTitle="No databases"
          emptyBody="Create an RDS instance to get started."
        />
      )}

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create database"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={create.isPending}
                disabled={!form.id.trim()}
                onClick={() => create.mutate()}
              >
                Create database
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="m">
            <FormField label="DB identifier">
              <Input value={form.id} onChange={({ detail }) => setForm({ ...form, id: detail.value })} placeholder="my-db" />
            </FormField>
            <FormField label="Engine">
              <Input value={form.engine} onChange={({ detail }) => setForm({ ...form, engine: detail.value })} />
            </FormField>
            <FormField label="DB instance class">
              <Input value={form.class} onChange={({ detail }) => setForm({ ...form, class: detail.value })} />
            </FormField>
            <FormField label="Master username">
              <Input value={form.username} onChange={({ detail }) => setForm({ ...form, username: detail.value })} />
            </FormField>
            <FormField label="Master password">
              <Input type="password" value={form.password} onChange={({ detail }) => setForm({ ...form, password: detail.value })} />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={confirmDelete}
        onDismiss={() => setConfirmDelete(false)}
        header="Delete databases"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setConfirmDelete(false)}>
                Cancel
              </Button>
              <Button variant="primary" loading={del.isPending} onClick={() => del.mutate(selected)}>
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Permanently delete {selected.length} database{selected.length !== 1 ? 's' : ''}?
      </Modal>
    </ContentLayout>
  )
}
