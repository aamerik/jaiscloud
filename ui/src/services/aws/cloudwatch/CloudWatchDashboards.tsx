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
  Modal,
  SpaceBetween,
  Spinner,
} from '@cloudscape-design/components'
import {
  listDashboards,
  getDashboard,
  putDashboard,
  deleteDashboard,
  type CWDashboard,
} from '../../../api/cloudwatch'
import { formatDate } from '../../../lib/date'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { JsonEditor } from '../../../components/JsonEditor'
import { useNotifications } from '../../../components/notifications'

const DEFAULT_BODY = JSON.stringify({ widgets: [] }, null, 2)

export function CloudWatchDashboards() {
  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [newBody, setNewBody] = useState(DEFAULT_BODY)
  const [viewDash, setViewDash] = useState<CWDashboard | null>(null)
  const [viewBody, setViewBody] = useState('')
  const [deleteTarget, setDeleteTarget] = useState<CWDashboard | null>(null)
  const [loadingView, setLoadingView] = useState(false)

  const qc = useQueryClient()
  const { notify } = useNotifications()

  const { data, isLoading, error } = useQuery({
    queryKey: ['cloudwatch', 'dashboards'],
    queryFn: listDashboards,
  })

  const createMut = useMutation({
    mutationFn: () => putDashboard({ dashboardName: newName, dashboardBody: newBody }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['cloudwatch', 'dashboards'] })
      notify({ type: 'success', header: 'Dashboard created', content: newName })
      setCreateOpen(false)
      setNewName('')
      setNewBody(DEFAULT_BODY)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Could not create dashboard', content: (err as Error).message }),
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteDashboard(name),
    onSuccess: (_result, name) => {
      void qc.invalidateQueries({ queryKey: ['cloudwatch', 'dashboards'] })
      notify({ type: 'success', header: 'Dashboard deleted', content: name })
      setDeleteTarget(null)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Could not delete dashboard', content: (err as Error).message }),
  })

  async function viewDashboard(d: CWDashboard) {
    setViewDash(d)
    setLoadingView(true)
    setViewBody('')
    try {
      const full = await getDashboard(d.dashboardName)
      try {
        setViewBody(JSON.stringify(JSON.parse(full.dashboardBody ?? '{}'), null, 2))
      } catch {
        setViewBody(full.dashboardBody ?? '')
      }
    } catch {
      setViewBody('Failed to load dashboard body')
    } finally {
      setLoadingView(false)
    }
  }

  const dashboards = data?.items ?? []

  const columns: ResourceColumn<CWDashboard>[] = [
    {
      id: 'name',
      header: 'Name',
      filterLabel: 'Name',
      filterValue: (d) => d.dashboardName,
      cell: (d) => d.dashboardName,
    },
    {
      id: 'lastModified',
      header: 'Last modified',
      cell: (d) => formatDate(d.lastModified),
    },
    {
      id: 'rowActions',
      header: '',
      cell: (d) => (
        <ButtonDropdown
          variant="icon"
          ariaLabel={`Actions for ${d.dashboardName}`}
          items={[
            { id: 'view', text: 'View' },
            { id: 'delete', text: 'Delete' },
          ]}
          onItemClick={({ detail }) => {
            if (detail.id === 'view') void viewDashboard(d)
            else if (detail.id === 'delete') setDeleteTarget(d)
          }}
        />
      ),
    },
  ]

  return (
    <ContentLayout header={<Header variant="h1">CloudWatch dashboards</Header>}>
      {error ? (
        <Alert type="error" header="Failed to load dashboards">
          {(error as Error).message}
        </Alert>
      ) : (
        <ResourceTable
          items={dashboards}
          columns={columns}
          trackBy={(d) => d.dashboardName}
          title="Dashboards"
          loading={isLoading}
          actions={
            <Button variant="primary" onClick={() => setCreateOpen(true)}>
              Create dashboard
            </Button>
          }
          emptyTitle="No dashboards"
          emptyBody="Create a dashboard to visualize your CloudWatch metrics."
        />
      )}

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create dashboard"
        size="large"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={createMut.isPending}
                disabled={!newName}
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
            <FormField label="Dashboard name">
              <Input
                autoFocus
                value={newName}
                onChange={({ detail }) => setNewName(detail.value)}
                placeholder="my-dashboard"
              />
            </FormField>
            <FormField label="Dashboard body" description="CloudWatch dashboard definition as JSON.">
              <JsonEditor value={newBody} onChange={setNewBody} height={280} ariaLabel="Dashboard body" />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={viewDash != null}
        onDismiss={() => setViewDash(null)}
        header={viewDash?.dashboardName ?? 'Dashboard'}
        size="large"
        footer={
          <Box float="right">
            <Button variant="link" onClick={() => setViewDash(null)}>
              Close
            </Button>
          </Box>
        }
      >
        {loadingView ? (
          <Spinner size="large" />
        ) : (
          <JsonEditor value={viewBody} onChange={() => {}} height={360} ariaLabel="Dashboard body" />
        )}
      </Modal>

      <Modal
        visible={deleteTarget != null}
        onDismiss={() => setDeleteTarget(null)}
        header="Delete dashboard"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setDeleteTarget(null)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={deleteMut.isPending}
                onClick={() => deleteTarget && deleteMut.mutate(deleteTarget.dashboardName)}
              >
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Delete <b>{deleteTarget?.dashboardName}</b>? This action cannot be undone.
      </Modal>
    </ContentLayout>
  )
}
