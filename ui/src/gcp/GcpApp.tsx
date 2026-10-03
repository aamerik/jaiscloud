import { useState } from 'react'
import {
  AppBar,
  Box,
  Button,
  Chip,
  CssBaseline,
  Divider,
  Drawer,
  IconButton,
  List,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Menu,
  MenuItem,
  ThemeProvider,
  Toolbar,
  Tooltip,
  Typography,
} from '@mui/material'
import MenuIcon from '@mui/icons-material/Menu'
import HelpOutlineIcon from '@mui/icons-material/HelpOutlineOutlined'
import SettingsOutlinedIcon from '@mui/icons-material/SettingsOutlined'
import { Link as RouterLink, Navigate, Route, Routes, useLocation } from 'react-router-dom'
import '@fontsource/roboto/400.css'
import '@fontsource/roboto/500.css'
import '@fontsource/roboto/700.css'
import { gcpTheme } from './theme'
import { GcpHome } from './GcpHome'
import { GcpAdminPage } from './admin/GcpAdminPage'
import { BucketsPage } from './storage/BucketsPage'
import { ObjectsPage } from './storage/ObjectsPage'
import { BucketSettingsPage } from './storage/BucketSettingsPage'
import { TopicsPage } from './pubsub/TopicsPage'
import { TopicDetailPage } from './pubsub/TopicDetailPage'
import { SubscriptionsPage } from './pubsub/SubscriptionsPage'
import { SubscriptionDetailPage } from './pubsub/SubscriptionDetailPage'
import { CollectionsPage } from './firestore/CollectionsPage'
import { DocumentsPage } from './firestore/DocumentsPage'
import { DocumentDetailPage } from './firestore/DocumentDetailPage'
import { InstancesPage } from './compute/InstancesPage'
import { InstanceDetailPage } from './compute/InstanceDetailPage'
import { ServicesPage as RunServicesPage } from './run/ServicesPage'
import { ServiceDetailPage as RunServiceDetailPage } from './run/ServiceDetailPage'
import { RevisionDetailPage as RunRevisionDetailPage } from './run/RevisionDetailPage'
import { JobsPage as SchedulerJobsPage } from './scheduler/JobsPage'
import { JobDetailPage as SchedulerJobDetailPage } from './scheduler/JobDetailPage'
import { QueuesPage as TasksQueuesPage } from './tasks/QueuesPage'
import { QueueDetailPage as TasksQueueDetailPage } from './tasks/QueueDetailPage'
import { DatasetsPage } from './bigquery/DatasetsPage'
import { DatasetDetailPage } from './bigquery/DatasetDetailPage'
import { TableDetailPage } from './bigquery/TableDetailPage'
import { JobsPage } from './bigquery/JobsPage'
import { JobDetailPage } from './bigquery/JobDetailPage'
import { ServiceAccountsPage } from './iam/ServiceAccountsPage'
import { ServiceAccountDetailPage } from './iam/ServiceAccountDetailPage'
import { KeyRingsPage } from './kms/KeyRingsPage'
import { KeyRingDetailPage } from './kms/KeyRingDetailPage'
import { CryptoKeyDetailPage } from './kms/CryptoKeyDetailPage'
import { SecretsPage } from './secretmanager/SecretsPage'
import { SecretDetailPage } from './secretmanager/SecretDetailPage'
import { LogsExplorer } from './logging/LogsExplorer'
import { MetricsPage as LoggingMetricsPage } from './logging/MetricsPage'
import { SinksPage } from './logging/SinksPage'
import { ExclusionsPage } from './logging/ExclusionsPage'
import { MetricsExplorer } from './monitoring/MetricsExplorer'
import { AlertingPage } from './monitoring/AlertingPage'
import { ChannelsPage } from './monitoring/ChannelsPage'
import { AccountProvider, useAccount, useAccounts } from '../context/AccountContext'
import { useEventStream } from '../hooks/useEventStream'
import { useMeta } from '../hooks/useMeta'
import { useServices } from '../hooks/useServices'
import { docsLink } from '../lib/cloudLinks'

const DRAWER_WIDTH = 256

/** Material shell approximating the Google Cloud Console chrome. */
function GcpShell() {
  const location = useLocation()
  const { data: meta } = useMeta()
  const { accountId, setAccountId } = useAccount()
  const { data: accountsData } = useAccounts()
  const { data: servicesData } = useServices()
  const services = servicesData?.services ?? []
  const accounts = accountsData?.accounts ?? (accountId ? [accountId] : [])
  const docs = docsLink('gcp')
  const { connected } = useEventStream()

  const [mobileOpen, setMobileOpen] = useState(false)
  const [projectAnchor, setProjectAnchor] = useState<HTMLElement | null>(null)

  const isSelected = (path: string) =>
    location.pathname === path || location.pathname.startsWith(`${path}/`)

  const navigation = (
    <Box role="navigation">
      <Toolbar />
      <Box sx={{ px: 2, py: 1.5 }}>
        <Typography variant="overline" color="text.secondary">
          JaisCloud
        </Typography>
        <Typography variant="subtitle1">Console</Typography>
      </Box>
      <Divider />
      <List sx={{ py: 1 }}>
        <ListItemButton
          component={RouterLink}
          to="/gcp"
          selected={location.pathname === '/gcp'}
          onClick={() => setMobileOpen(false)}
        >
          <ListItemText primary="Console home" />
        </ListItemButton>
        {services.map((service) => (
          <ListItemButton
            key={service.id}
            component={RouterLink}
            to={service.rootPath}
            selected={isSelected(service.rootPath)}
            onClick={() => setMobileOpen(false)}
          >
            <ListItemText primary={service.label} secondary={service.category} />
          </ListItemButton>
        ))}
      </List>
      <Divider />
      <List sx={{ py: 1 }}>
        <ListItemButton
          component={RouterLink}
          to="/gcp/admin"
          selected={isSelected('/gcp/admin')}
          onClick={() => setMobileOpen(false)}
        >
          <ListItemIcon>
            <SettingsOutlinedIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText primary="Admin" />
        </ListItemButton>
      </List>
    </Box>
  )

  return (
    <Box sx={{ display: 'flex' }}>
      <AppBar position="fixed" sx={{ zIndex: (theme) => theme.zIndex.drawer + 1 }}>
        <Toolbar>
          <IconButton
            edge="start"
            color="inherit"
            aria-label="Toggle navigation"
            onClick={() => setMobileOpen((open) => !open)}
            sx={{ mr: 1, display: { md: 'none' }, color: 'text.primary' }}
          >
            <MenuIcon />
          </IconButton>
          <Box sx={{ display: 'flex', alignItems: 'center', flexGrow: 1, minWidth: 0, mr: 2 }}>
            <Typography
              component={RouterLink}
              to="/gcp"
              variant="h6"
              noWrap
              sx={{
                color: 'text.primary',
                textDecoration: 'none',
                fontWeight: 500,
                minWidth: 0,
                overflow: 'hidden',
                textOverflow: 'ellipsis',
              }}
            >
              JaisCloud
            </Typography>
          </Box>
          <Button
            color="inherit"
            sx={{ color: 'text.primary', minWidth: 0, flexShrink: 1 }}
            onClick={(event) => setProjectAnchor(event.currentTarget)}
          >
            <Box
              component="span"
              sx={{
                display: 'block',
                maxWidth: { xs: 120, sm: 280 },
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}
            >
              {accountId || meta?.accountId || 'Project'}
            </Box>
          </Button>
          <Menu
            anchorEl={projectAnchor}
            open={Boolean(projectAnchor)}
            onClose={() => setProjectAnchor(null)}
          >
            {accounts.length === 0 && <MenuItem disabled>(no projects)</MenuItem>}
            {accounts.map((account) => (
              <MenuItem
                key={account}
                selected={account === accountId}
                onClick={() => {
                  setAccountId(account)
                  setProjectAnchor(null)
                }}
              >
                {account}
              </MenuItem>
            ))}
          </Menu>
          <Tooltip
            title={
              connected
                ? 'Live updates active'
                : 'Stream disconnected — falling back to polling'
            }
          >
            <Chip
              size="small"
              label={connected ? 'Live' : 'Polling'}
              color={connected ? 'success' : 'default'}
              variant="outlined"
              sx={{ mr: 1 }}
            />
          </Tooltip>
          <Tooltip title={docs.label}>
            <IconButton
              color="inherit"
              component="a"
              href={docs.href}
              target="_blank"
              rel="noreferrer"
              sx={{ color: 'text.secondary' }}
            >
              <HelpOutlineIcon />
            </IconButton>
          </Tooltip>
        </Toolbar>
      </AppBar>

      <Box component="nav" sx={{ width: { md: DRAWER_WIDTH }, flexShrink: { md: 0 } }}>
        <Drawer
          variant="temporary"
          open={mobileOpen}
          onClose={() => setMobileOpen(false)}
          ModalProps={{ keepMounted: true }}
          sx={{
            display: { xs: 'block', md: 'none' },
            '& .MuiDrawer-paper': { width: DRAWER_WIDTH, boxSizing: 'border-box' },
          }}
        >
          {navigation}
        </Drawer>
        <Drawer
          variant="permanent"
          open
          sx={{
            display: { xs: 'none', md: 'block' },
            '& .MuiDrawer-paper': { width: DRAWER_WIDTH, boxSizing: 'border-box' },
          }}
        >
          {navigation}
        </Drawer>
      </Box>

      <Box
        component="main"
        sx={{
          flexGrow: 1,
          minWidth: 0,
          p: 3,
          bgcolor: 'background.default',
          minHeight: '100vh',
          width: { md: `calc(100% - ${DRAWER_WIDTH}px)` },
        }}
      >
        <Toolbar />
        <Routes>
          <Route path="/gcp" element={<GcpHome />} />
          <Route path="/gcp/storage/buckets" element={<BucketsPage />} />
          <Route path="/gcp/storage/buckets/:bucket" element={<ObjectsPage />} />
          <Route path="/gcp/storage/buckets/:bucket/settings" element={<BucketSettingsPage />} />
          <Route path="/gcp/pubsub/topics" element={<TopicsPage />} />
          <Route path="/gcp/pubsub/topics/:topic" element={<TopicDetailPage />} />
          <Route path="/gcp/pubsub/subscriptions" element={<SubscriptionsPage />} />
          <Route path="/gcp/pubsub/subscriptions/:subscription" element={<SubscriptionDetailPage />} />
          <Route path="/gcp/firestore/collections" element={<CollectionsPage />} />
          <Route path="/gcp/firestore/collections/:collection" element={<DocumentsPage />} />
          <Route
            path="/gcp/firestore/collections/:collection/documents/:document"
            element={<DocumentDetailPage />}
          />
          <Route path="/gcp/compute/instances" element={<InstancesPage />} />
          <Route path="/gcp/compute/instances/:zone/:instance" element={<InstanceDetailPage />} />
          <Route path="/gcp/run/services" element={<RunServicesPage />} />
          <Route path="/gcp/run/services/:region/:service" element={<RunServiceDetailPage />} />
          <Route
            path="/gcp/run/services/:region/:service/revisions/:revision"
            element={<RunRevisionDetailPage />}
          />
          <Route path="/gcp/scheduler/jobs" element={<SchedulerJobsPage />} />
          <Route
            path="/gcp/scheduler/jobs/:location/:job"
            element={<SchedulerJobDetailPage />}
          />
          <Route path="/gcp/tasks/queues" element={<TasksQueuesPage />} />
          <Route
            path="/gcp/tasks/queues/:location/:queue"
            element={<TasksQueueDetailPage />}
          />
          <Route path="/gcp/bigquery/datasets" element={<DatasetsPage />} />
          <Route path="/gcp/bigquery/datasets/:dataset" element={<DatasetDetailPage />} />
          <Route
            path="/gcp/bigquery/datasets/:dataset/tables/:table"
            element={<TableDetailPage />}
          />
          <Route path="/gcp/bigquery/jobs" element={<JobsPage />} />
          <Route path="/gcp/bigquery/jobs/:job" element={<JobDetailPage />} />
          <Route path="/gcp/iam/service-accounts" element={<ServiceAccountsPage />} />
          <Route path="/gcp/iam/service-accounts/:email" element={<ServiceAccountDetailPage />} />
          <Route path="/gcp/kms/keyrings" element={<KeyRingsPage />} />
          <Route path="/gcp/kms/keyrings/:location/:keyRing" element={<KeyRingDetailPage />} />
          <Route
            path="/gcp/kms/keyrings/:location/:keyRing/keys/:key"
            element={<CryptoKeyDetailPage />}
          />
          <Route path="/gcp/secretmanager/secrets" element={<SecretsPage />} />
          <Route path="/gcp/secretmanager/secrets/:secret" element={<SecretDetailPage />} />
          <Route path="/gcp/logging/entries" element={<LogsExplorer />} />
          <Route path="/gcp/logging/metrics" element={<LoggingMetricsPage />} />
          <Route path="/gcp/logging/sinks" element={<SinksPage />} />
          <Route path="/gcp/logging/exclusions" element={<ExclusionsPage />} />
          <Route path="/gcp/monitoring/metrics" element={<MetricsExplorer />} />
          <Route path="/gcp/monitoring/alerting" element={<AlertingPage />} />
          <Route path="/gcp/monitoring/channels" element={<ChannelsPage />} />
          <Route path="/gcp/admin" element={<GcpAdminPage />} />
          <Route path="*" element={<Navigate to="/gcp" replace />} />
        </Routes>
      </Box>
    </Box>
  )
}

/** GCP console: Material Design, rendered only when meta.cloud === 'gcp'. */
export function GcpApp() {
  return (
    <ThemeProvider theme={gcpTheme}>
      <CssBaseline />
      <AccountProvider>
        <GcpShell />
      </AccountProvider>
    </ThemeProvider>
  )
}
