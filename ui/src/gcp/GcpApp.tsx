import { useState } from 'react'
import {
  AppBar,
  Box,
  Button,
  CssBaseline,
  Divider,
  Drawer,
  IconButton,
  List,
  ListItemButton,
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
import { Link as RouterLink, Navigate, Route, Routes, useLocation } from 'react-router-dom'
import '@fontsource/roboto/400.css'
import '@fontsource/roboto/500.css'
import '@fontsource/roboto/700.css'
import { gcpTheme } from './theme'
import { GcpHome } from './GcpHome'
import { BucketsPage } from './storage/BucketsPage'
import { ObjectsPage } from './storage/ObjectsPage'
import { AccountProvider, useAccount, useAccounts } from '../context/AccountContext'
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

  const [mobileOpen, setMobileOpen] = useState(false)
  const [projectAnchor, setProjectAnchor] = useState<HTMLElement | null>(null)

  const isSelected = (path: string) =>
    location.pathname === path || location.pathname.startsWith(`${path}/`)

  const navigation = (
    <Box role="navigation">
      <Toolbar />
      <Box sx={{ px: 2, py: 1.5 }}>
        <Typography variant="overline" color="text.secondary">
          Google Cloud
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
          <Typography
            component={RouterLink}
            to="/gcp"
            variant="h6"
            sx={{ color: 'text.primary', textDecoration: 'none', fontWeight: 500, mr: 3 }}
          >
            Google Cloud
          </Typography>
          <Box sx={{ flexGrow: 1 }} />
          <Button
            color="inherit"
            sx={{ color: 'text.primary' }}
            onClick={(event) => setProjectAnchor(event.currentTarget)}
          >
            {accountId || meta?.accountId || 'Project'}
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
