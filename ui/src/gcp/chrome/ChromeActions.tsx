import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Chip, Divider, IconButton, ListItemIcon, Menu, MenuItem, Tooltip } from '@mui/material'
import TerminalIcon from '@mui/icons-material/Terminal'
import NotificationsNoneIcon from '@mui/icons-material/NotificationsNone'
import SettingsOutlinedIcon from '@mui/icons-material/SettingsOutlined'
import HelpOutlineOutlinedIcon from '@mui/icons-material/HelpOutlineOutlined'
import ShieldOutlinedIcon from '@mui/icons-material/ShieldOutlined'
import { docsLink } from '../../lib/cloudLinks'

interface MenuState {
  anchor: HTMLElement | null
  kind: 'notifications' | 'settings' | null
}

/** Right-hand console action row: Cloud Shell, notifications, settings, help. */
export function ChromeActions({ connected }: { connected: boolean }) {
  const navigate = useNavigate()
  const docs = docsLink('gcp')
  const [menu, setMenu] = useState<MenuState>({ anchor: null, kind: null })
  const close = () => setMenu({ anchor: null, kind: null })

  return (
    <>
      <Tooltip
        title={connected ? 'Live updates active' : 'Stream disconnected — falling back to polling'}
      >
        <Chip
          size="small"
          label={connected ? 'Live' : 'Polling'}
          color={connected ? 'success' : 'default'}
          variant="outlined"
          sx={{ mr: 1, display: { xs: 'none', sm: 'inline-flex' } }}
        />
      </Tooltip>
      <Tooltip title="Cloud Shell is not available in the emulator">
        <span>
          <IconButton disabled aria-label="Cloud Shell" sx={{ color: 'text.secondary' }}>
            <TerminalIcon />
          </IconButton>
        </span>
      </Tooltip>
      <Tooltip title="Notifications">
        <IconButton
          aria-label="Notifications"
          sx={{ color: 'text.secondary' }}
          onClick={(event) => setMenu({ anchor: event.currentTarget, kind: 'notifications' })}
        >
          <NotificationsNoneIcon />
        </IconButton>
      </Tooltip>
      <Tooltip title="Settings">
        <IconButton
          aria-label="Settings"
          sx={{ color: 'text.secondary' }}
          onClick={(event) => setMenu({ anchor: event.currentTarget, kind: 'settings' })}
        >
          <SettingsOutlinedIcon />
        </IconButton>
      </Tooltip>
      <Tooltip title={docs.label}>
        <IconButton
          aria-label={docs.label}
          component="a"
          href={docs.href}
          target="_blank"
          rel="noreferrer"
          sx={{ color: 'text.secondary' }}
        >
          <HelpOutlineOutlinedIcon />
        </IconButton>
      </Tooltip>

      <Menu anchorEl={menu.anchor} open={Boolean(menu.anchor)} onClose={close}>
        {menu.kind === 'notifications' && <MenuItem disabled>No new notifications</MenuItem>}
        {menu.kind === 'settings' && [
          <MenuItem
            key="admin"
            onClick={() => {
              close()
              navigate('/gcp/admin')
            }}
          >
            <ListItemIcon>
              <ShieldOutlinedIcon fontSize="small" />
            </ListItemIcon>
            Admin panel
          </MenuItem>,
          <Divider key="divider" />,
          <MenuItem key="density" disabled>
            Density — coming soon
          </MenuItem>,
          <MenuItem key="dark" disabled>
            Dark mode — coming soon
          </MenuItem>,
        ]}
      </Menu>
    </>
  )
}
