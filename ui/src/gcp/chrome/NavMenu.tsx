import { useEffect, useMemo, useRef, useState } from 'react'
import { Link as RouterLink, useLocation } from 'react-router-dom'
import {
  Box,
  Collapse,
  Divider,
  Drawer,
  IconButton,
  InputAdornment,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Stack,
  TextField,
  Toolbar,
  Typography,
} from '@mui/material'
import SearchIcon from '@mui/icons-material/Search'
import StarIcon from '@mui/icons-material/Star'
import StarBorderIcon from '@mui/icons-material/StarBorder'
import ExpandLessIcon from '@mui/icons-material/ExpandLess'
import ExpandMoreIcon from '@mui/icons-material/ExpandMore'
import SettingsOutlinedIcon from '@mui/icons-material/SettingsOutlined'
import CloseIcon from '@mui/icons-material/Close'
import type { ServiceDescriptor } from '../../api/services'
import { GcpServiceIcon } from '../icons/GcpServiceIcon'
import { useFavorites } from '../../hooks/useFavorites'
import { buildNavGroups, type NavEntry } from './navModel'
import { rememberRecentService, useRecentServices } from './recentServices'

const NAV_MENU_WIDTH = 360

/** Children that add a real sub-nav (a lone child equal to the root is noise). */
function subNav(entry: NavEntry) {
  return entry.children.filter((child) => child.path !== entry.path)
}

interface NavMenuProps {
  open: boolean
  onClose: () => void
  services: ServiceDescriptor[]
}

/** Searchable, pinnable navigation-menu overlay mirroring the GCP console. */
export function NavMenu({ open, onClose, services }: NavMenuProps) {
  const location = useLocation()
  const { favorites, isFavorite, toggle } = useFavorites()
  const recent = useRecentServices()
  const [query, setQuery] = useState('')
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set())
  const searchRef = useRef<HTMLInputElement>(null)

  const groups = useMemo(
    () => buildNavGroups(services, favorites, recent, query),
    [services, favorites, recent, query],
  )

  useEffect(() => {
    if (open) searchRef.current?.focus()
  }, [open])

  const isSelected = (path: string) =>
    location.pathname === path || location.pathname.startsWith(`${path}/`)

  const select = (id: string) => {
    rememberRecentService(id)
    onClose()
  }

  const toggleExpanded = (id: string) => {
    setExpanded((current) => {
      const next = new Set(current)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  return (
    <Drawer
      anchor="left"
      open={open}
      onClose={onClose}
      ModalProps={{ keepMounted: true }}
      sx={{
        '& .MuiDrawer-paper': {
          width: NAV_MENU_WIDTH,
          maxWidth: '100vw',
          boxSizing: 'border-box',
        },
      }}
    >
      <Toolbar sx={{ gap: 1 }}>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <Typography variant="overline" color="text.secondary">
            JaisCloud
          </Typography>
          <Typography variant="subtitle1" sx={{ lineHeight: 1.1 }}>
            Navigation menu
          </Typography>
        </Box>
        <IconButton edge="end" aria-label="Close navigation menu" onClick={onClose}>
          <CloseIcon />
        </IconButton>
      </Toolbar>
      <Divider />
      <Box sx={{ px: 2, py: 1.5 }}>
        <TextField
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Search for products and services"
          size="small"
          fullWidth
          inputRef={searchRef}
          slotProps={{
            input: {
              startAdornment: (
                <InputAdornment position="start">
                  <SearchIcon fontSize="small" sx={{ color: 'text.secondary' }} />
                </InputAdornment>
              ),
            },
            htmlInput: { 'aria-label': 'Search navigation' },
          }}
        />
      </Box>
      <Divider />
      <List component="nav" sx={{ overflowY: 'auto', pb: 2 }}>
        <ListItem disablePadding>
          <ListItemButton
            component={RouterLink}
            to="/gcp"
            selected={location.pathname === '/gcp'}
            onClick={onClose}
          >
            <ListItemText primary="Console home" />
          </ListItemButton>
        </ListItem>
        {groups.length === 0 && (
          <Box sx={{ px: 2, py: 3 }}>
            <Typography variant="body2" color="text.secondary">
              No matching services
            </Typography>
          </Box>
        )}
        {groups.map((group) => (
          <Box key={`${group.kind}:${group.label}`}>
            <Typography
              variant="overline"
              sx={{ display: 'block', px: 2, pt: 1.5, color: 'text.secondary' }}
            >
              {group.label}
            </Typography>
            {group.entries.map((entry) => {
              const children = subNav(entry)
              const isOpen = expanded.has(entry.id)
              return (
                <Box key={entry.id}>
                  <ListItem
                    disablePadding
                    secondaryAction={
                      <Stack direction="row" spacing={0.25}>
                        {children.length > 0 && (
                          <IconButton
                            size="small"
                            aria-label={`Toggle ${entry.label} sections`}
                            aria-expanded={isOpen}
                            onClick={() => toggleExpanded(entry.id)}
                          >
                            {isOpen ? (
                              <ExpandLessIcon fontSize="small" />
                            ) : (
                              <ExpandMoreIcon fontSize="small" />
                            )}
                          </IconButton>
                        )}
                        <IconButton
                          size="small"
                          aria-label={
                            isFavorite(entry.id)
                              ? `Unpin ${entry.label}`
                              : `Pin ${entry.label}`
                          }
                          onClick={() => toggle(entry.id)}
                        >
                          {isFavorite(entry.id) ? (
                            <StarIcon fontSize="small" color="primary" />
                          ) : (
                            <StarBorderIcon fontSize="small" />
                          )}
                        </IconButton>
                      </Stack>
                    }
                  >
                    <ListItemButton
                      component={RouterLink}
                      to={entry.path}
                      selected={isSelected(entry.path)}
                      onClick={() => select(entry.id)}
                      sx={{ pr: children.length > 0 ? 10 : 7 }}
                    >
                      <ListItemIcon sx={{ minWidth: 36 }}>
                        <GcpServiceIcon id={entry.id} size={20} />
                      </ListItemIcon>
                      <ListItemText primary={entry.label} secondary={entry.category} />
                    </ListItemButton>
                  </ListItem>
                  {children.length > 0 && (
                    <Collapse in={isOpen} timeout="auto" unmountOnExit>
                      <List disablePadding>
                        {children.map((child) => (
                          <ListItem key={child.path} disablePadding>
                            <ListItemButton
                              component={RouterLink}
                              to={child.path}
                              selected={isSelected(child.path)}
                              onClick={() => select(entry.id)}
                              sx={{ pl: 5 }}
                            >
                              <ListItemText primary={child.label} />
                            </ListItemButton>
                          </ListItem>
                        ))}
                      </List>
                    </Collapse>
                  )}
                </Box>
              )
            })}
          </Box>
        ))}
      </List>
      <Divider />
      <List sx={{ py: 1 }}>
        <ListItem disablePadding>
          <ListItemButton
            component={RouterLink}
            to="/gcp/admin"
            selected={isSelected('/gcp/admin')}
            onClick={onClose}
          >
            <ListItemIcon>
              <SettingsOutlinedIcon fontSize="small" />
            </ListItemIcon>
            <ListItemText primary="Admin" />
          </ListItemButton>
        </ListItem>
      </List>
    </Drawer>
  )
}
