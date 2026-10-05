import { useState, type ReactNode } from 'react'
import { Box, Chip, IconButton, Popover, Stack, Tooltip, Typography } from '@mui/material'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import HelpOutlineIcon from '@mui/icons-material/HelpOutlineOutlined'
import StarIcon from '@mui/icons-material/Star'
import StarBorderIcon from '@mui/icons-material/StarBorder'
import { Link as RouterLink } from 'react-router-dom'
import { useFavorites } from '../../hooks/useFavorites'
import { useServices } from '../../hooks/useServices'
import { engineActive, engineModes, engineTag } from '../../lib/engine'
import { tierDescription, tierLabel } from '../../lib/tier'
import { GcpServiceIcon } from '../icons/GcpServiceIcon'
import { serviceAccent } from '../icons/serviceIcons'
import { serviceDocsHref } from './serviceDocs'

export interface GcpPageHeaderProps {
  /** Service descriptor id driving the product glyph, accent, docs link and pin. */
  id: string
  /** Page title (may be a long resource name). */
  title: ReactNode
  /** Secondary line under the title, e.g. `Cloud Storage · project abc`. */
  subtitle?: ReactNode
  /** Route to navigate to when the back button is shown. */
  backTo?: string
  /** Accessible label for the back button (defaults to `Back`). */
  backAriaLabel?: string
  /** Explicit docs URL; defaults to the per-service docs map. */
  docsHref?: string
  /** Right-aligned action controls (create buttons, etc.). */
  actions?: ReactNode
  /** Content rendered below the title row (e.g. a `GcpTabs` bar). */
  children?: ReactNode
}

/**
 * Shared page heading mirroring the real console: product glyph, title, a pin
 * toggle (shared with the navigation menu) and a docs link, with actions on the
 * right. Supersedes the narrower `GcpPageTitle` used by pages not yet migrated.
 */
export function GcpPageHeader({
  id,
  title,
  subtitle,
  backTo,
  backAriaLabel = 'Back',
  docsHref,
  actions,
  children,
}: GcpPageHeaderProps) {
  const { isFavorite, toggle } = useFavorites()
  const pinned = isFavorite(id)
  const { data: servicesData } = useServices()
  const service = servicesData?.services.find((candidate) => candidate.id === id)
  const status = service ? tierLabel(service) : undefined
  const statusDetail = service ? tierDescription(service) : undefined
  const engine = service ? engineTag(service) : undefined
  const engineIsActive = service ? engineActive(service) : false
  const backends = service ? engineModes(service) : []
  const [engineAnchor, setEngineAnchor] = useState<HTMLElement | null>(null)

  return (
    <Box sx={{ mb: 2 }}>
      <Stack
        direction="row"
        spacing={1.25}
        sx={{ alignItems: 'center', flexWrap: 'wrap', rowGap: 1 }}
      >
        {backTo && (
          <IconButton
            component={RouterLink}
            to={backTo}
            size="small"
            aria-label={backAriaLabel}
            sx={{ ml: -0.5 }}
          >
            <ArrowBackIcon fontSize="small" />
          </IconButton>
        )}
        <Box sx={{ display: 'flex', color: serviceAccent(id) }}>
          <GcpServiceIcon id={id} size={28} />
        </Box>
        <Box sx={{ minWidth: 0, flexGrow: 1 }}>
          <Stack direction="row" spacing={0.5} sx={{ alignItems: 'center', minWidth: 0 }}>
            <Typography variant="h5" sx={{ overflowWrap: 'anywhere', minWidth: 0 }}>
              {title}
            </Typography>
            {status && (
              <Tooltip title={statusDetail ?? status}>
                <Chip
                  label={status}
                  size="small"
                  variant="outlined"
                  sx={{ height: 20, fontSize: 11, flexShrink: 0 }}
                />
              </Tooltip>
            )}
            {engine && (
              <Chip
                label={engine}
                size="small"
                variant="outlined"
                color={engineIsActive ? 'primary' : 'default'}
                clickable
                onClick={(event) => setEngineAnchor(event.currentTarget)}
                aria-label={`${engine} runtime backends`}
                sx={{ height: 20, fontSize: 11, flexShrink: 0 }}
              />
            )}
            <Tooltip title={pinned ? 'Unpin from navigation' : 'Pin to navigation'}>
              <IconButton
                size="small"
                aria-label={pinned ? 'Unpin service' : 'Pin service'}
                onClick={() => toggle(id)}
              >
                {pinned ? (
                  <StarIcon fontSize="small" sx={{ color: serviceAccent(id) }} />
                ) : (
                  <StarBorderIcon fontSize="small" />
                )}
              </IconButton>
            </Tooltip>
            <Tooltip title="Documentation">
              <IconButton
                size="small"
                component="a"
                href={docsHref ?? serviceDocsHref(id)}
                target="_blank"
                rel="noreferrer"
                aria-label="Service documentation"
              >
                <HelpOutlineIcon fontSize="small" />
              </IconButton>
            </Tooltip>
          </Stack>
          {subtitle && (
            <Typography variant="body2" color="text.secondary">
              {subtitle}
            </Typography>
          )}
        </Box>
        {actions && (
          <Stack direction="row" spacing={1} sx={{ alignItems: 'center' }}>
            {actions}
          </Stack>
        )}
      </Stack>
      <Popover
        open={Boolean(engineAnchor)}
        anchorEl={engineAnchor}
        onClose={() => setEngineAnchor(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
      >
        <Box sx={{ p: 1.5, minWidth: 240 }}>
          <Typography variant="subtitle2" sx={{ mb: 1 }}>
            {service?.label ?? 'Service'} runtime backends
          </Typography>
          <Stack component="ul" spacing={0.75} sx={{ m: 0, p: 0, listStyle: 'none' }}>
            {backends.map((backend) => (
              <Box component="li" key={backend.name} sx={{ display: 'flex', gap: 1 }}>
                <Typography
                  component="span"
                  sx={{ width: 14, color: backend.supported ? 'success.main' : 'text.disabled' }}
                >
                  {backend.supported ? '✓' : '✗'}
                </Typography>
                <Box sx={{ minWidth: 0 }}>
                  <Typography variant="body2" sx={{ fontWeight: 500 }}>
                    {backend.name}
                  </Typography>
                  {backend.note && (
                    <Typography variant="caption" color="text.secondary" sx={{ display: 'block' }}>
                      {backend.note}
                    </Typography>
                  )}
                </Box>
              </Box>
            ))}
          </Stack>
        </Box>
      </Popover>
      {children}
    </Box>
  )
}
