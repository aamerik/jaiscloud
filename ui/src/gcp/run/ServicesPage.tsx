import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
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
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink } from 'react-router-dom'
import { deleteService, listServices, type RunService } from '../../api/gcp/run'
import { useAccount } from '../../context/AccountContext'
import { lastSegment, shortDate } from './util'
import { GcpPageTitle } from '../common/PageTitle'

function target(service: RunService) {
  return { region: service.region, service: service.id }
}

/** Cloud Run services across every region, with delete. */
export function ServicesPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'run'] })

  const services = useQuery({
    queryKey: ['gcp', 'run', 'services', accountId],
    queryFn: listServices,
  })

  const remove = useMutation({
    mutationFn: ({ region, service }: { region: string; service: string }) =>
      deleteService(region, service),
    onSuccess: invalidate,
  })

  return (
    <Box>
      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <Box>
          <GcpPageTitle id="run">Cloud Run</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Services · project {accountId || '—'}
          </Typography>
        </Box>
      </Stack>

      {services.isError && <Alert severity="error">Failed to load services.</Alert>}
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell>Region</TableCell>
              <TableCell>Latest revision</TableCell>
              <TableCell>Updated</TableCell>
              <TableCell>Ready</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {services.isLoading && (
              <TableRow>
                <TableCell colSpan={6} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!services.isLoading && (services.data?.services.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={6} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No Cloud Run services in this project.
                </TableCell>
              </TableRow>
            )}
            {services.data?.services.map((service) => (
              <TableRow key={`${service.region}/${service.id}`} hover>
                <TableCell>
                  <Link
                    component={RouterLink}
                    to={`/gcp/run/services/${encodeURIComponent(service.region)}/${encodeURIComponent(service.id)}`}
                  >
                    {service.id}
                  </Link>
                </TableCell>
                <TableCell>{service.region || '—'}</TableCell>
                <TableCell>
                  {service.latestReadyRevision ? (
                    <Link
                      component={RouterLink}
                      to={`/gcp/run/services/${encodeURIComponent(service.region)}/${encodeURIComponent(service.id)}/revisions/${encodeURIComponent(lastSegment(service.latestReadyRevision))}`}
                    >
                      {lastSegment(service.latestReadyRevision)}
                    </Link>
                  ) : (
                    '—'
                  )}
                </TableCell>
                <TableCell>{shortDate(service.updateTime)}</TableCell>
                <TableCell>
                  <Chip
                    size="small"
                    label={service.ready ? 'Ready' : 'Not ready'}
                    color={service.ready ? 'success' : 'warning'}
                  />
                </TableCell>
                <TableCell align="right">
                  <Tooltip title="Delete service">
                    <span>
                      <IconButton
                        size="small"
                        disabled={remove.isPending}
                        onClick={() => remove.mutate(target(service))}
                        aria-label={`Delete ${service.id}`}
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
    </Box>
  )
}
