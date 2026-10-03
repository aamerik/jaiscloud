import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  IconButton,
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
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import EditOutlinedIcon from '@mui/icons-material/EditOutlined'
import { deleteMetric, listMetrics, type LogMetric } from '../../api/gcp/logging'
import { useAccount } from '../../context/AccountContext'
import { MetricDialog } from './MetricDialog'
import { GcpPageTitle } from '../common/PageTitle'

/** Logs-based metrics: list, create, edit and delete. */
export function MetricsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editing, setEditing] = useState<LogMetric | undefined>(undefined)

  const metrics = useQuery({
    queryKey: ['gcp', 'logging', 'metrics', accountId],
    queryFn: listMetrics,
  })

  const remove = useMutation({
    mutationFn: (name: string) => deleteMetric(name),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'logging', 'metrics'] }),
  })

  const rows = metrics.data?.metrics ?? []

  return (
    <Box>
      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <Box>
          <GcpPageTitle id="logging">Logs-based metrics</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Count log entries matching a filter · project {accountId || '—'}
          </Typography>
        </Box>
        <Button
          variant="contained"
          startIcon={<AddIcon />}
          onClick={() => {
            setEditing(undefined)
            setDialogOpen(true)
          }}
        >
          Create metric
        </Button>
      </Stack>

      {metrics.isError && <Alert severity="error">Failed to load metrics.</Alert>}
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
              <TableCell>Filter</TableCell>
              <TableCell>Kind</TableCell>
              <TableCell>Value type</TableCell>
              <TableCell>Status</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {metrics.isLoading && (
              <TableRow>
                <TableCell colSpan={6} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!metrics.isLoading && rows.length === 0 && (
              <TableRow>
                <TableCell colSpan={6} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No logs-based metrics in this project.
                </TableCell>
              </TableRow>
            )}
            {rows.map((metric) => (
              <TableRow key={metric.name} hover>
                <TableCell>{metric.name}</TableCell>
                <TableCell sx={{ maxWidth: 360, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {metric.filter}
                </TableCell>
                <TableCell>{metric.metricDescriptor?.metricKind || '—'}</TableCell>
                <TableCell>{metric.metricDescriptor?.valueType || '—'}</TableCell>
                <TableCell>
                  <Chip
                    size="small"
                    label={metric.disabled ? 'DISABLED' : 'ENABLED'}
                    color={metric.disabled ? 'default' : 'success'}
                  />
                </TableCell>
                <TableCell align="right">
                  <Tooltip title="Edit metric">
                    <IconButton
                      size="small"
                      onClick={() => {
                        setEditing(metric)
                        setDialogOpen(true)
                      }}
                    >
                      <EditOutlinedIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title="Delete metric">
                    <IconButton size="small" onClick={() => remove.mutate(metric.name)} disabled={remove.isPending}>
                      <DeleteOutlineIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      <MetricDialog open={dialogOpen} onClose={() => setDialogOpen(false)} initial={editing} />
    </Box>
  )
}
