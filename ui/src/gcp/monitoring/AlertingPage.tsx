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
import {
  deleteAlertPolicy,
  listAlertPolicies,
  updateAlertPolicy,
  type AlertPolicy,
} from '../../api/gcp/monitoring'
import { useAccount } from '../../context/AccountContext'
import { AlertPolicyDialog } from './AlertPolicyDialog'
import { resourceID } from './util'

function toggleBody(policy: AlertPolicy) {
  return {
    displayName: policy.displayName ?? '',
    documentation: policy.documentation,
    conditions: policy.conditions,
    combiner: policy.combiner,
    enabled: !(policy.enabled ?? true),
    notificationChannels: policy.notificationChannels,
    userLabels: policy.userLabels,
  }
}

/** Cloud Monitoring alerting policies: list, create, edit, toggle and delete. */
export function AlertingPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editing, setEditing] = useState<AlertPolicy | undefined>(undefined)

  const policies = useQuery({
    queryKey: ['gcp', 'monitoring', 'policies', accountId],
    queryFn: listAlertPolicies,
  })

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'monitoring', 'policies'] })

  const remove = useMutation({
    mutationFn: (id: string) => deleteAlertPolicy(id),
    onSuccess: invalidate,
  })
  const toggle = useMutation({
    mutationFn: (policy: AlertPolicy) => updateAlertPolicy(resourceID(policy.name), toggleBody(policy)),
    onSuccess: invalidate,
  })

  const rows = policies.data?.alertPolicies ?? []

  return (
    <Box>
      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <Box>
          <Typography variant="h5">Alerting</Typography>
          <Typography variant="body2" color="text.secondary">
            Alerting policies · project {accountId || '—'}
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
          Create policy
        </Button>
      </Stack>

      {policies.isError && <Alert severity="error">Failed to load alert policies.</Alert>}
      {(remove.isError || toggle.isError) && (
        <Alert severity="error" sx={{ mb: 2 }}>
          The action failed.
        </Alert>
      )}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Display name</TableCell>
              <TableCell>Combiner</TableCell>
              <TableCell>Conditions</TableCell>
              <TableCell>Channels</TableCell>
              <TableCell>Status</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {policies.isLoading && (
              <TableRow>
                <TableCell colSpan={6} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!policies.isLoading && rows.length === 0 && (
              <TableRow>
                <TableCell colSpan={6} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No alerting policies in this project.
                </TableCell>
              </TableRow>
            )}
            {rows.map((policy) => {
              const on = policy.enabled ?? true
              return (
                <TableRow key={policy.name} hover>
                  <TableCell>{policy.displayName || resourceID(policy.name)}</TableCell>
                  <TableCell>{policy.combiner || '—'}</TableCell>
                  <TableCell>{policy.conditions?.length ?? 0}</TableCell>
                  <TableCell>{policy.notificationChannels?.length ?? 0}</TableCell>
                  <TableCell>
                    <Chip
                      size="small"
                      label={on ? 'ENABLED' : 'DISABLED'}
                      color={on ? 'success' : 'default'}
                      onClick={() => toggle.mutate(policy)}
                      clickable
                    />
                  </TableCell>
                  <TableCell align="right">
                    <Tooltip title="Edit policy">
                      <IconButton
                        size="small"
                        onClick={() => {
                          setEditing(policy)
                          setDialogOpen(true)
                        }}
                      >
                        <EditOutlinedIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                    <Tooltip title="Delete policy">
                      <IconButton
                        size="small"
                        onClick={() => remove.mutate(resourceID(policy.name))}
                        disabled={remove.isPending}
                      >
                        <DeleteOutlineIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </TableCell>
                </TableRow>
              )
            })}
          </TableBody>
        </Table>
      </TableContainer>

      <AlertPolicyDialog open={dialogOpen} onClose={() => setDialogOpen(false)} initial={editing} />
    </Box>
  )
}
