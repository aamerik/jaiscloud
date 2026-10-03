import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
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
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink } from 'react-router-dom'
import { deleteTrigger, listTriggers, type EventarcTrigger } from '../../api/gcp/eventarc'
import { useAccount } from '../../context/AccountContext'
import { TriggerDialog } from './TriggerDialog'
import { destinationLabel, filterSummary, shortDate } from './util'

/** Eventarc triggers across every location, with create/delete. */
export function TriggersPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'eventarc'] })

  const triggers = useQuery({
    queryKey: ['gcp', 'eventarc', 'triggers', accountId],
    queryFn: listTriggers,
  })

  const remove = useMutation({
    mutationFn: (trigger: EventarcTrigger) => deleteTrigger(trigger.location, trigger.name),
    onSuccess: invalidate,
  })

  return (
    <Box>
      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <Box>
          <Typography variant="h5">Eventarc</Typography>
          <Typography variant="body2" color="text.secondary">
            Triggers · project {accountId || '—'}
          </Typography>
        </Box>
        <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
          Create trigger
        </Button>
      </Stack>

      {triggers.isError && <Alert severity="error">Failed to load triggers.</Alert>}
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {(remove.error as Error).message}
        </Alert>
      )}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell>Location</TableCell>
              <TableCell>Destination type</TableCell>
              <TableCell>Destination</TableCell>
              <TableCell>Filters</TableCell>
              <TableCell>Created</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {triggers.isLoading && (
              <TableRow>
                <TableCell colSpan={7} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!triggers.isLoading && (triggers.data?.triggers.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={7} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No Eventarc triggers in this project.
                </TableCell>
              </TableRow>
            )}
            {triggers.data?.triggers.map((trigger) => (
              <TableRow key={`${trigger.location}/${trigger.name}`} hover>
                <TableCell>
                  <Link
                    component={RouterLink}
                    to={`/gcp/eventarc/triggers/${encodeURIComponent(trigger.location)}/${encodeURIComponent(trigger.name)}`}
                  >
                    {trigger.name}
                  </Link>
                </TableCell>
                <TableCell>{trigger.location || '—'}</TableCell>
                <TableCell>
                  <Chip size="small" label={destinationLabel(trigger.destinationType)} />
                </TableCell>
                <TableCell sx={{ maxWidth: 300, overflowWrap: 'anywhere' }}>
                  {trigger.destination || '—'}
                </TableCell>
                <TableCell sx={{ maxWidth: 320, overflowWrap: 'anywhere' }}>
                  {filterSummary(trigger)}
                </TableCell>
                <TableCell>{shortDate(trigger.createTime)}</TableCell>
                <TableCell align="right">
                  <Tooltip title="Delete trigger">
                    <span>
                      <IconButton
                        size="small"
                        disabled={remove.isPending}
                        onClick={() => remove.mutate(trigger)}
                        aria-label={`Delete ${trigger.name}`}
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

      <TriggerDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Box>
  )
}
