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
import { deleteWorkflow, listWorkflows, type Workflow } from '../../api/gcp/workflows'
import { useAccount } from '../../context/AccountContext'
import { WorkflowDialog } from './WorkflowDialog'
import { shortDate, workflowStateColor } from './util'

function target(workflow: Workflow) {
  return { location: workflow.location, workflow: workflow.id }
}

/** Workflows across every location, with create and delete. */
export function WorkflowsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'workflows'] })

  const workflows = useQuery({
    queryKey: ['gcp', 'workflows', 'list', accountId],
    queryFn: listWorkflows,
  })

  const remove = useMutation({
    mutationFn: ({ location, workflow }: { location: string; workflow: string }) =>
      deleteWorkflow(location, workflow),
    onSuccess: invalidate,
  })

  return (
    <Box>
      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <Box>
          <Typography variant="h5">Workflows</Typography>
          <Typography variant="body2" color="text.secondary">
            Workflows · project {accountId || '—'}
          </Typography>
        </Box>
        <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
          Create workflow
        </Button>
      </Stack>

      {workflows.isError && <Alert severity="error">Failed to load workflows.</Alert>}
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
              <TableCell>State</TableCell>
              <TableCell>Revision</TableCell>
              <TableCell>Last updated</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {workflows.isLoading && (
              <TableRow>
                <TableCell colSpan={6} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!workflows.isLoading && (workflows.data?.workflows.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={6} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No workflows in this project.
                </TableCell>
              </TableRow>
            )}
            {workflows.data?.workflows.map((workflow) => (
              <TableRow key={`${workflow.location}/${workflow.id}`} hover>
                <TableCell>
                  <Link
                    component={RouterLink}
                    to={`/gcp/workflows/${encodeURIComponent(workflow.location)}/${encodeURIComponent(workflow.id)}`}
                  >
                    {workflow.id}
                  </Link>
                </TableCell>
                <TableCell>{workflow.location || '—'}</TableCell>
                <TableCell>
                  <Chip size="small" label={workflow.state || '—'} color={workflowStateColor(workflow.state)} />
                </TableCell>
                <TableCell>{workflow.revisionId || '—'}</TableCell>
                <TableCell>{shortDate(workflow.updateTime)}</TableCell>
                <TableCell align="right">
                  <Tooltip title="Delete workflow">
                    <span>
                      <IconButton
                        size="small"
                        disabled={remove.isPending}
                        onClick={() => remove.mutate(target(workflow))}
                        aria-label={`Delete ${workflow.id}`}
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

      <WorkflowDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Box>
  )
}
