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
import { deleteExclusion, listExclusions, type LogExclusion } from '../../api/gcp/logging'
import { useAccount } from '../../context/AccountContext'
import { ExclusionDialog } from './ExclusionDialog'

/** Resource-level log exclusions: list, create, edit and delete. */
export function ExclusionsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editing, setEditing] = useState<LogExclusion | undefined>(undefined)

  const exclusions = useQuery({
    queryKey: ['gcp', 'logging', 'exclusions', accountId],
    queryFn: listExclusions,
  })

  const remove = useMutation({
    mutationFn: (name: string) => deleteExclusion(name),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'logging', 'exclusions'] }),
  })

  const rows = exclusions.data?.exclusions ?? []

  return (
    <Box>
      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <Box>
          <Typography variant="h5">Exclusions</Typography>
          <Typography variant="body2" color="text.secondary">
            Excluded entries are not ingested · project {accountId || '—'}
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
          Create exclusion
        </Button>
      </Stack>

      {exclusions.isError && <Alert severity="error">Failed to load exclusions.</Alert>}
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
              <TableCell>Description</TableCell>
              <TableCell>Status</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {exclusions.isLoading && (
              <TableRow>
                <TableCell colSpan={5} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!exclusions.isLoading && rows.length === 0 && (
              <TableRow>
                <TableCell colSpan={5} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No exclusions in this project.
                </TableCell>
              </TableRow>
            )}
            {rows.map((exclusion) => (
              <TableRow key={exclusion.name} hover>
                <TableCell>{exclusion.name}</TableCell>
                <TableCell
                  sx={{ maxWidth: 360, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                >
                  {exclusion.filter}
                </TableCell>
                <TableCell>{exclusion.description || '—'}</TableCell>
                <TableCell>
                  <Chip
                    size="small"
                    label={exclusion.disabled ? 'DISABLED' : 'ENABLED'}
                    color={exclusion.disabled ? 'default' : 'success'}
                  />
                </TableCell>
                <TableCell align="right">
                  <Tooltip title="Edit exclusion">
                    <IconButton
                      size="small"
                      onClick={() => {
                        setEditing(exclusion)
                        setDialogOpen(true)
                      }}
                    >
                      <EditOutlinedIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title="Delete exclusion">
                    <IconButton
                      size="small"
                      onClick={() => remove.mutate(exclusion.name)}
                      disabled={remove.isPending}
                    >
                      <DeleteOutlineIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      <ExclusionDialog open={dialogOpen} onClose={() => setDialogOpen(false)} initial={editing} />
    </Box>
  )
}
