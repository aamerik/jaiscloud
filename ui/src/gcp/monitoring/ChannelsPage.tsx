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
  deleteNotificationChannel,
  listNotificationChannels,
  type NotificationChannel,
} from '../../api/gcp/monitoring'
import { useAccount } from '../../context/AccountContext'
import { ChannelDialog } from './ChannelDialog'
import { resourceID, verificationColor } from './util'
import { GcpPageTitle } from '../common/PageTitle'

/** Notification channels: list, create, edit and delete. */
export function ChannelsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editing, setEditing] = useState<NotificationChannel | undefined>(undefined)

  const channels = useQuery({
    queryKey: ['gcp', 'monitoring', 'channels', accountId],
    queryFn: listNotificationChannels,
  })

  const remove = useMutation({
    mutationFn: (id: string) => deleteNotificationChannel(id),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'monitoring', 'channels'] }),
  })

  const rows = channels.data?.notificationChannels ?? []

  return (
    <Box>
      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <Box>
          <GcpPageTitle id="monitoring">Notification channels</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Destinations for alert notifications · project {accountId || '—'}
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
          Create channel
        </Button>
      </Stack>

      {channels.isError && <Alert severity="error">Failed to load channels.</Alert>}
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Display name</TableCell>
              <TableCell>Type</TableCell>
              <TableCell>Verification</TableCell>
              <TableCell>Status</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {channels.isLoading && (
              <TableRow>
                <TableCell colSpan={5} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!channels.isLoading && rows.length === 0 && (
              <TableRow>
                <TableCell colSpan={5} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No notification channels in this project.
                </TableCell>
              </TableRow>
            )}
            {rows.map((channel) => (
              <TableRow key={channel.name} hover>
                <TableCell>{channel.displayName || resourceID(channel.name)}</TableCell>
                <TableCell>{channel.type || '—'}</TableCell>
                <TableCell>
                  <Chip
                    size="small"
                    label={channel.verificationStatus || 'UNVERIFIED'}
                    color={verificationColor(channel.verificationStatus)}
                  />
                </TableCell>
                <TableCell>
                  <Chip
                    size="small"
                    label={(channel.enabled ?? true) ? 'ENABLED' : 'DISABLED'}
                    color={(channel.enabled ?? true) ? 'success' : 'default'}
                  />
                </TableCell>
                <TableCell align="right">
                  <Tooltip title="Edit channel">
                    <IconButton
                      size="small"
                      onClick={() => {
                        setEditing(channel)
                        setDialogOpen(true)
                      }}
                    >
                      <EditOutlinedIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title="Delete channel">
                    <IconButton
                      size="small"
                      onClick={() => remove.mutate(resourceID(channel.name))}
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

      <ChannelDialog open={dialogOpen} onClose={() => setDialogOpen(false)} initial={editing} />
    </Box>
  )
}
