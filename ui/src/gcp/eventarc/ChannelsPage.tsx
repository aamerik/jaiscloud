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
import { deleteChannel, listChannels, type EventarcChannel } from '../../api/gcp/eventarc'
import { useAccount } from '../../context/AccountContext'
import { ChannelDialog } from './ChannelDialog'
import { channelProviderLabel, channelStateColor, shortDate } from './util'

/** Eventarc channels across every location, with create/delete. */
export function ChannelsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'eventarc'] })

  const channels = useQuery({
    queryKey: ['gcp', 'eventarc', 'channels', accountId],
    queryFn: listChannels,
  })

  const remove = useMutation({
    mutationFn: (channel: EventarcChannel) => deleteChannel(channel.location, channel.name),
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
            Channels · project {accountId || '—'}
          </Typography>
        </Box>
        <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
          Create channel
        </Button>
      </Stack>

      {channels.isError && <Alert severity="error">Failed to load channels.</Alert>}
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
              <TableCell>Provider</TableCell>
              <TableCell>Pub/Sub topic</TableCell>
              <TableCell>Created</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {channels.isLoading && (
              <TableRow>
                <TableCell colSpan={7} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!channels.isLoading && (channels.data?.channels.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={7} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No Eventarc channels in this project.
                </TableCell>
              </TableRow>
            )}
            {channels.data?.channels.map((channel) => (
              <TableRow key={`${channel.location}/${channel.name}`} hover>
                <TableCell>
                  <Link
                    component={RouterLink}
                    to={`/gcp/eventarc/channels/${encodeURIComponent(channel.location)}/${encodeURIComponent(channel.name)}`}
                  >
                    {channel.name}
                  </Link>
                </TableCell>
                <TableCell>{channel.location || '—'}</TableCell>
                <TableCell>
                  <Chip size="small" label={channel.state || '—'} color={channelStateColor(channel.state)} />
                </TableCell>
                <TableCell>{channelProviderLabel(channel)}</TableCell>
                <TableCell sx={{ maxWidth: 340, overflowWrap: 'anywhere' }}>
                  {channel.pubsubTopic || '—'}
                </TableCell>
                <TableCell>{shortDate(channel.createTime)}</TableCell>
                <TableCell align="right">
                  <Tooltip title="Delete channel">
                    <span>
                      <IconButton
                        size="small"
                        disabled={remove.isPending}
                        onClick={() => remove.mutate(channel)}
                        aria-label={`Delete ${channel.name}`}
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

      <ChannelDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Box>
  )
}
