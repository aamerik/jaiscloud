import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Checkbox,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControlLabel,
  IconButton,
  Link,
  MenuItem,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink } from 'react-router-dom'
import {
  createSubscription,
  deleteSubscription,
  listSubscriptions,
  listTopics,
} from '../../api/gcp/pubsub'
import { useAccount } from '../../context/AccountContext'

/** Pub/Sub subscription list with create / delete. */
export function SubscriptionsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)

  const [name, setName] = useState('')
  const [topic, setTopic] = useState('')
  const [delivery, setDelivery] = useState<'pull' | 'push'>('pull')
  const [pushEndpoint, setPushEndpoint] = useState('')
  const [ackDeadline, setAckDeadline] = useState('')
  const [retention, setRetention] = useState('')
  const [filter, setFilter] = useState('')
  const [exactlyOnce, setExactlyOnce] = useState(false)
  const [ordering, setOrdering] = useState(false)

  const subscriptions = useQuery({
    queryKey: ['gcp', 'pubsub', 'subscriptions', accountId],
    queryFn: listSubscriptions,
  })
  const topics = useQuery({
    queryKey: ['gcp', 'pubsub', 'topics', accountId],
    queryFn: listTopics,
  })

  const create = useMutation({
    mutationFn: createSubscription,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'pubsub', 'subscriptions'] })
      setCreateOpen(false)
      setName('')
      setTopic('')
      setDelivery('pull')
      setPushEndpoint('')
      setAckDeadline('')
      setRetention('')
      setFilter('')
      setExactlyOnce(false)
      setOrdering(false)
    },
  })

  const remove = useMutation({
    mutationFn: deleteSubscription,
    onSuccess: () =>
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'pubsub', 'subscriptions'] }),
  })

  const submit = () =>
    create.mutate({
      name,
      topic,
      ...(delivery === 'push' ? { pushEndpoint } : {}),
      ...(ackDeadline ? { ackDeadlineSeconds: Number(ackDeadline) } : {}),
      ...(retention ? { messageRetentionDuration: retention } : {}),
      ...(filter ? { filter } : {}),
      ...(exactlyOnce ? { enableExactlyOnceDelivery: true } : {}),
      ...(ordering ? { enableMessageOrdering: true } : {}),
    })

  return (
    <Box>
      <Stack direction="row" sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
        <Box>
          <Typography variant="h5">Subscriptions</Typography>
          <Typography variant="body2" color="text.secondary">
            Pub/Sub · project {accountId || '—'}
          </Typography>
        </Box>
        <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
          Create subscription
        </Button>
      </Stack>

      {subscriptions.isError && <Alert severity="error">Failed to load subscriptions.</Alert>}
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
              <TableCell>Topic</TableCell>
              <TableCell>Delivery</TableCell>
              <TableCell>Ack deadline</TableCell>
              <TableCell>State</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {subscriptions.isLoading && (
              <TableRow>
                <TableCell colSpan={6} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!subscriptions.isLoading && (subscriptions.data?.subscriptions.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={6} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No subscriptions in this project.
                </TableCell>
              </TableRow>
            )}
            {subscriptions.data?.subscriptions.map((sub) => (
              <TableRow key={sub.name} hover>
                <TableCell>
                  <Link
                    component={RouterLink}
                    to={`/gcp/pubsub/subscriptions/${encodeURIComponent(sub.name)}`}
                  >
                    {sub.name}
                  </Link>
                </TableCell>
                <TableCell>{sub.topic || '—'}</TableCell>
                <TableCell>{sub.pushEndpoint ? 'Push' : 'Pull'}</TableCell>
                <TableCell>{sub.ackDeadlineSeconds ? `${sub.ackDeadlineSeconds}s` : '—'}</TableCell>
                <TableCell>{sub.detached ? 'Detached' : sub.state || 'ACTIVE'}</TableCell>
                <TableCell align="right">
                  <Tooltip title="Delete subscription">
                    <span>
                      <IconButton
                        size="small"
                        disabled={remove.isPending}
                        onClick={() => remove.mutate(sub.name)}
                        aria-label={`Delete ${sub.name}`}
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

      <Dialog open={createOpen} onClose={() => setCreateOpen(false)} fullWidth maxWidth="sm">
        <DialogTitle>Create subscription</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            {create.isError && <Alert severity="error">Could not create the subscription.</Alert>}
            <TextField
              autoFocus
              label="Subscription ID"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="my-subscription"
              fullWidth
            />
            <TextField
              select
              label="Topic"
              value={topic}
              onChange={(e) => setTopic(e.target.value)}
              fullWidth
            >
              {(topics.data?.topics ?? []).map((t) => (
                <MenuItem key={t.name} value={t.name}>
                  {t.name}
                </MenuItem>
              ))}
            </TextField>
            <TextField
              select
              label="Delivery type"
              value={delivery}
              onChange={(e) => setDelivery(e.target.value as 'pull' | 'push')}
              fullWidth
            >
              <MenuItem value="pull">Pull</MenuItem>
              <MenuItem value="push">Push</MenuItem>
            </TextField>
            {delivery === 'push' && (
              <TextField
                label="Push endpoint"
                value={pushEndpoint}
                onChange={(e) => setPushEndpoint(e.target.value)}
                placeholder="https://example.com/push"
                fullWidth
              />
            )}
            <TextField
              label="Ack deadline (10-600s)"
              value={ackDeadline}
              onChange={(e) => setAckDeadline(e.target.value)}
              fullWidth
            />
            <TextField
              label="Message retention (e.g. 604800s)"
              value={retention}
              onChange={(e) => setRetention(e.target.value)}
              fullWidth
            />
            <TextField
              label="Filter (immutable after create)"
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              fullWidth
            />
            <FormControlLabel
              control={
                <Checkbox
                  checked={exactlyOnce}
                  disabled={delivery === 'push'}
                  onChange={(e) => setExactlyOnce(e.target.checked)}
                />
              }
              label="Exactly-once delivery (pull only)"
            />
            <FormControlLabel
              control={
                <Checkbox checked={ordering} onChange={(e) => setOrdering(e.target.checked)} />
              }
              label="Message ordering"
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setCreateOpen(false)}>Cancel</Button>
          <Button
            variant="contained"
            disabled={!name || !topic || (delivery === 'push' && !pushEndpoint) || create.isPending}
            onClick={submit}
          >
            Create
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  )
}
