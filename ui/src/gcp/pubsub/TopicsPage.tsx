import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  IconButton,
  Link,
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
import SendOutlinedIcon from '@mui/icons-material/SendOutlined'
import { Link as RouterLink } from 'react-router-dom'
import { createTopic, deleteTopic, listTopics } from '../../api/gcp/pubsub'
import { useAccount } from '../../context/AccountContext'
import { PublishDialog } from './PublishDialog'
import { GcpPageTitle } from '../common/PageTitle'

/** Pub/Sub topic list with create / delete / publish. */
export function TopicsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [name, setName] = useState('')
  const [retention, setRetention] = useState('')
  const [publishTopic, setPublishTopic] = useState('')

  const topics = useQuery({
    queryKey: ['gcp', 'pubsub', 'topics', accountId],
    queryFn: listTopics,
  })

  const create = useMutation({
    mutationFn: createTopic,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'pubsub', 'topics'] })
      setCreateOpen(false)
      setName('')
      setRetention('')
    },
  })

  const remove = useMutation({
    mutationFn: deleteTopic,
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'pubsub', 'topics'] }),
  })

  return (
    <Box>
      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <Box>
          <GcpPageTitle id="pubsub">Topics</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Pub/Sub · project {accountId || '—'}
          </Typography>
        </Box>
        <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
          Create topic
        </Button>
      </Stack>

      {topics.isError && <Alert severity="error">Failed to load topics.</Alert>}
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
              <TableCell>Message retention</TableCell>
              <TableCell>Encryption</TableCell>
              <TableCell>Labels</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {topics.isLoading && (
              <TableRow>
                <TableCell colSpan={5} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!topics.isLoading && (topics.data?.topics.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={5} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No topics in this project.
                </TableCell>
              </TableRow>
            )}
            {topics.data?.topics.map((topic) => (
              <TableRow key={topic.name} hover>
                <TableCell>
                  <Link component={RouterLink} to={`/gcp/pubsub/topics/${encodeURIComponent(topic.name)}`}>
                    {topic.name}
                  </Link>
                </TableCell>
                <TableCell>{topic.messageRetentionDuration || '7 days'}</TableCell>
                <TableCell>{topic.kmsKeyName ? 'CMEK' : 'Google-managed'}</TableCell>
                <TableCell>
                  {topic.labels
                    ? Object.entries(topic.labels)
                        .map(([k, v]) => `${k}=${v}`)
                        .join(', ')
                    : '—'}
                </TableCell>
                <TableCell align="right">
                  <Tooltip title="Publish message">
                    <IconButton size="small" onClick={() => setPublishTopic(topic.name)}>
                      <SendOutlinedIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title="Delete topic">
                    <span>
                      <IconButton
                        size="small"
                        disabled={remove.isPending}
                        onClick={() => remove.mutate(topic.name)}
                        aria-label={`Delete ${topic.name}`}
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

      <Dialog open={createOpen} onClose={() => setCreateOpen(false)} fullWidth maxWidth="xs">
        <DialogTitle>Create topic</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            {create.isError && <Alert severity="error">Could not create the topic.</Alert>}
            <TextField
              autoFocus
              label="Topic ID"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="my-topic"
              fullWidth
            />
            <TextField
              label="Message retention (e.g. 604800s)"
              value={retention}
              onChange={(e) => setRetention(e.target.value)}
              fullWidth
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setCreateOpen(false)}>Cancel</Button>
          <Button
            variant="contained"
            disabled={!name || create.isPending}
            onClick={() =>
              create.mutate({
                name,
                ...(retention ? { messageRetentionDuration: retention } : {}),
              })
            }
          >
            Create
          </Button>
        </DialogActions>
      </Dialog>

      {publishTopic && (
        <PublishDialog
          topic={publishTopic}
          open={Boolean(publishTopic)}
          onClose={() => setPublishTopic('')}
        />
      )}
    </Box>
  )
}
