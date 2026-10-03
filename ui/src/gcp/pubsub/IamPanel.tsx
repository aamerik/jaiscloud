import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  IconButton,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import {
  getSubscriptionIam,
  getTopicIam,
  putSubscriptionIam,
  putTopicIam,
  type IamBinding,
  type IamPolicy,
} from '../../api/gcp/pubsub'

/** Editable IAM policy for a Pub/Sub topic or subscription. */
export function IamPanel({
  kind,
  name,
}: {
  kind: 'topic' | 'subscription'
  name: string
}) {
  const queryClient = useQueryClient()
  const fetch = kind === 'topic' ? getTopicIam : getSubscriptionIam
  const savePolicy = kind === 'topic' ? putTopicIam : putSubscriptionIam
  const queryKey = ['gcp', 'pubsub', kind, name, 'iam']

  const query = useQuery({ queryKey, queryFn: () => fetch(name) })
  const [bindings, setBindings] = useState<IamBinding[]>([])
  const [role, setRole] = useState(
    kind === 'topic' ? 'roles/pubsub.publisher' : 'roles/pubsub.subscriber',
  )
  const [members, setMembers] = useState('')

  useEffect(() => {
    setBindings(query.data?.bindings ?? [])
  }, [query.data])

  const save = useMutation({
    mutationFn: (policy: IamPolicy) => savePolicy(name, policy),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey }),
  })

  const add = () => {
    if (!role || !members.trim()) return
    setBindings((prev) => [
      ...prev.filter((b) => b.role !== role),
      { role, members: members.split(',').map((m) => m.trim()).filter(Boolean) },
    ])
    setMembers('')
  }

  return (
    <Box sx={{ maxWidth: 720 }}>
      <Typography variant="h6" sx={{ mb: 1 }}>
        {kind === 'topic' ? 'Topic' : 'Subscription'} IAM policy
      </Typography>
      {query.isError && <Alert severity="error">Failed to load IAM policy.</Alert>}
      {save.isError && <Alert severity="error">Failed to save (etag mismatch?).</Alert>}
      {save.isSuccess && <Alert severity="success">Policy saved.</Alert>}

      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell>Role</TableCell>
            <TableCell>Members</TableCell>
            <TableCell align="right">Actions</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {bindings.map((b) => (
            <TableRow key={b.role}>
              <TableCell>{b.role}</TableCell>
              <TableCell>{b.members.join(', ')}</TableCell>
              <TableCell align="right">
                <IconButton
                  size="small"
                  onClick={() => setBindings((prev) => prev.filter((x) => x.role !== b.role))}
                >
                  <DeleteOutlineIcon fontSize="small" />
                </IconButton>
              </TableCell>
            </TableRow>
          ))}
          {bindings.length === 0 && (
            <TableRow>
              <TableCell colSpan={3} sx={{ color: 'text.secondary' }}>
                No bindings.
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>

      <Stack direction="row" spacing={1} sx={{ mt: 2 }}>
        <TextField label="Role" value={role} onChange={(e) => setRole(e.target.value)} size="small" sx={{ minWidth: 260 }} />
        <TextField
          label="Members (comma separated)"
          value={members}
          onChange={(e) => setMembers(e.target.value)}
          size="small"
          fullWidth
        />
        <Button onClick={add} startIcon={<AddIcon />}>
          Add
        </Button>
      </Stack>
      <Button
        variant="contained"
        sx={{ mt: 2 }}
        disabled={save.isPending}
        onClick={() =>
          save.mutate({ bindings, etag: query.data?.etag, version: query.data?.version ?? 1 })
        }
      >
        Save policy
      </Button>
    </Box>
  )
}
