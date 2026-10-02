import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Header,
  Input,
  Modal,
  SpaceBetween,
  Table,
} from '@cloudscape-design/components'
import type { TableProps } from '@cloudscape-design/components'
import { receiveMessages, deleteMessage, type Message } from '../../../api/sqs'
import { formatDate } from '../../../lib/date'
import { useNotifications } from '../../../components/notifications'

interface Props {
  queueUrl: string
}

function tryPrettyJson(s: string): string {
  try {
    return JSON.stringify(JSON.parse(s), null, 2)
  } catch {
    return s
  }
}

export function SQSMessageReceive({ queueUrl }: Props) {
  const [messages, setMessages] = useState<Message[]>([])
  const [maxMessages, setMaxMessages] = useState(10)
  const [viewMessage, setViewMessage] = useState<Message | null>(null)
  const qc = useQueryClient()
  const { notify } = useNotifications()

  const receiveMut = useMutation({
    mutationFn: () => receiveMessages(queueUrl, { maxMessages }),
    onSuccess: (data) => {
      const received = data.messages ?? []
      setMessages(received)
      void qc.invalidateQueries({ queryKey: ['sqs', 'queue', queueUrl] })
      notify({
        type: 'success',
        header: `Received ${received.length} message${received.length !== 1 ? 's' : ''}`,
      })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Receive failed', content: (err as Error).message }),
  })

  const deleteMut = useMutation({
    mutationFn: (receipt: string) => deleteMessage(queueUrl, receipt),
    onSuccess: (_result, receipt) => {
      setMessages((prev) => prev.filter((m) => m.receiptHandle !== receipt))
      void qc.invalidateQueries({ queryKey: ['sqs', 'queue', queueUrl] })
      notify({ type: 'success', header: 'Message deleted' })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const columns: TableProps.ColumnDefinition<Message>[] = [
    {
      id: 'messageId',
      header: 'Message ID',
      cell: (message) => <Box variant="code">{message.messageId}</Box>,
    },
    { id: 'sentAt', header: 'Sent', cell: (message) => formatDate(message.sentAt) },
    { id: 'body', header: 'Body', cell: (message) => <Box variant="code">{message.body}</Box> },
    {
      id: 'actions',
      header: '',
      cell: (message) => (
        <SpaceBetween direction="horizontal" size="xs">
          <Button variant="inline-link" onClick={() => setViewMessage(message)}>
            View body
          </Button>
          <Button
            variant="inline-link"
            loading={deleteMut.isPending}
            onClick={() => deleteMut.mutate(message.receiptHandle)}
          >
            Delete
          </Button>
        </SpaceBetween>
      ),
    },
  ]

  return (
    <SpaceBetween size="m">
      {receiveMut.error && (
        <Alert type="error" header="Could not receive messages">
          {(receiveMut.error as Error).message}
        </Alert>
      )}

      <Table
        items={messages}
        columnDefinitions={columns}
        trackBy={(message) => message.messageId}
        header={
          <Header
            variant="h2"
            counter={`(${messages.length})`}
            actions={
              <SpaceBetween direction="horizontal" size="xs">
                <Input
                  type="number"
                  value={String(maxMessages)}
                  onChange={({ detail }) => setMaxMessages(Number(detail.value))}
                  ariaLabel="Maximum messages"
                />
                <Button
                  variant="primary"
                  loading={receiveMut.isPending}
                  onClick={() => receiveMut.mutate()}
                >
                  Poll for messages
                </Button>
              </SpaceBetween>
            }
          >
            Receive messages
          </Header>
        }
        empty={
          <Box textAlign="center" color="inherit">
            <b>No messages</b>
            <Box variant="p" color="inherit">
              No messages available in the queue.
            </Box>
          </Box>
        }
      />

      <Modal
        visible={viewMessage !== null}
        onDismiss={() => setViewMessage(null)}
        header="Message body"
        size="large"
        footer={
          <Box float="right">
            <Button variant="link" onClick={() => setViewMessage(null)}>
              Close
            </Button>
          </Box>
        }
      >
        {viewMessage && <Box variant="pre">{tryPrettyJson(viewMessage.body)}</Box>}
      </Modal>
    </SpaceBetween>
  )
}
