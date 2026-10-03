import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Container,
  Form,
  FormField,
  Header,
  Input,
  SpaceBetween,
  Textarea,
} from '@cloudscape-design/components'
import { sendMessage, type SendMessageRequest } from '../../../api/sqs'
import { useNotifications } from '../../../components/notifications'

interface Props {
  queueUrl: string
  isFifo: boolean
  onSent?: () => void
}

export function SQSMessageSend({ queueUrl, isFifo, onSent }: Props) {
  const [body, setBody] = useState('')
  const [delay, setDelay] = useState(0)
  const [groupId, setGroupId] = useState('')
  const [dedupId, setDedupId] = useState('')
  const [lastMsgId, setLastMsgId] = useState<string | null>(null)
  const qc = useQueryClient()
  const { notify } = useNotifications()

  const mut = useMutation({
    mutationFn: (req: SendMessageRequest) => sendMessage(queueUrl, req),
    onSuccess: (data) => {
      setLastMsgId(data.messageId)
      setBody('')
      void qc.invalidateQueries({ queryKey: ['sqs', 'queue', queueUrl] })
      notify({ type: 'success', header: 'Message sent', content: data.messageId })
      onSent?.()
    },
    onError: (err) => {
      setLastMsgId(null)
      notify({ type: 'error', header: 'Could not send message', content: (err as Error).message })
    },
  })

  function submit() {
    const req: SendMessageRequest = {
      body,
      ...(delay > 0 ? { delaySeconds: delay } : {}),
      ...(isFifo && groupId ? { messageGroupId: groupId } : {}),
      ...(isFifo && dedupId ? { messageDeduplicationId: dedupId } : {}),
    }
    mut.mutate(req)
  }

  return (
    <Container header={<Header variant="h2">Send message</Header>}>
      <Form>
        <SpaceBetween size="m">
          <FormField label="Message body">
            <Textarea
              value={body}
              onChange={({ detail }) => setBody(detail.value)}
              rows={5}
              placeholder='{"key": "value"}'
            />
          </FormField>

          <SpaceBetween direction="horizontal" size="xs">
            <FormField label="Delay (s)">
              <Input
                type="number"
                value={String(delay)}
                onChange={({ detail }) => setDelay(Number(detail.value))}
              />
            </FormField>
            {isFifo && (
              <FormField label="Message group ID">
                <Input
                  value={groupId}
                  onChange={({ detail }) => setGroupId(detail.value)}
                  placeholder="required for FIFO"
                />
              </FormField>
            )}
            {isFifo && (
              <FormField
                label="Deduplication ID"
                constraintText="Leave blank for content-based deduplication."
              >
                <Input value={dedupId} onChange={({ detail }) => setDedupId(detail.value)} />
              </FormField>
            )}
          </SpaceBetween>

          {mut.error && (
            <Alert type="error" header="Could not send message">
              {(mut.error as Error).message}
            </Alert>
          )}

          {lastMsgId && !mut.isPending && (
            <Alert type="success" header="Message sent">
              MessageId: <Box variant="code">{lastMsgId}</Box>
            </Alert>
          )}

          <Button
            variant="primary"
            loading={mut.isPending}
            disabled={!body}
            onClick={submit}
          >
            Send message
          </Button>
        </SpaceBetween>
      </Form>
    </Container>
  )
}
