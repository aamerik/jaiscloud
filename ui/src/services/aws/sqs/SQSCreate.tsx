import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  ExpandableSection,
  Form,
  FormField,
  Input,
  Modal,
  Select,
  SpaceBetween,
} from '@cloudscape-design/components'
import { createQueue, type CreateQueueRequest } from '../../../api/sqs'
import { useNotifications } from '../../../components/notifications'

interface Props {
  onClose: () => void
  onCreated: () => void
}

const TYPE_OPTIONS = [
  { value: 'Standard', label: 'Standard' },
  { value: 'FIFO', label: 'FIFO' },
]

export function SQSCreate({ onClose, onCreated }: Props) {
  const [name, setName] = useState('')
  const [type, setType] = useState<'Standard' | 'FIFO'>('Standard')
  const [visibility, setVisibility] = useState(30)
  const [retention, setRetention] = useState(345600)
  const [dlqArn, setDlqArn] = useState('')
  const [dlqMaxReceive, setDlqMaxReceive] = useState(3)
  const [tagKey, setTagKey] = useState('')
  const [tagVal, setTagVal] = useState('')
  const [tags, setTags] = useState<Record<string, string>>({})
  const { notify } = useNotifications()

  const mut = useMutation({
    mutationFn: (req: CreateQueueRequest) => createQueue(req),
    onSuccess: () => onCreated(),
    onError: (err) =>
      notify({ type: 'error', header: 'Could not create queue', content: (err as Error).message }),
  })

  function addTag() {
    if (tagKey.trim()) {
      setTags((t) => ({ ...t, [tagKey.trim()]: tagVal }))
      setTagKey('')
      setTagVal('')
    }
  }

  function removeTag(k: string) {
    setTags((t) => {
      const n = { ...t }
      delete n[k]
      return n
    })
  }

  function submit() {
    const queueName = type === 'FIFO' && !name.endsWith('.fifo') ? `${name}.fifo` : name
    const req: CreateQueueRequest = {
      name: queueName,
      type,
      visibilityTimeout: visibility,
      retentionPeriod: retention,
      ...(dlqArn ? { dlqArn, dlqMaxReceive } : {}),
      ...(Object.keys(tags).length > 0 ? { tags } : {}),
    }
    mut.mutate(req)
  }

  return (
    <Modal
      visible
      onDismiss={onClose}
      header="Create queue"
      footer={
        <Box float="right">
          <SpaceBetween direction="horizontal" size="xs">
            <Button variant="link" onClick={onClose}>
              Cancel
            </Button>
            <Button
              variant="primary"
              loading={mut.isPending}
              disabled={!name.trim()}
              onClick={submit}
            >
              Create queue
            </Button>
          </SpaceBetween>
        </Box>
      }
    >
      <Form>
        <SpaceBetween size="m">
          <FormField
            label="Queue name"
            constraintText={
              type === 'FIFO' && name && !name.endsWith('.fifo')
                ? `Will be created as ${name}.fifo`
                : undefined
            }
          >
            <Input
              autoFocus
              value={name}
              onChange={({ detail }) => setName(detail.value)}
              placeholder="my-queue"
            />
          </FormField>

          <FormField label="Type">
            <Select
              selectedOption={TYPE_OPTIONS.find((option) => option.value === type) ?? TYPE_OPTIONS[0]!}
              onChange={({ detail }) => setType(detail.selectedOption.value as 'Standard' | 'FIFO')}
              options={TYPE_OPTIONS}
            />
          </FormField>

          <SpaceBetween direction="horizontal" size="xs">
            <FormField label="Visibility timeout (s)">
              <Input
                type="number"
                value={String(visibility)}
                onChange={({ detail }) => setVisibility(Number(detail.value))}
              />
            </FormField>
            <FormField label="Retention period (s)">
              <Input
                type="number"
                value={String(retention)}
                onChange={({ detail }) => setRetention(Number(detail.value))}
              />
            </FormField>
          </SpaceBetween>

          <ExpandableSection headerText="Dead-letter queue (optional)">
            <SpaceBetween size="s">
              <FormField label="DLQ ARN">
                <Input
                  value={dlqArn}
                  onChange={({ detail }) => setDlqArn(detail.value)}
                  placeholder="arn:aws:sqs:us-east-1:000000000000:my-dlq"
                />
              </FormField>
              {dlqArn && (
                <FormField label="Max receive count">
                  <Input
                    type="number"
                    value={String(dlqMaxReceive)}
                    onChange={({ detail }) => setDlqMaxReceive(Number(detail.value))}
                  />
                </FormField>
              )}
            </SpaceBetween>
          </ExpandableSection>

          <ExpandableSection headerText="Tags (optional)">
            <SpaceBetween size="s">
              <SpaceBetween direction="horizontal" size="xs">
                <Input
                  value={tagKey}
                  onChange={({ detail }) => setTagKey(detail.value)}
                  placeholder="Key"
                />
                <Input
                  value={tagVal}
                  onChange={({ detail }) => setTagVal(detail.value)}
                  placeholder="Value"
                />
                <Button onClick={addTag}>Add</Button>
              </SpaceBetween>
              {Object.entries(tags).map(([k, v]) => (
                <SpaceBetween key={k} direction="horizontal" size="xs">
                  <Box variant="code">{k}</Box>
                  <Box variant="span" color="text-body-secondary">
                    =
                  </Box>
                  <Box variant="code">{v}</Box>
                  <Button variant="inline-link" onClick={() => removeTag(k)}>
                    Remove
                  </Button>
                </SpaceBetween>
              ))}
            </SpaceBetween>
          </ExpandableSection>

          {mut.error && (
            <Alert type="error" header="Could not create queue">
              {(mut.error as Error).message}
            </Alert>
          )}
        </SpaceBetween>
      </Form>
    </Modal>
  )
}
