import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Badge,
  Box,
  Button,
  Container,
  ContentLayout,
  Form,
  FormField,
  Header,
  Input,
  KeyValuePairs,
  Link,
  Modal,
  Select,
  SpaceBetween,
  Table,
  Tabs,
  Textarea,
} from '@cloudscape-design/components'
import {
  getTopic,
  listSubscriptionsByTopic,
  subscribe,
  unsubscribe,
  publish,
  type Subscription,
} from '../../../api/sns'
import { useNotifications } from '../../../components/notifications'

type Tab = 'overview' | 'subscriptions'

const PROTOCOLS = [
  { value: 'sqs', label: 'sqs' },
  { value: 'lambda', label: 'lambda' },
  { value: 'http', label: 'http' },
  { value: 'https', label: 'https' },
  { value: 'email', label: 'email' },
  { value: 'sms', label: 'sms' },
]

export function SNSDetail() {
  const { topicArn: encodedArn } = useParams<{ topicArn: string }>()
  const topicArn = decodeURIComponent(encodedArn ?? '')
  const navigate = useNavigate()
  const qc = useQueryClient()

  const [tab, setTab] = useState<Tab>('overview')
  const [subOpen, setSubOpen] = useState(false)
  const [subProtocol, setSubProtocol] = useState('sqs')
  const [subEndpoint, setSubEndpoint] = useState('')
  const [publishOpen, setPublishOpen] = useState(false)
  const [publishMsg, setPublishMsg] = useState('')
  const [publishSubject, setPublishSubject] = useState('')
  const [publishResult, setPublishResult] = useState('')
  const { notify } = useNotifications()

  const { data: topic } = useQuery({
    queryKey: ['sns', 'topic', topicArn],
    queryFn: () => getTopic(topicArn),
  })

  const { data: subsData, isLoading: subsLoading } = useQuery({
    queryKey: ['sns', 'subscriptions', topicArn],
    queryFn: () => listSubscriptionsByTopic(topicArn),
  })

  const subscribeMut = useMutation({
    mutationFn: () => subscribe(topicArn, { protocol: subProtocol, endpoint: subEndpoint }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['sns', 'subscriptions', topicArn] })
      void qc.invalidateQueries({ queryKey: ['sns', 'topics'] })
      notify({ type: 'success', header: 'Subscription created' })
      setSubOpen(false)
      setSubEndpoint('')
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Subscribe failed', content: (err as Error).message }),
  })

  const unsubscribeMut = useMutation({
    mutationFn: (subArn: string) => unsubscribe(subArn),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['sns', 'subscriptions', topicArn] })
      notify({ type: 'success', header: 'Subscription removed' })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Remove failed', content: (err as Error).message }),
  })

  const publishMut = useMutation({
    mutationFn: () =>
      publish(topicArn, { message: publishMsg, subject: publishSubject || undefined }),
    onSuccess: (res) => {
      setPublishResult(res.MessageId ?? 'sent')
      setPublishMsg('')
      setPublishSubject('')
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Publish failed', content: (err as Error).message }),
  })

  const subs = subsData?.items ?? []
  const topicName = topic?.name ?? topicArn.split(':').pop()

  return (
    <ContentLayout
      breadcrumbs={
        <Link
          href="/ui/aws/sns"
          onFollow={(event) => {
            event.preventDefault()
            navigate('/aws/sns')
          }}
        >
          Topics
        </Link>
      }
      header={
        <Header
          variant="h1"
          description={<Box variant="code">{topicArn}</Box>}
          actions={
            <SpaceBetween direction="horizontal" size="xs">
              <Button onClick={() => setSubOpen(true)}>Subscribe</Button>
              <Button
                variant="primary"
                onClick={() => {
                  setPublishOpen(true)
                  setPublishMsg('')
                  setPublishSubject('')
                  setPublishResult('')
                }}
              >
                Publish
              </Button>
            </SpaceBetween>
          }
        >
          {topicName}
        </Header>
      }
    >
      <Tabs
        tabs={[
          { id: 'overview', label: 'Overview' },
          { id: 'subscriptions', label: `Subscriptions (${subs.length})` },
        ]}
        activeTabId={tab}
        onChange={({ detail }) => setTab(detail.activeTabId as Tab)}
      />

      {tab === 'overview' && (
        <KeyValuePairs
          columns={2}
          items={[
            { label: 'Topic ARN', value: topicArn },
            { label: 'Type', value: topic?.type ?? '—' },
            { label: 'Display name', value: topic?.displayName || '—' },
            { label: 'Subscriptions', value: String(topic?.subscriptionCount ?? subs.length) },
          ]}
        />
      )}

      {tab === 'subscriptions' && (
        <Container header={<Header variant="h2">Subscriptions</Header>}>
          <Table
            items={subs}
            columnDefinitions={[
              {
                id: 'protocol',
                header: 'Protocol',
                cell: (s: Subscription) => <Badge color="grey">{s.protocol}</Badge>,
              },
              {
                id: 'endpoint',
                header: 'Endpoint',
                cell: (s: Subscription) => <Box variant="code">{s.endpoint}</Box>,
              },
              {
                id: 'actions',
                header: '',
                cell: (s: Subscription) => (
                  <Button
                    variant="link"
                    loading={unsubscribeMut.isPending}
                    onClick={() => unsubscribeMut.mutate(s.subscriptionArn)}
                  >
                    Remove
                  </Button>
                ),
              },
            ]}
            trackBy={(s) => s.subscriptionArn}
            loading={subsLoading}
            loadingText="Loading subscriptions"
            empty={
              <Box textAlign="center" color="inherit">
                <b>No subscriptions</b>
                <Box variant="p" color="inherit">
                  Subscribe this topic to an SQS queue, Lambda function, or HTTP endpoint.
                </Box>
              </Box>
            }
          />
        </Container>
      )}

      <Modal
        visible={subOpen}
        onDismiss={() => setSubOpen(false)}
        header="Subscribe"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setSubOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={subscribeMut.isPending}
                disabled={!subEndpoint}
                onClick={() => subscribeMut.mutate()}
              >
                Subscribe
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="l">
            <FormField label="Protocol">
              <Select
                selectedOption={PROTOCOLS.find((p) => p.value === subProtocol) ?? PROTOCOLS[0]!}
                onChange={({ detail }) => setSubProtocol(detail.selectedOption.value ?? 'sqs')}
                options={PROTOCOLS}
                ariaLabel="Protocol"
              />
            </FormField>
            <FormField label="Endpoint">
              <Input
                autoFocus
                value={subEndpoint}
                onChange={({ detail }) => setSubEndpoint(detail.value)}
                placeholder={
                  subProtocol === 'sqs'
                    ? 'arn:aws:sqs:us-east-1:000000000000:my-queue'
                    : 'https://…'
                }
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={publishOpen}
        onDismiss={() => {
          setPublishOpen(false)
          setPublishResult('')
        }}
        header="Publish message"
        footer={
          publishResult ? (
            <Box float="right">
              <Button
                variant="primary"
                onClick={() => {
                  setPublishOpen(false)
                  setPublishResult('')
                }}
              >
                Done
              </Button>
            </Box>
          ) : (
            <Box float="right">
              <SpaceBetween direction="horizontal" size="xs">
                <Button variant="link" onClick={() => setPublishOpen(false)}>
                  Cancel
                </Button>
                <Button
                  variant="primary"
                  loading={publishMut.isPending}
                  disabled={!publishMsg}
                  onClick={() => publishMut.mutate()}
                >
                  Publish
                </Button>
              </SpaceBetween>
            </Box>
          )
        }
      >
        {publishResult ? (
          <Alert type="success" header="Published">
            MessageId: {publishResult}
          </Alert>
        ) : (
          <Form>
            <SpaceBetween size="l">
              <FormField label="Topic">
                <Box color="text-body-secondary">{topic?.name}</Box>
              </FormField>
              <FormField label="Subject" description="Optional">
                <Input
                  value={publishSubject}
                  onChange={({ detail }) => setPublishSubject(detail.value)}
                  placeholder="Subject…"
                />
              </FormField>
              <FormField label="Message">
                <Textarea
                  autoFocus
                  value={publishMsg}
                  onChange={({ detail }) => setPublishMsg(detail.value)}
                  placeholder="Message body…"
                />
              </FormField>
            </SpaceBetween>
          </Form>
        )}
      </Modal>
    </ContentLayout>
  )
}
