import { Fragment, useEffect, useRef, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getQueue, purgeQueue, listDLQSources, getTags, tagQueue, untagQueue, peekMessages, type PeekedMessage } from '../../../api/sqs'
import {
  AttributeEditor,
  Box,
  Button,
  ContentLayout,
  Header,
  Input,
  KeyValuePairs,
  SpaceBetween,
  Tabs,
} from '@cloudscape-design/components'
import { formatDate } from '../../../lib/date'
import { useNotifications } from '../../../components/notifications'
import { SQSMessageSend } from './SQSMessageSend'

type Tab = 'overview' | 'messages' | 'dlq' | 'tags'

const TAB_LABELS: Record<Tab, string> = {
  overview: 'Overview',
  messages: 'Messages',
  dlq: 'Dead-letter queue',
  tags: 'Tags',
}

export function SQSDetail() {
  const { queueUrl: rawParam } = useParams<{ queueUrl: string }>()
  const queueUrl = rawParam ? decodeURIComponent(rawParam) : ''
  const [tab, setTab] = useState<Tab>('overview')
  const [msgPage, setMsgPage] = useState(0)
  const [expandedMsgId, setExpandedMsgId] = useState<string | null>(null)
  const [showSend, setShowSend] = useState(false)
  const [purgeConfirm, setPurgeConfirm] = useState(false)
  const PAGE_SIZE = 50
  const qc = useQueryClient()

  const { data: queue, isLoading, error } = useQuery({
    queryKey: ['sqs', 'queue', queueUrl],
    queryFn: () => getQueue(queueUrl),
    enabled: !!queueUrl,
  })

  const { data: dlqSources } = useQuery({
    queryKey: ['sqs', 'queue', queueUrl, 'dlq-sources'],
    queryFn: () => listDLQSources(queueUrl),
    enabled: tab === 'dlq' && !!queueUrl,
  })

  const { data: tags } = useQuery({
    queryKey: ['sqs', 'queue', queueUrl, 'tags'],
    queryFn: () => getTags(queueUrl),
    enabled: tab === 'tags' && !!queueUrl,
  })

  const [tagItems, setTagItems] = useState<{ key: string; value: string }[]>([])
  const tagBaseline = useRef<Record<string, string>>({})
  const { notify } = useNotifications()

  useEffect(() => {
    if (tags) {
      tagBaseline.current = tags
      setTagItems(Object.entries(tags).map(([key, value]) => ({ key, value })))
    }
  }, [tags])

  const saveTags = useMutation({
    mutationFn: async () => {
      const next: Record<string, string> = {}
      for (const { key, value } of tagItems) {
        if (key.trim()) next[key.trim()] = value
      }
      const added: Record<string, string> = {}
      for (const [k, v] of Object.entries(next)) {
        if (tagBaseline.current[k] !== v) added[k] = v
      }
      const removed = Object.keys(tagBaseline.current).filter((k) => !(k in next))
      if (Object.keys(added).length > 0) await tagQueue(queueUrl, added)
      if (removed.length > 0) await untagQueue(queueUrl, removed)
    },
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['sqs', 'queue', queueUrl, 'tags'] })
      notify({ type: 'success', header: 'Tags saved' })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Failed to save tags', content: (err as Error).message }),
  })

  const { data: peekData, isFetching: peekFetching, refetch: refetchPeek } = useQuery({
    queryKey: ['sqs', 'queue', queueUrl, 'peek', msgPage],
    queryFn: () => peekMessages(queueUrl, { offset: msgPage * PAGE_SIZE, limit: PAGE_SIZE }),
    enabled: tab === 'messages' && !!queueUrl,
  })

  const purgeMut = useMutation({
    mutationFn: () => purgeQueue(queueUrl),
    onSuccess: () => {
      setPurgeConfirm(false)
      void qc.invalidateQueries({ queryKey: ['sqs', 'queue', queueUrl] })
    },
  })

  if (isLoading) {
    return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading…</div>
  }

  if (error || !queue) {
    return (
      <div>
        <Link to="/aws/sqs" style={{ color: '#0972d3', fontSize: '0.9em', textDecoration: 'none' }}>← Queues</Link>
        <p style={{ color: '#d13212' }}>{error ? (error as Error).message : 'Queue not found.'}</p>
      </div>
    )
  }

  const overviewRows: [string, string][] = [
    ['URL', queue.url],
    ['ARN', queue.arn],
    ['Type', queue.type],
    ['Messages available', queue.messagesAvailable.toLocaleString()],
    ['Messages in flight', queue.messagesInFlight.toLocaleString()],
    ['Visibility timeout', `${queue.visibilityTimeout}s`],
    ['Message retention', `${queue.retentionPeriod}s`],
    ['Max message size', `${Math.round(queue.maxMessageSize / 1024)} KB`],
    ['Dead-letter queue', queue.dlqArn || '—'],
    ['Max receive count', queue.dlqMaxReceive ? String(queue.dlqMaxReceive) : '—'],
    ['Created', formatDate(queue.createdAt)],
  ]

  return (
    <ContentLayout
      header={
        <Header
          variant="h1"
          description={<Box variant="code">{queue.arn}</Box>}
          actions={<Button onClick={() => setPurgeConfirm(true)}>Purge queue</Button>}
        >
          {queue.name}
        </Header>
      }
    >
      <Tabs
        tabs={(Object.keys(TAB_LABELS) as Tab[]).map((t) => ({ id: t, label: TAB_LABELS[t] }))}
        activeTabId={tab}
        onChange={({ detail }) => setTab(detail.activeTabId as Tab)}
      />

      {/* Tab content */}
      {tab === 'overview' && (
        <KeyValuePairs
          columns={2}
          items={overviewRows.map(([label, value]) => ({ label, value }))}
        />
      )}

      {tab === 'messages' && (
        <div>
          {/* Toolbar */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem', flexWrap: 'wrap' }}>
            <span style={{ fontSize: '0.88em', color: '#5f6b7a' }}>
              {peekData ? `${peekData.total.toLocaleString()} message${peekData.total !== 1 ? 's' : ''}` : '—'}
            </span>
            <button
              onClick={() => { setMsgPage(0); void refetchPeek() }}
              disabled={peekFetching}
              style={{ ...btnSmall, marginLeft: 'auto', opacity: peekFetching ? 0.6 : 1 }}
            >
              {peekFetching ? 'Loading…' : '↻ Refresh'}
            </button>
            <button
              onClick={() => setShowSend((s) => !s)}
              style={{ ...btnSmall, borderColor: '#e77600', color: '#e77600' }}
            >
              {showSend ? 'Hide send form' : '+ Send message'}
            </button>
          </div>

          {/* Optional send form */}
          {showSend && (
            <div style={{ marginBottom: '1.25rem' }}>
              <SQSMessageSend
                queueUrl={queueUrl}
                isFifo={queue.type === 'FIFO'}
                onSent={() => { void refetchPeek() }}
              />
            </div>
          )}

          {/* Messages table */}
          {!peekData || peekData.messages.length === 0 ? (
            <p style={{ color: '#5f6b7a', fontSize: '0.9em', fontStyle: 'italic' }}>
              {peekFetching ? 'Loading messages…' : 'No messages in this queue.'}
            </p>
          ) : (
            <div style={{ border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.88em' }}>
                <thead>
                  <tr style={{ background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }}>
                    <th style={th}>Status</th>
                    <th style={th}>Message ID</th>
                    <th style={th}>Body</th>
                    <th style={{ ...th, textAlign: 'right' }}>Rcv</th>
                    <th style={th}>Sent At</th>
                    {queue.type === 'FIFO' && <th style={th}>Group</th>}
                  </tr>
                </thead>
                <tbody>
                  {peekData.messages.map((m: PeekedMessage) => (
                    <Fragment key={m.messageId}>
                      <tr
                        style={{ borderBottom: expandedMsgId === m.messageId ? 'none' : '1px solid #e7e9ec', cursor: 'pointer' }}
                        onClick={() => setExpandedMsgId(expandedMsgId === m.messageId ? null : m.messageId)}
                        onMouseEnter={(e) => (e.currentTarget.style.background = '#fafbfc')}
                        onMouseLeave={(e) => (e.currentTarget.style.background = '')}
                      >
                        <td style={td}>
                          <span style={{
                            display: 'inline-block', padding: '0.1em 0.5em', borderRadius: 3,
                            fontSize: '0.8em', fontWeight: 500,
                            background: m.status === 'visible' ? '#d1fae5' : m.status === 'in-flight' ? '#fef3c7' : '#e0f2fe',
                            color: m.status === 'visible' ? '#065f46' : m.status === 'in-flight' ? '#92400e' : '#0369a1',
                          }}>
                            {m.status}
                          </span>
                        </td>
                        <td style={{ ...td, fontFamily: 'monospace', fontSize: '0.8em', color: '#5f6b7a', maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                          {m.messageId}
                        </td>
                        <td style={{ ...td, maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontFamily: 'monospace', fontSize: '0.82em' }}>
                          {m.body}
                        </td>
                        <td style={{ ...td, textAlign: 'right', color: '#5f6b7a' }}>{m.receiveCount}</td>
                        <td style={{ ...td, color: '#5f6b7a', whiteSpace: 'nowrap' }}>
                          {formatDate(m.sentAt)}
                        </td>
                        {queue.type === 'FIFO' && <td style={{ ...td, fontFamily: 'monospace', fontSize: '0.82em' }}>{m.groupId ?? '—'}</td>}
                      </tr>
                      {expandedMsgId === m.messageId && (
                        <tr style={{ borderBottom: '1px solid #e7e9ec' }}>
                          <td colSpan={queue.type === 'FIFO' ? 6 : 5} style={{ padding: '0 1rem 0.75rem' }}>
                            <pre style={{
                              margin: 0, padding: '0.75rem', background: '#1e1e1e', color: '#d4d4d4',
                              borderRadius: 6, overflow: 'auto', fontSize: '0.82em',
                              whiteSpace: 'pre-wrap', wordBreak: 'break-all', maxHeight: 300,
                            }}>
                              {(() => { try { return JSON.stringify(JSON.parse(m.body), null, 2) } catch { return m.body } })()}
                            </pre>
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  ))}
                </tbody>
              </table>

              {/* Pagination */}
              {peekData.total > PAGE_SIZE && (
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '0.6rem 1rem', background: '#f4f5f7', borderTop: '1px solid #e7e9ec', fontSize: '0.85em' }}>
                  <span style={{ color: '#5f6b7a' }}>
                    {msgPage * PAGE_SIZE + 1}–{Math.min((msgPage + 1) * PAGE_SIZE, peekData.total)} of {peekData.total.toLocaleString()}
                  </span>
                  <div style={{ display: 'flex', gap: '0.5rem' }}>
                    <button onClick={() => { setMsgPage((p) => p - 1); setExpandedMsgId(null) }} disabled={msgPage === 0} style={btnSmall}>
                      ← Prev
                    </button>
                    <button onClick={() => { setMsgPage((p) => p + 1); setExpandedMsgId(null) }} disabled={(msgPage + 1) * PAGE_SIZE >= peekData.total} style={btnSmall}>
                      Next →
                    </button>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {tab === 'dlq' && (
        <div>
          {queue.dlqArn ? (
            <div style={{ marginBottom: '1.5rem', padding: '1rem', background: '#f4f5f7', borderRadius: 6, fontSize: '0.9em' }}>
              <div style={{ fontWeight: 500, marginBottom: '0.5rem', color: '#5f6b7a' }}>Dead-letter queue ARN</div>
              <code style={{ wordBreak: 'break-all' }}>{queue.dlqArn}</code>
              {queue.dlqMaxReceive && (
                <div style={{ marginTop: '0.5rem', color: '#5f6b7a' }}>
                  Max receive count: <strong>{queue.dlqMaxReceive}</strong>
                </div>
              )}
            </div>
          ) : (
            <p style={{ color: '#5f6b7a', fontSize: '0.9em', fontStyle: 'italic' }}>
              No dead-letter queue configured.
            </p>
          )}

          {dlqSources && dlqSources.items.length > 0 && (
            <div>
              <h4 style={{ margin: '0 0 0.75rem', fontSize: '0.95rem', fontWeight: 600, color: '#16191f' }}>
                Source queues using this queue as DLQ
              </h4>
              <div style={{ border: '1px solid #e7e9ec', borderRadius: 6, overflow: 'hidden' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }}>
                  <thead>
                    <tr style={{ background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }}>
                      <th style={th}>Name</th>
                      <th style={th}>Type</th>
                      <th style={{ ...th, textAlign: 'right' }}>Max Receive Count</th>
                    </tr>
                  </thead>
                  <tbody>
                    {dlqSources.items.map((q) => (
                      <tr key={q.url} style={{ borderBottom: '1px solid #e7e9ec' }}>
                        <td style={td}>
                          <Link to={`/aws/sqs/${encodeURIComponent(q.url)}`} style={{ color: '#0972d3', textDecoration: 'none' }}>
                            {q.name}
                          </Link>
                        </td>
                        <td style={td}>{q.type}</td>
                        <td style={{ ...td, textAlign: 'right' }}>{q.dlqMaxReceive ?? '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {dlqSources && dlqSources.items.length === 0 && (
            <p style={{ color: '#5f6b7a', fontSize: '0.9em', fontStyle: 'italic' }}>
              No queues are using this queue as their dead-letter queue.
            </p>
          )}
        </div>
      )}

      {tab === 'tags' && (
        <SpaceBetween size="m">
          <AttributeEditor
            items={tagItems}
            onAddButtonClick={() => setTagItems((prev) => [...prev, { key: '', value: '' }])}
            onRemoveButtonClick={({ detail: { itemIndex } }) =>
              setTagItems((prev) => prev.filter((_, i) => i !== itemIndex))
            }
            addButtonText="Add tag"
            removeButtonText="Remove"
            definition={[
              {
                label: 'Key',
                control: (item: { key: string; value: string }, i: number) => (
                  <Input
                    value={item.key}
                    placeholder="Key"
                    onChange={({ detail }) =>
                      setTagItems((prev) =>
                        prev.map((it, idx) => (idx === i ? { ...it, key: detail.value } : it)),
                      )
                    }
                  />
                ),
              },
              {
                label: 'Value',
                control: (item: { key: string; value: string }, i: number) => (
                  <Input
                    value={item.value}
                    placeholder="Value"
                    onChange={({ detail }) =>
                      setTagItems((prev) =>
                        prev.map((it, idx) => (idx === i ? { ...it, value: detail.value } : it)),
                      )
                    }
                  />
                ),
              },
            ]}
          />
          <Button variant="primary" loading={saveTags.isPending} onClick={() => saveTags.mutate()}>
            Save tags
          </Button>
        </SpaceBetween>
      )}

      {/* Purge confirm dialog */}
      {purgeConfirm && (
        <div style={overlayStyle}>
          <div style={dialogStyle} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 0.75rem', fontSize: '1.05rem' }}>Purge queue?</h3>
            <p style={{ margin: '0 0 1.25rem', color: '#5f6b7a', fontSize: '0.9em' }}>
              All messages in <strong>{queue.name}</strong> will be permanently deleted. This action cannot be undone.
            </p>
            {purgeMut.error && (
              <p style={{ color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }}>
                {(purgeMut.error as Error).message}
              </p>
            )}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button onClick={() => setPurgeConfirm(false)} style={btnSecondary}>Cancel</button>
              <button
                onClick={() => purgeMut.mutate()}
                disabled={purgeMut.isPending}
                style={{ background: '#e77600', color: '#fff', border: 'none', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em', opacity: purgeMut.isPending ? 0.6 : 1 }}
              >
                {purgeMut.isPending ? 'Purging…' : 'Purge'}
              </button>
            </div>
          </div>
        </div>
      )}
    </ContentLayout>
  )
}

const th: React.CSSProperties = { padding: '0.55rem 1rem', textAlign: 'left', fontWeight: 600, color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' }
const td: React.CSSProperties = { padding: '0.7rem 1rem' }
const btnSmall: React.CSSProperties = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.3rem 0.75rem', cursor: 'pointer', fontSize: '0.83em' }
const btnSecondary: React.CSSProperties = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const dialogStyle: React.CSSProperties = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 380, maxWidth: 460, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' }
