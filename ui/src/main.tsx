import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import '@cloudscape-design/global-styles/index.css'
import { applyMode, Mode, setThemeClass, Theme } from '@cloudscape-design/global-styles'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { BrowserRouter } from 'react-router-dom'
import App from './App'
import { api } from './api/client'
import type { Meta } from './hooks/useMeta'
import { setActiveCloud } from './lib/cloud'
import { applyCloudTheme } from './theme/selectTheme'
import './theme/aws/console.css'

// Cloudscape OneTheme base; the cloud-specific palette is applied in
// bootstrap() once /api/ui/v1/meta identifies the cloud.
setThemeClass(Theme.OneTheme)
applyMode(localStorage.getItem('jaiscloud-mode') === 'dark' ? Mode.Dark : Mode.Light)

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 10_000,
      retry: 1,
    },
  },
})

const root = document.getElementById('root')
if (!root) throw new Error('Root element not found')
const container = createRoot(root)

/**
 * Resolve the cloud identity before the first paint so the console theme and
 * cloud-dependent defaults (density, terminology) are correct on the initial
 * render, then seed the query cache so App does not refetch meta. A failed
 * fetch falls through to the connection error App already renders.
 */
async function bootstrap() {
  let cloud = 'aws'
  try {
    const meta = await api.get<Meta>('/api/ui/v1/meta')
    cloud = meta.cloud || cloud
    queryClient.setQueryData(['meta'], meta)
  } catch {
    /* App renders the connection error */
  }

  setActiveCloud(cloud)
  applyCloudTheme(cloud)

  container.render(
    <StrictMode>
      <QueryClientProvider client={queryClient}>
        <BrowserRouter basename="/ui">
          <App />
        </BrowserRouter>
      </QueryClientProvider>
    </StrictMode>,
  )
}

void bootstrap()
