import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import '@cloudscape-design/global-styles/index.css'
import { applyMode, Mode, setThemeClass, Theme } from '@cloudscape-design/global-styles'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { BrowserRouter } from 'react-router-dom'
import App from './App'
import { applyAwsConsoleTheme } from './theme/aws/theme'
import './theme/aws/console.css'

// Current AWS Console visual language + the AWS orange primary palette.
setThemeClass(Theme.OneTheme)
applyMode(localStorage.getItem('jaiscloud-mode') === 'dark' ? Mode.Dark : Mode.Light)
applyAwsConsoleTheme()

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

createRoot(root).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <BrowserRouter basename="/ui">
        <App />
      </BrowserRouter>
    </QueryClientProvider>
  </StrictMode>,
)
