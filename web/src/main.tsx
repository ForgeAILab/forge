import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ReactQueryDevtools } from '@tanstack/react-query-devtools'
import { RouterProvider } from '@tanstack/react-router'
import NiceModal from '@ebay/nice-modal-react'
import { Toaster } from 'sonner'
import './index.css'
import { createAppRouter } from './router'
import './lib/i18n'
import { applyThemeFromStorage } from './stores/layout'

const devToolsEnabled = import.meta.env.DEV && import.meta.env.VITE_DISABLE_REACT_DEVTOOLS !== '1'

if (devToolsEnabled) {
  void import('react-grab')
  void import('react-scan')
}

applyThemeFromStorage()

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5_000,
      gcTime: 5 * 60_000,
      // Failed reads stay failed until the user retries or the SSE stream
      // proves the server is reachable again. Automatic retries across many
      // mounted queries turn one outage into a request storm.
      retry: false,
      refetchOnWindowFocus: false,
      refetchOnReconnect: false,
    },
  },
})

const router = createAppRouter(queryClient)

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <NiceModal.Provider>
        <RouterProvider router={router} />
        <Toaster richColors position="bottom-right" />
        {devToolsEnabled ? <ReactQueryDevtools initialIsOpen={false} /> : null}
      </NiceModal.Provider>
    </QueryClientProvider>
  </StrictMode>,
)
