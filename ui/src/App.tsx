import { BrowserRouter, Routes, Route, NavLink, useSearchParams } from 'react-router-dom'
import ODAGList from '@/pages/ODAGList'
import ODAGDetail from '@/pages/ODAGDetail'
import TemplateList from '@/pages/TemplateList'
import TemplateDetail from '@/pages/TemplateDetail'
import Cluster from '@/pages/Cluster'
import Compare from '@/pages/Compare'
import BatchExecution from '@/pages/BatchExecution'
import { useSSE } from '@/hooks/useSSE'
import { useTheme } from '@/hooks/useTheme'

function AppRoutes() {
  // Connect to the SSE stream once; invalidates queries on every server push.
  useSSE()

  return (
    <Routes>
      <Route path="/" element={<ODAGList />} />
      <Route path="/odags/:namespace/:name" element={<ODAGDetail />} />
      <Route path="/templates" element={<TemplateList />} />
      <Route path="/templates/odag/:namespace/:name" element={<TemplateDetail />} />
      <Route path="/cluster" element={<Cluster />} />
      <Route path="/compare" element={<Compare />} />
      <Route path="/batch" element={<BatchExecution />} />
    </Routes>
  )
}

/** NavLink that highlights when ?type= matches the expected value on /templates */
function TemplateNavLink({ type, children }: { type: string; children: React.ReactNode }) {
  return (
    <NavLink
      to={`/templates?type=${type}`}
      className={() => 'text-on-muted hover:text-on-secondary'}
    >
      {/* We render children through a wrapper that checks the real active state */}
      <TemplateNavInner type={type}>{children}</TemplateNavInner>
    </NavLink>
  )
}

function TemplateNavInner({ type, children }: { type: string; children: React.ReactNode }) {
  const [searchParams] = useSearchParams()
  const pathname = window.location.pathname
  const isActive = pathname === '/templates' && searchParams.get('type') === type
  return <span className={isActive ? 'text-on' : ''}>{children}</span>
}

export default function App() {
  const { theme, toggle } = useTheme()

  return (
    <BrowserRouter>
      <div className="min-h-screen bg-surface text-on font-mono">
        <header className="border-b border-line px-6 py-3 flex items-center gap-8">
          <span className="font-bold text-on tracking-tight">Wayline</span>
          <nav className="flex gap-6 text-sm">
            <TemplateNavLink type="odag">ODAG Templates</TemplateNavLink>
            <NavLink
              to="/"
              end
              className={({ isActive }) =>
                isActive ? 'text-on' : 'text-on-muted hover:text-on-secondary'
              }
            >
              ODAGs
            </NavLink>
            <NavLink
              to="/cluster"
              className={({ isActive }) =>
                isActive ? 'text-on' : 'text-on-muted hover:text-on-secondary'
              }
            >
              Cluster
            </NavLink>
            <NavLink
              to="/compare"
              className={({ isActive }) =>
                isActive ? 'text-on' : 'text-on-muted hover:text-on-secondary'
              }
            >
              Compare
            </NavLink>
          </nav>
          <button
            onClick={toggle}
            className="ml-auto text-on-muted hover:text-on text-sm px-2 py-1 border border-line rounded"
            title={`Switch to ${theme === 'light' ? 'dark' : 'light'} mode`}
          >
            {theme === 'light' ? 'Dark' : 'Light'}
          </button>
        </header>
        <main className="p-6">
          <AppRoutes />
        </main>
      </div>
    </BrowserRouter>
  )
}
