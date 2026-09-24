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

const linkClass = ({ isActive }: { isActive: boolean }) =>
  isActive
    ? 'text-ctrl font-semibold border-b-2 border-ctrl pb-0.5'
    : 'text-on-muted hover:text-on border-b-2 border-transparent pb-0.5'

/** NavLink that highlights when ?type= matches the expected value on /templates */
function TemplateNavLink({ type, children }: { type: string; children: React.ReactNode }) {
  const [searchParams] = useSearchParams()
  const isActive = window.location.pathname === '/templates' && searchParams.get('type') === type
  return (
    <NavLink to={`/templates?type=${type}`} className={() => linkClass({ isActive })}>
      {children}
    </NavLink>
  )
}

export default function App() {
  const { theme, toggle } = useTheme()

  return (
    <BrowserRouter>
      <div className="min-h-screen bg-surface text-on font-sans">
        <header className="border-b-2 border-ctrl px-6 py-3 flex items-center gap-8">
          <span className="text-lg font-bold text-ctrl tracking-tight">Wayline</span>
          <nav className="flex gap-6 text-sm">
            <TemplateNavLink type="odag">Templates</TemplateNavLink>
            <NavLink to="/" end className={linkClass}>Runs</NavLink>
            <NavLink to="/cluster" className={linkClass}>Cluster</NavLink>
            <NavLink to="/compare" className={linkClass}>Compare</NavLink>
            <NavLink to="/batch" className={linkClass}>Batch</NavLink>
          </nav>
          <button
            onClick={toggle}
            className="ml-auto text-on-muted hover:text-on text-xs px-2 py-1 border border-line rounded-sm"
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
