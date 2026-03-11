import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom'
import Home from './pages/Home.jsx'
import ClientDetail from './pages/ClientDetail.jsx'
import RunPipeline from './pages/RunPipeline.jsx'

function Nav() {
  const linkClass = ({ isActive }) =>
    `px-4 py-2 rounded text-sm font-medium transition-colors ${
      isActive
        ? 'bg-white text-pivot-blue'
        : 'text-blue-100 hover:bg-blue-700 hover:text-white'
    }`
  return (
    <nav className="bg-pivot-blue text-white shadow-md">
      <div className="max-w-7xl mx-auto px-4 py-3 flex items-center gap-6">
        <span className="text-lg font-bold tracking-tight text-white mr-4">
          📊 PIVOT File Review
        </span>
        <NavLink to="/" end className={linkClass}>
          Dashboard
        </NavLink>
        <NavLink to="/run" className={linkClass}>
          Run Validation
        </NavLink>
      </div>
    </nav>
  )
}

export default function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen flex flex-col">
        <Nav />
        <main className="flex-1 max-w-7xl w-full mx-auto px-4 py-6">
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/run" element={<RunPipeline />} />
            <Route path="/client/:client" element={<ClientDetail />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  )
}
