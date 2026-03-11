import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { api } from '../lib/api.js'
import ReadinessBadge from '../components/ReadinessBadge.jsx'
import SeverityPills from '../components/SeverityPills.jsx'

function ClientCard({ entry }) {
  return (
    <Link
      to={`/client/${encodeURIComponent(entry.client)}`}
      className="block bg-white rounded-xl shadow-sm border border-gray-200 p-5 hover:shadow-md hover:border-pivot-teal transition-all"
    >
      <div className="flex items-start justify-between mb-2">
        <div>
          <h3 className="text-base font-bold text-gray-900">{entry.client}</h3>
          <p className="text-xs text-gray-500">Round {entry.round}</p>
        </div>
        <ReadinessBadge verdict={entry.readiness} />
      </div>
      {entry.test_month && (
        <p className="text-xs text-gray-400 mb-2">Test month: {entry.test_month}</p>
      )}
      <SeverityPills
        counts={{
          CRITICAL: entry.critical,
          HIGH: entry.high,
          MEDIUM: entry.medium,
          LOW: entry.low,
        }}
      />
      {entry.date_run && (
        <p className="text-xs text-gray-400 mt-2">Run: {entry.date_run}</p>
      )}
    </Link>
  )
}

export default function Home() {
  const { data, isLoading, error } = useQuery({
    queryKey: ['clients'],
    queryFn: api.listClients,
  })

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Client Validations</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            Click a card to view findings, or run a new validation below.
          </p>
        </div>
        <Link
          to="/run"
          className="bg-pivot-blue text-white px-4 py-2 rounded-lg text-sm font-medium hover:bg-blue-900 transition-colors"
        >
          + Run New Validation
        </Link>
      </div>

      {isLoading && (
        <div className="text-center py-16 text-gray-400">Loading clients…</div>
      )}

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700 text-sm">
          Failed to load clients: {error.message}
        </div>
      )}

      {data && data.length === 0 && (
        <div className="text-center py-16">
          <p className="text-gray-400 text-lg mb-4">No completed validations yet.</p>
          <Link
            to="/run"
            className="bg-pivot-blue text-white px-6 py-3 rounded-lg text-sm font-medium hover:bg-blue-900 transition-colors"
          >
            Run Your First Validation
          </Link>
        </div>
      )}

      {data && data.length > 0 && (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {data.map((entry) => (
            <ClientCard key={`${entry.client}-${entry.round}`} entry={entry} />
          ))}
        </div>
      )}
    </div>
  )
}
