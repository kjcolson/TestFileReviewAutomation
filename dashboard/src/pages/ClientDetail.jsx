import { useQuery } from '@tanstack/react-query'
import { useParams, Link } from 'react-router-dom'
import { api } from '../lib/api.js'
import ReadinessBadge from '../components/ReadinessBadge.jsx'
import SeverityPills from '../components/SeverityPills.jsx'
import SourceCard from '../components/SourceCard.jsx'
import IssuesTable from '../components/IssuesTable.jsx'
import CrossSourceMatrix from '../components/CrossSourceMatrix.jsx'

const READINESS_BG = (verdict) => {
  const v = (verdict || '').toLowerCase()
  if (v.includes('ready for') || v === 'ready') return 'bg-green-50 border-green-300'
  if (v.includes('conditionally')) return 'bg-yellow-50 border-yellow-300'
  return 'bg-red-50 border-red-300'
}

function Section({ title, children }) {
  return (
    <section className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
      <h2 className="text-base font-bold text-gray-800 mb-4">{title}</h2>
      {children}
    </section>
  )
}

function MetaRow({ label, value }) {
  if (!value) return null
  return (
    <div className="flex gap-2 text-sm">
      <span className="text-gray-500 w-36 shrink-0">{label}</span>
      <span className="text-gray-800 font-medium">{value}</span>
    </div>
  )
}

export default function ClientDetail() {
  const { client } = useParams()

  const { data, isLoading, error } = useQuery({
    queryKey: ['findings', client],
    queryFn: () => api.getFindings(client),
  })

  if (isLoading) {
    return <div className="text-center py-16 text-gray-400">Loading findings…</div>
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700 text-sm">
        {error.message}
      </div>
    )
  }

  const readiness = data?.readiness || {}
  const verdict = readiness.overall || 'Unknown'
  const total = readiness.total_counts || {}
  const perSource = readiness.per_source || {}
  const sourceSummary = data?.source_summary || {}
  const issues = data?.client_issues || []
  const crossSource = data?.cross_source_summary || {}
  const phaseMeta = data?.phase_metadata || {}
  const round = data?.round || ''
  const reportUrl = api.downloadReport(client)

  // Merge source summary + readiness per_source for the cards
  const sourceKeys = new Set([...Object.keys(perSource), ...Object.keys(sourceSummary)])
  const sourceCards = [...sourceKeys].filter((k) => k !== 'cross_source')

  return (
    <div className="space-y-6">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-sm text-gray-500">
        <Link to="/" className="hover:text-pivot-teal">Dashboard</Link>
        <span>/</span>
        <span className="text-gray-800 font-medium">{client}</span>
        <span>/</span>
        <span className="text-gray-800">{round}</span>
      </div>

      {/* Readiness Banner */}
      <div className={`rounded-xl border-2 p-5 ${READINESS_BG(verdict)}`}>
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <ReadinessBadge verdict={verdict} size="lg" />
            {readiness.reason && (
              <p className="text-sm text-gray-600 mt-1">{readiness.reason}</p>
            )}
          </div>
          <SeverityPills counts={total} hideZero={false} />
          <a
            href={reportUrl}
            className="bg-white border border-gray-300 text-gray-700 px-4 py-2 rounded-lg text-sm font-medium hover:bg-gray-50 transition-colors flex items-center gap-2"
            download
          >
            ⬇ Download Phase 5 Report
          </a>
        </div>
      </div>

      {/* Metadata */}
      <Section title="Run Summary">
        <div className="grid grid-cols-2 gap-2">
          <MetaRow label="Client" value={data?.client} />
          <MetaRow label="Round" value={data?.round} />
          <MetaRow label="Test Month" value={data?.test_month} />
          <MetaRow label="Billing Format" value={data?.billing_format} />
          <MetaRow label="Phase 1 run" value={phaseMeta.phase1_date} />
          <MetaRow label="Phase 2 run" value={phaseMeta.phase2_date} />
          <MetaRow label="Phase 3 run" value={phaseMeta.phase3_date} />
          <MetaRow label="Phase 4 run" value={phaseMeta.phase4_date} />
        </div>
      </Section>

      {/* Source Cards */}
      {sourceCards.length > 0 && (
        <Section title="Source Results">
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {sourceCards.map((key) => {
              const merged = {
                ...(sourceSummary[key] || {}),
                ...(perSource[key] || {}),
              }
              return <SourceCard key={key} name={key} data={merged} />
            })}
          </div>
          {perSource.cross_source && (
            <div className="mt-3">
              <SourceCard name="cross_source" data={perSource.cross_source} />
            </div>
          )}
        </Section>
      )}

      {/* Issues Table */}
      <Section title={`Issues (${issues.length})`}>
        {issues.length === 0 ? (
          <p className="text-sm text-gray-400 italic">No issues found.</p>
        ) : (
          <IssuesTable issues={issues} />
        )}
      </Section>

      {/* Cross-Source Matrix */}
      <Section title="Cross-Source Validation">
        <CrossSourceMatrix summary={crossSource} />
      </Section>
    </div>
  )
}
