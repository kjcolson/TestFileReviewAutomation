import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useParams, Link } from 'react-router-dom'
import { api } from '../lib/api.js'
import ReadinessBadge from '../components/ReadinessBadge.jsx'
import SeverityPills from '../components/SeverityPills.jsx'
import SourceCard from '../components/SourceCard.jsx'
import IssuesTable from '../components/IssuesTable.jsx'
import CrossSourceMatrix from '../components/CrossSourceMatrix.jsx'
import Spinner from '../components/Spinner.jsx'
import Breadcrumb from '../components/Breadcrumb.jsx'
import FeedbackEmail from '../components/FeedbackEmail.jsx'

const READINESS_BG = (verdict) => {
  const v = (verdict || '').toLowerCase()
  if (v.includes('ready for') || v === 'ready') return 'bg-green-900/30 border-green-700'
  if (v.includes('conditionally')) return 'bg-yellow-900/30 border-yellow-700'
  return 'bg-red-900/30 border-red-700'
}

function Section({ title, children }) {
  return (
    <section className="bg-pivot-surface rounded-xl shadow-sm border border-pivot-border p-6">
      <h2 className="text-base font-bold text-pivot-textPrimary mb-4">{title}</h2>
      {children}
    </section>
  )
}

function MetaRow({ label, value }) {
  if (!value) return null
  return (
    <div className="flex gap-2 text-sm">
      <span className="text-pivot-textSecondary w-36 shrink-0">{label}</span>
      <span className="text-pivot-textPrimary font-medium">{value}</span>
    </div>
  )
}

export default function ClientDetail() {
  const { client } = useParams()
  const [feedbackOpen, setFeedbackOpen] = useState(false)

  const { data, isLoading, error } = useQuery({
    queryKey: ['findings', client],
    queryFn: () => api.getFindings(client),
  })

  if (isLoading) {
    return <Spinner label="Loading findings…" />
  }

  if (error) {
    return (
      <div className="bg-red-900/30 border border-red-800 rounded-lg p-4 text-red-300 text-sm">
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

  const critHighCount = issues.filter(
    (i) => i.severity === 'CRITICAL' || i.severity === 'HIGH'
  ).length

  // Merge source summary + readiness per_source for the cards
  const sourceKeys = new Set([...Object.keys(perSource), ...Object.keys(sourceSummary)])
  const sourceCards = [...sourceKeys].filter((k) => k !== 'cross_source')

  return (
    <div className="space-y-6">
      <Breadcrumb items={[
        { label: 'Dashboard', to: '/' },
        { label: client },
        { label: round },
      ]} />

      {/* Readiness Banner */}
      <div className={`rounded-xl border-2 p-5 ${READINESS_BG(verdict)}`}>
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <ReadinessBadge verdict={verdict} size="lg" />
            {readiness.reason && (
              <p className="text-sm text-pivot-textSecondary mt-1">{readiness.reason}</p>
            )}
          </div>
          <SeverityPills counts={total} hideZero={false} />
          <div className="flex flex-wrap gap-2">
            <a
              href={reportUrl}
              className="bg-pivot-surface border border-pivot-border text-pivot-textPrimary px-4 py-2 rounded-lg text-sm font-medium hover:bg-pivot-surfaceHover transition-colors flex items-center gap-2"
              download
            >
              Download Phase 5 Report
            </a>
            <Link
              to={`/client/${client}/generate`}
              className="bg-pivot-blue text-white px-4 py-2 rounded-lg text-sm font-medium hover:bg-blue-700 transition-colors flex items-center gap-2"
            >
              Generate SQL
            </Link>
            <button
              onClick={() => setFeedbackOpen((o) => !o)}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors flex items-center gap-2 ${
                feedbackOpen
                  ? 'bg-pivot-teal text-white'
                  : 'border border-pivot-teal text-pivot-teal hover:bg-pivot-teal hover:text-white'
              }`}
            >
              Feedback Email
              {critHighCount > 0 && (
                <span className="bg-red-800/60 text-red-200 text-xs px-1.5 py-0.5 rounded-full font-medium">
                  {critHighCount}
                </span>
              )}
            </button>
          </div>
        </div>
      </div>

      {/* Feedback Email Builder — expands below banner */}
      {feedbackOpen && <FeedbackEmail data={data} />}

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
          <p className="text-sm text-pivot-textMuted italic">No issues found.</p>
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
