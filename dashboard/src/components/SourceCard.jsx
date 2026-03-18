import SeverityPills from './SeverityPills.jsx'

const STATUS_STYLE = {
  PASS:        'bg-green-900/20 border-green-700',
  CONDITIONAL: 'bg-yellow-900/20 border-yellow-700',
  FAIL:        'bg-red-900/20 border-red-700',
}
const STATUS_BADGE = {
  PASS:        'bg-green-800/50 text-green-300',
  CONDITIONAL: 'bg-yellow-800/50 text-yellow-300',
  FAIL:        'bg-red-800/50 text-red-300',
}

function fmt(n) {
  if (n == null) return '—'
  return Number(n).toLocaleString()
}

export default function SourceCard({ name, data }) {
  const status = data?.status || 'UNKNOWN'
  const cardStyle = STATUS_STYLE[status] || 'bg-pivot-surface border-pivot-border'
  const badgeStyle = STATUS_BADGE[status] || 'bg-slate-700 text-slate-300'

  const dateRange = data?.date_range
  const dateStr = dateRange
    ? `${dateRange.min ?? '?'} → ${dateRange.max ?? '?'}`
    : null

  return (
    <div className={`rounded-lg border-2 p-4 ${cardStyle}`}>
      <div className="flex items-center justify-between mb-2">
        <span className="font-semibold text-sm text-pivot-textPrimary">{data?.display_name || name}</span>
        <span className={`text-xs font-bold px-2 py-0.5 rounded-full ${badgeStyle}`}>
          {status}
        </span>
      </div>
      <div className="text-xs text-pivot-textMuted mb-2">
        {data?.row_count != null && (
          <span>{fmt(data.row_count)} rows</span>
        )}
        {dateStr && (
          <span className="ml-2 truncate">{dateStr}</span>
        )}
      </div>
      <SeverityPills counts={data?.severity_counts} />
    </div>
  )
}
