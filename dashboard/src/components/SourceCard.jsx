import SeverityPills from './SeverityPills.jsx'

const STATUS_STYLE = {
  PASS:        'bg-green-50 border-green-300',
  CONDITIONAL: 'bg-yellow-50 border-yellow-300',
  FAIL:        'bg-red-50 border-red-300',
}
const STATUS_BADGE = {
  PASS:        'bg-green-100 text-green-800',
  CONDITIONAL: 'bg-yellow-100 text-yellow-800',
  FAIL:        'bg-red-100 text-red-800',
}

function fmt(n) {
  if (n == null) return '—'
  return Number(n).toLocaleString()
}

export default function SourceCard({ name, data }) {
  const status = data?.status || 'UNKNOWN'
  const cardStyle = STATUS_STYLE[status] || 'bg-gray-50 border-gray-200'
  const badgeStyle = STATUS_BADGE[status] || 'bg-gray-100 text-gray-700'

  const dateRange = data?.date_range
  const dateStr = dateRange
    ? `${dateRange.min ?? '?'} → ${dateRange.max ?? '?'}`
    : null

  return (
    <div className={`rounded-lg border-2 p-4 ${cardStyle}`}>
      <div className="flex items-center justify-between mb-2">
        <span className="font-semibold text-sm text-gray-800">{data?.display_name || name}</span>
        <span className={`text-xs font-bold px-2 py-0.5 rounded-full ${badgeStyle}`}>
          {status}
        </span>
      </div>
      <div className="text-xs text-gray-500 mb-2">
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
