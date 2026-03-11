const STATUS_STYLE = {
  PASS:        'bg-green-100 text-green-800',
  CONDITIONAL: 'bg-yellow-100 text-yellow-800',
  FAIL:        'bg-red-100 text-red-800',
}
const SEV_STYLE = {
  CRITICAL: 'text-red-700 font-semibold',
  HIGH:     'text-orange-600 font-semibold',
  MEDIUM:   'text-yellow-600',
  LOW:      'text-blue-600',
  INFO:     'text-gray-500',
}

const CHECK_LABELS = {
  C0:  'C0 — Transactions ↔ Charges',
  C0a: 'C0a — Charge ID match rate',
  C0b: 'C0b — Payment balance',
  C1:  'C1 — Billing ↔ GL Cost Center',
  C2:  'C2 — Billing ↔ Payroll NPI',
  C3:  'C3 — Billing ↔ Scheduling',
  C3a: 'C3a — Location/Dept',
  C3b: 'C3b — Provider NPI',
  C3c: 'C3c — Patient ID',
  C4:  'C4 — Payroll ↔ GL Dept',
  C5:  'C5 — Scheduling ↔ GL Location',
}

export default function CrossSourceMatrix({ summary }) {
  if (!summary || Object.keys(summary).length === 0) {
    return <p className="text-sm text-gray-400 italic">No cross-source checks were run.</p>
  }

  const rows = Object.entries(summary)

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm border-collapse">
        <thead>
          <tr className="bg-gray-100 text-left">
            <th className="px-3 py-2 font-semibold text-gray-700 w-48">Check</th>
            <th className="px-3 py-2 font-semibold text-gray-700 w-28">Status</th>
            <th className="px-3 py-2 font-semibold text-gray-700 w-24">Severity</th>
            <th className="px-3 py-2 font-semibold text-gray-700">Message</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(([key, check]) => {
            const statusStyle = STATUS_STYLE[check?.status] || 'bg-gray-100 text-gray-700'
            const sevStyle = SEV_STYLE[check?.severity] || 'text-gray-600'
            return (
              <tr key={key} className="border-t border-gray-200 hover:bg-gray-50">
                <td className="px-3 py-2 font-medium text-gray-700">
                  {CHECK_LABELS[key] || key}
                </td>
                <td className="px-3 py-2">
                  <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${statusStyle}`}>
                    {check?.status || '—'}
                  </span>
                </td>
                <td className={`px-3 py-2 text-xs ${sevStyle}`}>
                  {check?.severity || '—'}
                </td>
                <td className="px-3 py-2 text-gray-600 text-xs leading-relaxed">
                  {check?.message || '—'}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
