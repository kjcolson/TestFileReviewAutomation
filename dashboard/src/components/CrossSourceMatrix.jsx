const STATUS_STYLE = {
  PASS:        'bg-green-800/50 text-green-300',
  CONDITIONAL: 'bg-yellow-800/50 text-yellow-300',
  FAIL:        'bg-red-800/50 text-red-300',
}
const SEV_STYLE = {
  CRITICAL: 'text-red-400 font-semibold',
  HIGH:     'text-orange-400 font-semibold',
  MEDIUM:   'text-yellow-400',
  LOW:      'text-blue-400',
  INFO:     'text-slate-400',
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
    return <p className="text-sm text-pivot-textMuted italic">No cross-source checks were run.</p>
  }

  const rows = Object.entries(summary)

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm border-collapse">
        <thead>
          <tr className="bg-slate-700 text-left">
            <th className="px-3 py-2 font-semibold text-slate-200 w-48">Check</th>
            <th className="px-3 py-2 font-semibold text-slate-200 w-28">Status</th>
            <th className="px-3 py-2 font-semibold text-slate-200 w-24">Severity</th>
            <th className="px-3 py-2 font-semibold text-slate-200">Message</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(([key, check]) => {
            const statusStyle = STATUS_STYLE[check?.status] || 'bg-slate-700 text-slate-300'
            const sevStyle = SEV_STYLE[check?.severity] || 'text-slate-400'
            return (
              <tr key={key} className="border-t border-slate-700 hover:bg-slate-700/50">
                <td className="px-3 py-2 font-medium text-pivot-textPrimary">
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
                <td className="px-3 py-2 text-pivot-textSecondary text-xs leading-relaxed">
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
