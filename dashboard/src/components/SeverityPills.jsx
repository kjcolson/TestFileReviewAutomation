const COLORS = {
  CRITICAL: 'bg-red-800/60 text-red-200',
  HIGH:     'bg-orange-800/60 text-orange-200',
  MEDIUM:   'bg-yellow-800/60 text-yellow-200',
  LOW:      'bg-blue-800/60 text-blue-200',
  INFO:     'bg-slate-700 text-slate-300',
}

export default function SeverityPills({ counts = {}, hideZero = true }) {
  const levels = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'INFO']
  return (
    <div className="flex flex-wrap gap-1">
      {levels.map((lvl) => {
        const n = counts[lvl] ?? 0
        if (hideZero && n === 0) return null
        return (
          <span
            key={lvl}
            className={`inline-flex items-center rounded text-xs px-2 py-0.5 font-medium ${COLORS[lvl]}`}
          >
            {lvl[0]}{lvl.slice(1).toLowerCase()}: {n}
          </span>
        )
      })}
    </div>
  )
}
