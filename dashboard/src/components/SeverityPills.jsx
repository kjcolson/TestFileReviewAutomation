const COLORS = {
  CRITICAL: 'bg-red-100 text-red-800',
  HIGH:     'bg-orange-100 text-orange-800',
  MEDIUM:   'bg-yellow-100 text-yellow-800',
  LOW:      'bg-blue-100 text-blue-800',
  INFO:     'bg-gray-100 text-gray-600',
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
