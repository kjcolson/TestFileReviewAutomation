export default function ReadinessBadge({ verdict, size = 'md' }) {
  const v = (verdict || '').toLowerCase()
  let color = 'bg-slate-700 text-slate-300'
  let label = verdict || 'Unknown'

  if (v.includes('ready for') || v === 'ready') {
    color = 'bg-green-800/50 text-green-300 border border-green-600'
  } else if (v.includes('conditionally')) {
    color = 'bg-yellow-800/50 text-yellow-300 border border-yellow-600'
  } else if (v.includes('needs revision') || v.includes('revision')) {
    color = 'bg-red-800/50 text-red-300 border border-red-600'
  }

  const sizeClass = size === 'lg'
    ? 'text-base px-4 py-2 font-semibold'
    : 'text-xs px-2.5 py-1 font-medium'

  return (
    <span className={`inline-flex items-center rounded-full ${sizeClass} ${color}`}>
      {label}
    </span>
  )
}
