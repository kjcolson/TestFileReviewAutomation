export default function ReadinessBadge({ verdict, size = 'md' }) {
  const v = (verdict || '').toLowerCase()
  let color = 'bg-gray-100 text-gray-700'
  let label = verdict || 'Unknown'

  if (v.includes('ready for') || v === 'ready') {
    color = 'bg-green-100 text-green-800 border border-green-300'
  } else if (v.includes('conditionally')) {
    color = 'bg-yellow-100 text-yellow-800 border border-yellow-300'
  } else if (v.includes('needs revision') || v.includes('revision')) {
    color = 'bg-red-100 text-red-800 border border-red-300'
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
