import { useState, useMemo } from 'react'

const SEV_ORDER = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'INFO']

const SEV_BADGE = {
  CRITICAL: 'bg-red-800/60 text-red-200',
  HIGH:     'bg-orange-800/60 text-orange-200',
  MEDIUM:   'bg-yellow-800/60 text-yellow-200',
  LOW:      'bg-blue-800/60 text-blue-200',
  INFO:     'bg-slate-700 text-slate-300',
}

const SEV_PILL_ACTIVE = {
  CRITICAL: 'bg-red-700 text-red-100 border-red-500',
  HIGH:     'bg-orange-700 text-orange-100 border-orange-500',
  MEDIUM:   'bg-yellow-700 text-yellow-100 border-yellow-500',
  LOW:      'bg-blue-700 text-blue-100 border-blue-500',
  INFO:     'bg-slate-600 text-slate-200 border-slate-400',
}

const SEV_PILL_INACTIVE = {
  CRITICAL: 'bg-transparent text-red-400 border-red-800 opacity-50',
  HIGH:     'bg-transparent text-orange-400 border-orange-800 opacity-50',
  MEDIUM:   'bg-transparent text-yellow-400 border-yellow-800 opacity-50',
  LOW:      'bg-transparent text-blue-400 border-blue-800 opacity-50',
  INFO:     'bg-transparent text-slate-500 border-slate-700 opacity-50',
}

function generateEmailText(data, selectedIds) {
  const round = data?.round || '?'
  const testMonth = data?.test_month || '?'
  const selected = (data?.client_issues || []).filter((i) => selectedIds.has(i.id))

  if (selected.length === 0) {
    return [
      'Hi team,',
      '',
      `We\u2019ve completed our review of Round ${round} (test month: ${testMonth}). The data looks good \u2014 no critical issues to flag.`,
      '',
      'Full details are in the attached report.',
      '',
      'Let us know if you have any questions.',
    ].join('\n')
  }

  // Group selected issues by source_display
  const grouped = {}
  for (const issue of selected) {
    const src = issue.source_display || issue.source || 'General'
    if (!grouped[src]) grouped[src] = []
    grouped[src].push(issue.description)
  }

  const lines = [
    'Hi team,',
    '',
    `We\u2019ve completed our review of Round ${round} (test month: ${testMonth}). Please address the following before resubmission:`,
    '',
  ]

  for (const [source, descriptions] of Object.entries(grouped)) {
    lines.push(`${source}:`)
    for (const desc of descriptions) {
      lines.push(`  \u2022 ${desc}`)
    }
    lines.push('')
  }

  lines.push('Full details are in the attached report.')
  lines.push('')
  lines.push('Let us know if you have any questions.')

  return lines.join('\n')
}

export default function FeedbackEmail({ data }) {
  const [copied, setCopied] = useState(false)

  // All issues (not just CRITICAL/HIGH)
  const allIssues = useMemo(() => data?.client_issues || [], [data])

  // CRITICAL checked by default
  const [selectedIds, setSelectedIds] = useState(() => {
    const initial = new Set()
    for (const i of (data?.client_issues || [])) {
      if (i.severity === 'CRITICAL') initial.add(i.id)
    }
    return initial
  })

  // Severity visibility filter — CRITICAL and HIGH shown by default
  const [visibleSevs, setVisibleSevs] = useState(new Set(['CRITICAL', 'HIGH']))

  // Source filter
  const [srcFilter, setSrcFilter] = useState('')

  // Available sources
  const sources = useMemo(() => {
    const s = new Set(allIssues.map((i) => i.source_display).filter(Boolean))
    return [...s].sort()
  }, [allIssues])

  // Severity counts
  const sevCounts = useMemo(() => {
    const counts = {}
    for (const sev of SEV_ORDER) counts[sev] = 0
    for (const i of allIssues) counts[i.severity] = (counts[i.severity] || 0) + 1
    return counts
  }, [allIssues])

  // Filtered issues (by severity visibility + source)
  const filteredIssues = useMemo(() => {
    return allIssues.filter((i) => {
      if (!visibleSevs.has(i.severity)) return false
      if (srcFilter && i.source_display !== srcFilter) return false
      return true
    })
  }, [allIssues, visibleSevs, srcFilter])

  // Group filtered issues by source
  const groupedBySource = useMemo(() => {
    const groups = {}
    for (const issue of filteredIssues) {
      const src = issue.source_display || issue.source || 'General'
      if (!groups[src]) groups[src] = []
      groups[src].push(issue)
    }
    return groups
  }, [filteredIssues])

  function toggleSev(sev) {
    setVisibleSevs((prev) => {
      const next = new Set(prev)
      if (next.has(sev)) next.delete(sev)
      else next.add(sev)
      return next
    })
  }

  function toggleIssue(id) {
    setSelectedIds((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  function selectAllVisible() {
    setSelectedIds((prev) => {
      const next = new Set(prev)
      for (const i of filteredIssues) next.add(i.id)
      return next
    })
  }

  function selectNoneVisible() {
    setSelectedIds((prev) => {
      const next = new Set(prev)
      for (const i of filteredIssues) next.delete(i.id)
      return next
    })
  }

  function toggleSourceGroup(sourceIssues) {
    const allSelected = sourceIssues.every((i) => selectedIds.has(i.id))
    setSelectedIds((prev) => {
      const next = new Set(prev)
      for (const i of sourceIssues) {
        if (allSelected) next.delete(i.id)
        else next.add(i.id)
      }
      return next
    })
  }

  const emailText = useMemo(
    () => generateEmailText(data, selectedIds),
    [data, selectedIds]
  )

  async function handleCopy() {
    await navigator.clipboard.writeText(emailText)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <section className="bg-pivot-surface rounded-xl shadow-sm border border-pivot-border p-6">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-base font-bold text-pivot-textPrimary">
          Feedback Email
          <span className="text-xs font-normal text-pivot-textMuted ml-2">
            {selectedIds.size} selected
          </span>
        </h2>
        <button
          onClick={handleCopy}
          className="text-xs px-3 py-1.5 bg-pivot-teal text-white hover:bg-teal-600 rounded transition-colors font-medium"
        >
          {copied ? '\u2713 Copied' : 'Copy to Clipboard'}
        </button>
      </div>

      {/* Filter bar */}
      <div className="flex flex-wrap items-center gap-2 mb-4">
        {SEV_ORDER.map((sev) => {
          const active = visibleSevs.has(sev)
          const count = sevCounts[sev] || 0
          if (count === 0) return null
          return (
            <button
              key={sev}
              onClick={() => toggleSev(sev)}
              className={`text-xs px-2.5 py-1 rounded-full border font-medium transition-all ${
                active ? SEV_PILL_ACTIVE[sev] : SEV_PILL_INACTIVE[sev]
              }`}
            >
              {sev} ({count})
            </button>
          )
        })}
        <select
          value={srcFilter}
          onChange={(e) => setSrcFilter(e.target.value)}
          className="text-xs border border-slate-600 bg-slate-800 text-slate-200 rounded px-2 py-1 ml-auto"
        >
          <option value="">All Sources</option>
          {sources.map((s) => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Left: issue selector grouped by source */}
        <div>
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-pivot-textSecondary">Select issues to include:</span>
            <div className="flex gap-2">
              <button onClick={selectAllVisible} className="text-xs text-pivot-teal hover:underline">All</button>
              <button onClick={selectNoneVisible} className="text-xs text-pivot-textMuted hover:underline">None</button>
            </div>
          </div>
          <div className="border border-slate-700 rounded-lg max-h-96 overflow-y-auto">
            {Object.keys(groupedBySource).length === 0 && (
              <p className="text-sm text-pivot-textMuted italic p-4 text-center">
                No issues match the current filters.
              </p>
            )}
            {Object.entries(groupedBySource).map(([source, sourceIssues]) => {
              const allSelected = sourceIssues.every((i) => selectedIds.has(i.id))
              const someSelected = sourceIssues.some((i) => selectedIds.has(i.id))
              return (
                <div key={source}>
                  {/* Source group header */}
                  <div className="flex items-center justify-between px-3 py-2 bg-slate-700/50 border-b border-slate-700 sticky top-0">
                    <label className="flex items-center gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={allSelected}
                        ref={(el) => { if (el) el.indeterminate = someSelected && !allSelected }}
                        onChange={() => toggleSourceGroup(sourceIssues)}
                        className="w-3.5 h-3.5 accent-pivot-teal"
                      />
                      <span className="text-sm font-semibold text-pivot-textPrimary">{source}</span>
                      <span className="text-xs text-pivot-textMuted">({sourceIssues.length})</span>
                    </label>
                  </div>
                  {/* Issues in this source */}
                  <div className="divide-y divide-slate-700/50">
                    {sourceIssues.map((issue) => (
                      <label
                        key={issue.id}
                        className={`flex items-start gap-2 px-3 py-2 cursor-pointer hover:bg-slate-700/30 transition-colors ${
                          selectedIds.has(issue.id) ? '' : 'opacity-40'
                        }`}
                      >
                        <input
                          type="checkbox"
                          checked={selectedIds.has(issue.id)}
                          onChange={() => toggleIssue(issue.id)}
                          className="mt-0.5 w-4 h-4 accent-pivot-teal shrink-0"
                        />
                        <div className="min-w-0">
                          <div className="flex items-center gap-2">
                            <span className="text-xs font-mono text-pivot-textMuted">{issue.id}</span>
                            <span className={`text-xs font-semibold px-1.5 py-0.5 rounded-full ${SEV_BADGE[issue.severity] || ''}`}>
                              {issue.severity}
                            </span>
                          </div>
                          <p className="text-sm text-pivot-textPrimary mt-0.5 leading-snug">{issue.description}</p>
                        </div>
                      </label>
                    ))}
                  </div>
                </div>
              )
            })}
          </div>
          <p className="text-xs text-pivot-textMuted mt-1">
            {selectedIds.size} of {allIssues.length} selected
          </p>
        </div>

        {/* Right: email preview */}
        <div>
          <span className="text-sm font-medium text-pivot-textSecondary mb-2 block">Preview:</span>
          <pre className="bg-slate-800 border border-slate-700 rounded-lg p-4 text-sm text-pivot-textPrimary whitespace-pre-wrap leading-relaxed max-h-96 overflow-y-auto font-sans">
            {emailText}
          </pre>
        </div>
      </div>
    </section>
  )
}
