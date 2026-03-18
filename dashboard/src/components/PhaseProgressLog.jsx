import { useEffect, useRef, useState, useCallback } from 'react'

const PHASE_PATTERNS = [
  { pattern: /phase 1/i, label: 'Phase 1 — Ingestion' },
  { pattern: /phase 2/i, label: 'Phase 2 — Schema Validation' },
  { pattern: /phase 3/i, label: 'Phase 3 — Data Quality' },
  { pattern: /phase 4/i, label: 'Phase 4 — Cross-Source' },
  { pattern: /phase 5/i, label: 'Phase 5 — Aggregation' },
]

export default function PhaseProgressLog({ jobId, onComplete }) {
  const [lines, setLines] = useState([])
  const [done, setDone] = useState(false)
  const [exitCode, setExitCode] = useState(null)
  const bottomRef = useRef(null)
  // Ref so onerror can check done without stale closure
  const doneRef = useRef(false)

  useEffect(() => {
    if (!jobId) return
    doneRef.current = false
    const es = new EventSource(`/api/run/${jobId}/progress`)

    es.onmessage = (e) => {
      const data = e.data
      // SSE comments (": connected") have empty data — ignore them
      if (!data) return
      if (data.startsWith('__DONE__')) {
        const match = data.match(/exit=(\d+)/)
        const code = match ? parseInt(match[1]) : 0
        setExitCode(code)
        setDone(true)
        doneRef.current = true
        es.close()
        if (onComplete) onComplete(code)
        return
      }
      setLines((prev) => [...prev, data])
    }

    es.onerror = () => {
      // Don't show "Connection lost" if the job already finished normally —
      // the browser EventSource fires onerror when the server closes the stream.
      if (!doneRef.current) {
        setLines((prev) => [...prev, '[Connection lost — check that the server is still running]'])
      }
      es.close()
    }

    return () => es.close()
  }, [jobId, onComplete])

  // Auto-scroll to bottom
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [lines])

  const completedPhases = PHASE_PATTERNS.filter(({ pattern }) =>
    lines.some((l) => pattern.test(l))
  )

  return (
    <div className="space-y-4">
      {/* Phase checkmarks */}
      <div className="flex flex-wrap gap-3">
        {PHASE_PATTERNS.map(({ label }, i) => {
          const completed = completedPhases.some((p) => p.label === label)
          const isNext = !completed && completedPhases.length === i
          return (
            <div
              key={label}
              className={`flex items-center gap-1.5 text-sm px-3 py-1.5 rounded-full border ${
                completed
                  ? 'bg-green-900/30 border-green-700 text-green-300'
                  : isNext && !done
                  ? 'bg-blue-900/30 border-blue-700 text-blue-300 animate-pulse'
                  : 'bg-slate-800 border-slate-700 text-pivot-textMuted'
              }`}
            >
              {completed ? '\u2713' : isNext && !done ? '\u22EF' : '\u25CB'}
              <span>{label}</span>
            </div>
          )
        })}
      </div>

      {/* Log output */}
      <div className="bg-gray-950 text-green-400 rounded-lg p-4 h-80 overflow-y-auto font-mono text-xs leading-relaxed">
        {lines.length === 0 && !done && (
          <span className="text-slate-500 italic">Starting pipeline…</span>
        )}
        {lines.map((line, i) => (
          <div key={i} className="whitespace-pre-wrap break-all">{line}</div>
        ))}
        {done && (
          <div className={`mt-2 font-bold ${exitCode === 0 ? 'text-green-300' : 'text-red-400'}`}>
            {exitCode === 0
              ? '\u2713 Pipeline completed successfully.'
              : `\u2717 Pipeline exited with code ${exitCode}.`}
          </div>
        )}
        <div ref={bottomRef} />
      </div>
    </div>
  )
}
