import { useState, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { api } from '../lib/api.js'
import PhaseProgressLog from '../components/PhaseProgressLog.jsx'

export default function RunPipeline() {
  const navigate = useNavigate()
  const [form, setForm] = useState({ client: '', round: 'v1', dateStart: '', dateEnd: '' })
  const [jobId, setJobId] = useState(null)
  const [error, setError] = useState('')
  const [submitting, setSubmitting] = useState(false)

  const handleSubmit = async (e) => {
    e.preventDefault()
    setError('')
    if (!form.client.trim()) { setError('Client name is required.'); return }
    if (!form.round.trim()) { setError('Round is required.'); return }

    setSubmitting(true)
    try {
      const res = await api.startRun({
        client: form.client.trim(),
        round: form.round.trim(),
        date_start: form.dateStart,
        date_end: form.dateEnd,
      })
      setJobId(res.job_id)
    } catch (err) {
      setError(err.message)
    } finally {
      setSubmitting(false)
    }
  }

  const handleComplete = useCallback(
    (exitCode) => {
      if (exitCode === 0) {
        setTimeout(() => {
          navigate(`/client/${encodeURIComponent(form.client.trim())}`)
        }, 1500)
      }
    },
    [form.client, navigate]
  )

  return (
    <div className="max-w-2xl">
      <h1 className="text-2xl font-bold text-pivot-textPrimary mb-1">Run Validation</h1>
      <p className="text-sm text-pivot-textSecondary mb-6">
        Make sure client files are placed in{' '}
        <code className="bg-slate-800 px-1 rounded text-xs text-slate-300">input/ClientName/source_type/</code>{' '}
        before running.
      </p>

      {!jobId && (
        <form onSubmit={handleSubmit} className="bg-pivot-surface rounded-xl shadow-sm border border-pivot-border p-6 space-y-4">
          <div>
            <label className="block text-sm font-medium text-pivot-textSecondary mb-1">
              Client Name <span className="text-red-400">*</span>
            </label>
            <input
              type="text"
              placeholder="e.g. Franciscan"
              value={form.client}
              onChange={(e) => setForm((f) => ({ ...f, client: e.target.value }))}
              className="w-full border border-slate-600 bg-slate-800 text-slate-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-pivot-teal placeholder:text-slate-500"
            />
            <p className="text-xs text-pivot-textMuted mt-1">
              Must match the folder name inside <code className="bg-slate-800 px-1 rounded text-slate-300">input/</code>
            </p>
          </div>

          <div>
            <label className="block text-sm font-medium text-pivot-textSecondary mb-1">
              Round <span className="text-red-400">*</span>
            </label>
            <select
              value={form.round}
              onChange={(e) => setForm((f) => ({ ...f, round: e.target.value }))}
              className="border border-slate-600 bg-slate-800 text-slate-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-pivot-teal"
            >
              {['v1', 'v2', 'v3', 'v4', 'v5'].map((r) => (
                <option key={r} value={r}>{r}</option>
              ))}
            </select>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-pivot-textSecondary mb-1">
                Expected Date Start <span className="text-pivot-textMuted font-normal">(optional)</span>
              </label>
              <input
                type="date"
                value={form.dateStart}
                onChange={(e) => setForm((f) => ({ ...f, dateStart: e.target.value }))}
                className="w-full border border-slate-600 bg-slate-800 text-slate-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-pivot-teal"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-pivot-textSecondary mb-1">
                Expected Date End <span className="text-pivot-textMuted font-normal">(optional)</span>
              </label>
              <input
                type="date"
                value={form.dateEnd}
                onChange={(e) => setForm((f) => ({ ...f, dateEnd: e.target.value }))}
                className="w-full border border-slate-600 bg-slate-800 text-slate-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-pivot-teal"
              />
            </div>
          </div>

          {error && (
            <div className="bg-red-900/30 border border-red-800 rounded-lg p-3 text-red-300 text-sm">
              {error}
            </div>
          )}

          <button
            type="submit"
            disabled={submitting}
            className="w-full bg-pivot-blue text-white py-2.5 rounded-lg font-medium hover:bg-blue-900 transition-colors disabled:opacity-50"
          >
            {submitting ? 'Starting…' : 'Run All Phases'}
          </button>
        </form>
      )}

      {jobId && (
        <div className="bg-pivot-surface rounded-xl shadow-sm border border-pivot-border p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="font-semibold text-pivot-textPrimary">
              Running: {form.client} / {form.round}
            </h2>
            <span className="text-xs text-pivot-textMuted">Job {jobId.slice(0, 8)}</span>
          </div>
          <PhaseProgressLog jobId={jobId} onComplete={handleComplete} />
          <p className="text-xs text-pivot-textMuted mt-3 text-center">
            You'll be redirected to the results when complete…
          </p>
        </div>
      )}
    </div>
  )
}
