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
      <h1 className="text-2xl font-bold text-gray-900 mb-1">Run Validation</h1>
      <p className="text-sm text-gray-500 mb-6">
        Make sure client files are placed in{' '}
        <code className="bg-gray-100 px-1 rounded text-xs">input/ClientName/source_type/</code>{' '}
        before running.
      </p>

      {!jobId && (
        <form onSubmit={handleSubmit} className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Client Name <span className="text-red-500">*</span>
            </label>
            <input
              type="text"
              placeholder="e.g. Franciscan"
              value={form.client}
              onChange={(e) => setForm((f) => ({ ...f, client: e.target.value }))}
              className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-pivot-teal"
            />
            <p className="text-xs text-gray-400 mt-1">
              Must match the folder name inside <code className="bg-gray-100 px-1 rounded">input/</code>
            </p>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Round <span className="text-red-500">*</span>
            </label>
            <select
              value={form.round}
              onChange={(e) => setForm((f) => ({ ...f, round: e.target.value }))}
              className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-pivot-teal"
            >
              {['v1', 'v2', 'v3', 'v4', 'v5'].map((r) => (
                <option key={r} value={r}>{r}</option>
              ))}
            </select>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Expected Date Start <span className="text-gray-400 font-normal">(optional)</span>
              </label>
              <input
                type="date"
                value={form.dateStart}
                onChange={(e) => setForm((f) => ({ ...f, dateStart: e.target.value }))}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-pivot-teal"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Expected Date End <span className="text-gray-400 font-normal">(optional)</span>
              </label>
              <input
                type="date"
                value={form.dateEnd}
                onChange={(e) => setForm((f) => ({ ...f, dateEnd: e.target.value }))}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-pivot-teal"
              />
            </div>
          </div>

          {error && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-3 text-red-700 text-sm">
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
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="font-semibold text-gray-800">
              Running: {form.client} / {form.round}
            </h2>
            <span className="text-xs text-gray-400">Job {jobId.slice(0, 8)}</span>
          </div>
          <PhaseProgressLog jobId={jobId} onComplete={handleComplete} />
          <p className="text-xs text-gray-400 mt-3 text-center">
            You'll be redirected to the results when complete…
          </p>
        </div>
      )}
    </div>
  )
}
