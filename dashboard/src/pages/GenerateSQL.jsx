/**
 * GenerateSQL.jsx
 *
 * Full "Generate SQL" page for a client.
 *
 * Flow:
 *   1. Load defaults from GET /api/sqlgen/defaults/{client}
 *   2. User reviews/edits the form (client ID, raw DB name, per-file settings)
 *   3. Click "Generate" → POST /api/sqlgen/generate
 *   4. Show SqlPreview with tabbed output files
 */

import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { api } from '../lib/api.js'
import FileConfigCard from '../components/FileConfigCard.jsx'
import SqlPreview from '../components/SqlPreview.jsx'
import Spinner from '../components/Spinner.jsx'
import Breadcrumb from '../components/Breadcrumb.jsx'

function Field({ label, value, onChange, hint, monospace = false }) {
  return (
    <div>
      <label className="block text-sm font-medium text-pivot-textSecondary mb-1">{label}</label>
      <input
        type="text"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className={`w-full border border-slate-600 bg-slate-800 text-slate-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-pivot-blue ${
          monospace ? 'font-mono' : ''
        }`}
      />
      {hint && <p className="text-xs text-pivot-textMuted mt-0.5">{hint}</p>}
    </div>
  )
}

export default function GenerateSQL() {
  const { client } = useParams()

  // Editable form state
  const [clientId, setClientId] = useState('')
  const [clientName, setClientName] = useState('')
  const [rawDatabase, setRawDatabase] = useState('')
  const [files, setFiles] = useState([])

  // Client params collapsible — auto-expand when Client ID is empty
  const [paramsOpen, setParamsOpen] = useState(false)

  // Generation result
  const [result, setResult] = useState(null)
  const [generating, setGenerating] = useState(false)
  const [generateError, setGenerateError] = useState(null)

  // Load defaults from API
  const { data: defaults, isLoading, error: loadError } = useQuery({
    queryKey: ['sqlgen-defaults', client],
    queryFn: () => api.getSqlgenDefaults(client),
  })

  // Populate form once defaults arrive (onSuccess was removed in React Query v5)
  useEffect(() => {
    if (!defaults) return
    setClientName(defaults.client_name || client)
    setRawDatabase(defaults.raw_database || '')
    setFiles((defaults.files || []).map(f => ({ ...f, include: true })))
  }, [defaults])

  function handleFileChange(index, updated) {
    setFiles((prev) => prev.map((f, i) => (i === index ? updated : f)))
  }

  // Auto-update raw_database when clientId or clientName changes
  function handleClientId(val) {
    setClientId(val)
    if (clientName) {
      setRawDatabase(`${val}_${clientName}_Raw`)
    }
  }

  function handleClientName(val) {
    setClientName(val)
    if (clientId) {
      setRawDatabase(`${clientId}_${val}_Raw`)
    }
  }

  async function handleGenerate() {
    if (!clientId.trim()) {
      setGenerateError('Client ID is required (4-digit, e.g. 0073)')
      return
    }
    const activeFiles = files.filter(f => f.include !== false)
    if (activeFiles.length === 0) {
      setGenerateError('Select at least one source file to include in generation.')
      return
    }
    setGenerating(true)
    setGenerateError(null)
    setResult(null)
    try {
      const res = await api.generateSql({
        client,
        client_id: clientId,
        client_name: clientName,
        raw_database: rawDatabase,
        files: activeFiles,
      })
      setResult(res)
      setTimeout(() => {
        document.getElementById('sql-preview')?.scrollIntoView({ behavior: 'smooth' })
      }, 100)
    } catch (err) {
      setGenerateError(err.message)
    } finally {
      setGenerating(false)
    }
  }

  if (isLoading) {
    return <Spinner label="Loading Phase 1 defaults…" />
  }

  if (loadError) {
    const noPhase1 = loadError.message.includes('phase1') || loadError.message.includes('not found') || loadError.message.includes('404')
    if (noPhase1) {
      return (
        <div className="text-center py-16">
          <p className="text-pivot-textSecondary text-lg mb-2">No Phase 1 data available yet.</p>
          <p className="text-sm text-pivot-textMuted mb-4">Run the validation pipeline first to generate column mappings.</p>
          <Link
            to="/run"
            className="bg-pivot-blue text-white px-6 py-2.5 rounded-lg text-sm font-medium hover:bg-blue-700 transition-colors"
          >
            Run Validation
          </Link>
        </div>
      )
    }
    return (
      <div className="bg-red-900/30 border border-red-800 rounded-lg p-4 text-red-300 text-sm">
        {loadError.message}
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <Breadcrumb items={[
        { label: 'Dashboard', to: '/' },
        { label: client, to: `/client/${client}` },
        { label: 'Generate SQL' },
      ]} />

      {/* Page header */}
      <div className="bg-pivot-surface border border-pivot-border rounded-xl shadow-sm p-6">
        <h1 className="text-xl font-bold text-pivot-textPrimary mb-1">Generate SQL Scripts</h1>
        <p className="text-sm text-pivot-textSecondary">
          Review the auto-filled parameters below, then click Generate to produce the config SQL,
          load stored procedure(s), and Liquibase XML.
        </p>
      </div>

      {/* Global parameters — collapsed by default */}
      <div className="bg-pivot-surface border border-pivot-border rounded-xl shadow-sm p-6">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <h2 className="text-base font-bold text-pivot-textPrimary">Client Parameters</h2>
            {!paramsOpen && (
              <span className="text-sm text-pivot-textMuted font-mono">
                {clientId || '????'} &middot; {clientName || '—'} &middot; {rawDatabase || '—'}
              </span>
            )}
          </div>
          <button
            type="button"
            onClick={() => setParamsOpen((o) => !o)}
            className="text-xs text-pivot-teal hover:text-teal-400 font-medium"
          >
            {paramsOpen ? 'Collapse' : 'Edit'}
          </button>
        </div>
        {paramsOpen && (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 mt-4">
            <Field
              label="Client ID (4-digit)"
              value={clientId}
              onChange={handleClientId}
              hint='e.g. "0073" — used in sproc names and Liquibase context'
              monospace
            />
            <Field
              label="Client Name"
              value={clientName}
              onChange={handleClientName}
              hint='e.g. "Ardent"'
            />
            <Field
              label="Raw Database Name"
              value={rawDatabase}
              onChange={setRawDatabase}
              hint='e.g. "0073_Ardent_Raw"'
              monospace
            />
          </div>
        )}
      </div>

      {/* Per-file config cards */}
      {files.length > 0 && (
        <div className="space-y-4">
          <h2 className="text-base font-bold text-pivot-textSecondary">Source File Configuration</h2>
          {files.map((file, i) => (
            <FileConfigCard key={i} file={file} index={i} onChange={handleFileChange} />
          ))}
        </div>
      )}

      {/* Error */}
      {generateError && (
        <div className="bg-red-900/30 border border-red-800 rounded-lg p-4 text-red-300 text-sm">
          {generateError}
        </div>
      )}

      {/* Generate button */}
      <div className="flex justify-end">
        <button
          onClick={handleGenerate}
          disabled={generating}
          className="bg-pivot-blue text-white px-6 py-2.5 rounded-lg font-medium text-sm hover:bg-blue-700 disabled:opacity-60 disabled:cursor-not-allowed transition-colors"
        >
          {generating ? 'Generating…' : 'Generate SQL'}
        </button>
      </div>

      {/* Preview */}
      {result && (
        <div id="sql-preview">
          <SqlPreview client={client} result={result} />
        </div>
      )}
    </div>
  )
}
