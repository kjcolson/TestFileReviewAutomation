/**
 * SqlPreview.jsx
 *
 * Tabbed SQL/XML preview panel.  Each tab shows the content of one generated file
 * with a Copy and Download button.
 */

import { useState, useEffect } from 'react'
import { api } from '../lib/api.js'

function basename(path) {
  return path.replace(/\\/g, '/').split('/').pop()
}

function TabButton({ label, active, onClick }) {
  return (
    <button
      onClick={onClick}
      className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors whitespace-nowrap ${
        active
          ? 'border-pivot-teal text-pivot-teal'
          : 'border-transparent text-pivot-textMuted hover:text-pivot-textSecondary hover:border-slate-600'
      }`}
    >
      {label}
    </button>
  )
}

function FileTab({ client, filePath, downloadUrl }) {
  const [content, setContent] = useState(null)
  const [loading, setLoading] = useState(true)
  const [copied, setCopied] = useState(false)
  const filename = basename(filePath)

  useEffect(() => {
    setLoading(true)
    api
      .previewSqlFile(client, filename)
      .then((text) => setContent(text))
      .catch((err) => setContent(`-- Error loading preview: ${err.message}`))
      .finally(() => setLoading(false))
  }, [client, filename])

  async function handleCopy() {
    if (!content) return
    await navigator.clipboard.writeText(content)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center justify-between">
        <span className="text-xs text-pivot-textMuted font-mono">{filename}</span>
        <div className="flex gap-2">
          <button
            onClick={handleCopy}
            disabled={!content}
            className="text-xs px-3 py-1 bg-slate-700 hover:bg-slate-600 border border-slate-600 text-slate-200 rounded transition-colors disabled:opacity-40"
          >
            {copied ? '\u2713 Copied' : 'Copy'}
          </button>
          <a
            href={downloadUrl}
            download={filename}
            className="text-xs px-3 py-1 bg-pivot-blue text-white hover:bg-blue-700 border border-pivot-blue rounded transition-colors"
          >
            Download
          </a>
        </div>
      </div>

      {loading ? (
        <div className="text-center py-8 text-pivot-textMuted text-sm">Loading…</div>
      ) : (
        <div className="bg-gray-950 rounded-lg p-4 overflow-auto max-h-[60vh] font-mono text-xs leading-relaxed">
          {content && content.split('\n').map((line, i) => (
            <div key={i} className="flex hover:bg-gray-900">
              <span className="select-none text-gray-600 w-10 text-right pr-3 shrink-0">{i + 1}</span>
              <span className="text-green-300 whitespace-pre">{line}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export default function SqlPreview({ client, result }) {
  const [activeTab, setActiveTab] = useState(0)

  const tabs = [
    { label: 'Config SQL', path: result.config_sql_path },
    ...result.sproc_paths.map((p) => ({ label: basename(p).replace('.sql', ''), path: p })),
    { label: 'Liquibase XML', path: result.liquibase_xml_path },
  ]

  return (
    <div className="bg-pivot-surface border border-pivot-border rounded-xl shadow-sm p-5 space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-base font-bold text-pivot-textPrimary">Generated Files</h2>
        <button
          onClick={() => {
            tabs.forEach((t, i) => {
              setTimeout(() => {
                const a = document.createElement('a')
                a.href = api.downloadSqlFile(client, basename(t.path))
                a.download = basename(t.path)
                document.body.appendChild(a)
                a.click()
                document.body.removeChild(a)
              }, i * 200)
            })
          }}
          className="text-xs px-3 py-1.5 bg-pivot-blue text-white hover:bg-blue-700 rounded transition-colors font-medium"
        >
          Download All
        </button>
      </div>

      {result.warnings?.length > 0 && (
        <div className="bg-yellow-900/30 border border-yellow-700 rounded-lg p-3 text-sm text-yellow-300 space-y-1">
          <p className="font-semibold">Warnings</p>
          <ul className="list-disc list-inside space-y-0.5">
            {result.warnings.map((w, i) => (
              <li key={i}>{w}</li>
            ))}
          </ul>
        </div>
      )}

      {/* Tabs */}
      <div className="border-b border-slate-700 flex gap-1 overflow-x-auto">
        {tabs.map((t, i) => (
          <TabButton
            key={i}
            label={t.label}
            active={activeTab === i}
            onClick={() => setActiveTab(i)}
          />
        ))}
      </div>

      {/* Active tab content */}
      <FileTab
        key={tabs[activeTab]?.path}
        client={client}
        filePath={tabs[activeTab]?.path || ''}
        downloadUrl={api.downloadSqlFile(client, basename(tabs[activeTab]?.path || ''))}
      />
    </div>
  )
}
