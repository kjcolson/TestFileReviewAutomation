import { useState, useMemo } from 'react'
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  getFilteredRowModel,
  flexRender,
  createColumnHelper,
} from '@tanstack/react-table'

const SEV_STYLE = {
  CRITICAL: 'bg-red-50',
  HIGH:     'bg-orange-50',
  MEDIUM:   'bg-yellow-50',
  LOW:      'bg-blue-50',
  INFO:     'bg-gray-50',
}
const SEV_BADGE = {
  CRITICAL: 'bg-red-100 text-red-800',
  HIGH:     'bg-orange-100 text-orange-800',
  MEDIUM:   'bg-yellow-100 text-yellow-800',
  LOW:      'bg-blue-100 text-blue-800',
  INFO:     'bg-gray-100 text-gray-600',
}

const col = createColumnHelper()

const COLUMNS = [
  col.accessor('id', {
    header: 'ID',
    size: 80,
    cell: (i) => <span className="font-mono text-xs text-gray-500">{i.getValue()}</span>,
  }),
  col.accessor('severity', {
    header: 'Severity',
    size: 100,
    cell: (i) => {
      const v = i.getValue()
      return (
        <span className={`text-xs font-semibold px-2 py-0.5 rounded-full ${SEV_BADGE[v] || 'bg-gray-100 text-gray-700'}`}>
          {v}
        </span>
      )
    },
  }),
  col.accessor('source_display', { header: 'Source', size: 120 }),
  col.accessor('description', {
    header: 'Description',
    cell: (i) => <span className="text-sm">{i.getValue()}</span>,
  }),
  col.accessor('priority', {
    header: 'Priority',
    size: 120,
    cell: (i) => {
      const v = i.getValue()
      const style = v === 'MUST FIX'
        ? 'text-red-700 font-semibold'
        : v === 'SHOULD FIX'
        ? 'text-orange-600 font-medium'
        : 'text-gray-500'
      return <span className={`text-xs ${style}`}>{v}</span>
    },
  }),
]

const SEV_ORDER = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'INFO']

export default function IssuesTable({ issues = [] }) {
  const [sevFilter, setSevFilter] = useState('')
  const [srcFilter, setSrcFilter] = useState('')
  const [search, setSearch] = useState('')
  const [sorting, setSorting] = useState([{ id: 'severity', desc: false }])

  const sources = useMemo(() => {
    const s = new Set(issues.map((i) => i.source_display).filter(Boolean))
    return [...s].sort()
  }, [issues])

  const filtered = useMemo(() => {
    let rows = issues
    if (sevFilter) rows = rows.filter((r) => r.severity === sevFilter)
    if (srcFilter) rows = rows.filter((r) => r.source_display === srcFilter)
    if (search) {
      const q = search.toLowerCase()
      rows = rows.filter(
        (r) =>
          r.description?.toLowerCase().includes(q) ||
          r.id?.toLowerCase().includes(q) ||
          r.source_display?.toLowerCase().includes(q)
      )
    }
    // Sort by severity order
    return [...rows].sort((a, b) => {
      const ai = SEV_ORDER.indexOf(a.severity)
      const bi = SEV_ORDER.indexOf(b.severity)
      return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi)
    })
  }, [issues, sevFilter, srcFilter, search])

  const table = useReactTable({
    data: filtered,
    columns: COLUMNS,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
  })

  return (
    <div>
      {/* Filters */}
      <div className="flex flex-wrap gap-3 mb-3">
        <select
          value={sevFilter}
          onChange={(e) => setSevFilter(e.target.value)}
          className="text-sm border border-gray-300 rounded px-2 py-1"
        >
          <option value="">All Severities</option>
          {SEV_ORDER.map((s) => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>
        <select
          value={srcFilter}
          onChange={(e) => setSrcFilter(e.target.value)}
          className="text-sm border border-gray-300 rounded px-2 py-1"
        >
          <option value="">All Sources</option>
          {sources.map((s) => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>
        <input
          type="text"
          placeholder="Search descriptions…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="text-sm border border-gray-300 rounded px-3 py-1 flex-1 min-w-48"
        />
        <span className="text-xs text-gray-500 self-center">
          {filtered.length} of {issues.length} issues
        </span>
      </div>

      {/* Table */}
      <div className="overflow-x-auto rounded border border-gray-200">
        <table className="w-full text-sm border-collapse">
          <thead>
            {table.getHeaderGroups().map((hg) => (
              <tr key={hg.id} className="bg-gray-100">
                {hg.headers.map((h) => (
                  <th
                    key={h.id}
                    className="px-3 py-2 text-left font-semibold text-gray-700 cursor-pointer select-none"
                    style={{ width: h.column.columnDef.size }}
                    onClick={h.column.getToggleSortingHandler()}
                  >
                    {flexRender(h.column.columnDef.header, h.getContext())}
                    {h.column.getIsSorted() === 'asc' ? ' ↑' : h.column.getIsSorted() === 'desc' ? ' ↓' : ''}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.length === 0 && (
              <tr>
                <td colSpan={COLUMNS.length} className="px-3 py-6 text-center text-gray-400 italic">
                  No issues match the current filters.
                </td>
              </tr>
            )}
            {table.getRowModel().rows.map((row) => {
              const sev = row.original.severity
              return (
                <tr key={row.id} className={`border-t border-gray-200 hover:opacity-90 ${SEV_STYLE[sev] || ''}`}>
                  {row.getVisibleCells().map((cell) => (
                    <td key={cell.id} className="px-3 py-2">
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </td>
                  ))}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
