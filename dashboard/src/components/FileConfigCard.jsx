/**
 * FileConfigCard.jsx
 *
 * Displays an editable configuration card for one source file.
 * Used on the GenerateSQL page — one card per source type detected in Phase 1.
 */

const SOURCE_LABELS = {
  billing_combined: 'Billing — Combined',
  billing_charges: 'Billing — Charges',
  billing_transactions: 'Billing — Transactions',
  payroll: 'Payroll',
  gl: 'General Ledger',
  scheduling: 'Scheduling',
}

function Field({ label, name, value, onChange, type = 'text', hint }) {
  return (
    <div>
      <label className="block text-xs font-medium text-pivot-textSecondary mb-1">{label}</label>
      <input
        type={type}
        name={name}
        value={value}
        onChange={onChange}
        className="w-full text-sm border border-slate-600 bg-slate-800 text-slate-200 rounded px-2 py-1.5 focus:outline-none focus:ring-2 focus:ring-pivot-blue"
      />
      {hint && <p className="text-xs text-pivot-textMuted mt-0.5">{hint}</p>}
    </div>
  )
}

function Toggle({ label, name, checked, onChange }) {
  return (
    <label className="flex items-center gap-2 cursor-pointer text-sm text-pivot-textSecondary">
      <input
        type="checkbox"
        name={name}
        checked={checked}
        onChange={onChange}
        className="w-4 h-4 accent-pivot-blue"
      />
      {label}
    </label>
  )
}

export default function FileConfigCard({ file, index, onChange }) {
  const sourceLabel = SOURCE_LABELS[file.source] || file.source
  const nn = String(file.ds_number).padStart(2, '0')

  function handleChange(e) {
    const { name, value, type, checked } = e.target
    onChange(index, { ...file, [name]: type === 'checkbox' ? checked : value })
  }

  function handleDsNumber(e) {
    const val = parseInt(e.target.value, 10)
    if (!isNaN(val) && val > 0) {
      onChange(index, { ...file, ds_number: val })
    }
  }

  const included = file.include !== false

  function handleInclude() {
    onChange(index, { ...file, include: !included })
  }

  return (
    <div className={`bg-pivot-surface border rounded-xl shadow-sm p-5 space-y-4 transition-opacity ${included ? 'border-pivot-border' : 'border-slate-700 opacity-50'}`}>
      {/* Card header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <button
            type="button"
            onClick={handleInclude}
            className={`w-5 h-5 rounded border-2 flex items-center justify-center flex-shrink-0 transition-colors ${
              included ? 'bg-pivot-blue border-pivot-blue text-white' : 'border-slate-600 bg-slate-800'
            }`}
            title={included ? 'Click to exclude from generation' : 'Click to include in generation'}
          >
            {included && <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" /></svg>}
          </button>
          <div>
            <span className="text-xs font-semibold text-pivot-textMuted uppercase tracking-wide">
              Data Source {nn}
            </span>
            <h3 className="text-base font-bold text-pivot-textPrimary mt-0.5">{sourceLabel}</h3>
          </div>
        </div>
        <span className="text-xs bg-slate-700 text-slate-300 px-2 py-1 rounded font-mono">
          cst.DataSource{nn}_{file.source_name}_Load
        </span>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        <div>
          <label className="block text-xs font-medium text-pivot-textSecondary mb-1">Data Source #</label>
          <input
            type="number"
            min="1"
            max="99"
            value={file.ds_number}
            onChange={handleDsNumber}
            className="w-20 text-sm border border-slate-600 bg-slate-800 text-slate-200 rounded px-2 py-1.5 focus:outline-none focus:ring-2 focus:ring-pivot-blue"
          />
        </div>
        <Field
          label="Source Name"
          name="source_name"
          value={file.source_name}
          onChange={handleChange}
          hint='Used in sproc name (e.g. "Payroll")'
        />
        <Field
          label="File Name Pattern"
          name="file_name_pattern"
          value={file.file_name_pattern}
          onChange={handleChange}
          hint='SQL LIKE pattern (e.g. "%Payroll%")'
        />
        <Field
          label="Row Terminator"
          name="row_terminator"
          value={file.row_terminator}
          onChange={handleChange}
          hint='Default: 0x0a (LF). Use \r\n for CRLF.'
        />
      </div>

      <Field
        label="SFTP Folder (To Load)"
        name="sftp_folder"
        value={file.sftp_folder}
        onChange={handleChange}
        hint="Full UNC path to the client's To Load folder"
      />
      <Field
        label="SFTP Folder (Loaded)"
        name="loaded_folder"
        value={file.loaded_folder}
        onChange={handleChange}
      />

      <div className="flex gap-6">
        <Toggle
          label="Automated Load"
          name="automated_load"
          checked={file.automated_load}
          onChange={handleChange}
        />
        <Toggle
          label="Daily Load"
          name="daily_load"
          checked={file.daily_load}
          onChange={handleChange}
        />
      </div>
    </div>
  )
}
