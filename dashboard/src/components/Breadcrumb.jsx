import { Fragment } from 'react'
import { Link } from 'react-router-dom'

export default function Breadcrumb({ items }) {
  return (
    <div className="flex items-center gap-2 text-sm text-pivot-textSecondary">
      {items.map((item, i) => (
        <Fragment key={i}>
          {i > 0 && <span>/</span>}
          {item.to ? (
            <Link to={item.to} className="hover:text-pivot-teal">{item.label}</Link>
          ) : (
            <span className="text-pivot-textPrimary font-medium">{item.label}</span>
          )}
        </Fragment>
      ))}
    </div>
  )
}
