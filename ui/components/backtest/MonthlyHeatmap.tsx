"use client";

import { cls, fmtPercent } from "./format";

type MonthlyRow = Record<string, number | null>;

function cellTone(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "bg-slate-950 text-slate-600";
  }
  if (value >= 0) {
    if (value > 8) return "bg-emerald-500/45 text-emerald-100";
    if (value > 3) return "bg-emerald-500/30 text-emerald-200";
    return "bg-emerald-500/15 text-emerald-200";
  }
  if (value < -8) return "bg-rose-500/45 text-rose-100";
  if (value < -3) return "bg-rose-500/30 text-rose-200";
  return "bg-rose-500/15 text-rose-200";
}

export function MonthlyHeatmap({ rows }: { rows: MonthlyRow[] }) {
  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  return (
    <div className="grid-panel overflow-auto p-4">
      <div className="mb-3 text-xs uppercase tracking-[0.14em] text-terminal-cyan">Monthly P&L Grid</div>
      {!rows.length ? <div className="text-xs text-slate-500">No monthly returns available.</div> : null}
      {rows.length ? (
        <table className="w-full min-w-[860px] text-xs">
          <thead>
            <tr className="border-b border-terminal-border text-left uppercase tracking-[0.12em] text-slate-400">
              <th className="px-2 py-2">Year</th>
              {months.map((m) => (
                <th key={m} className="px-2 py-2 numeric">
                  {m}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, idx) => (
              <tr key={`m-row-${idx}`} className="border-b border-slate-900/80">
                <td className="numeric px-2 py-2 text-slate-300">{String(row.year ?? "--")}</td>
                {months.map((_, monthIdx) => {
                  const key = `m${String(monthIdx + 1).padStart(2, "0")}`;
                  const value = row[key];
                  return (
                    <td key={`m-cell-${idx}-${key}`} className="px-1 py-1">
                      <div className={cls("numeric rounded px-2 py-1 text-center", cellTone(value))}>{fmtPercent(value, 1)}</div>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      ) : null}
    </div>
  );
}
