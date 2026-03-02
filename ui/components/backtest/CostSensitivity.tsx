"use client";

import { asNumber, cls, fmtNumber, fmtPercent } from "./format";

type CostRow = Record<string, number>;

export function CostSensitivity({ rows }: { rows: CostRow[] }) {
  const values = rows.map((row) => asNumber(row.estimated_total_return_pct, 0));
  const maxAbs = Math.max(1, ...values.map((v) => Math.abs(v)));

  return (
    <div className="grid-panel p-4">
      <div className="mb-3 text-xs uppercase tracking-[0.14em] text-terminal-cyan">Cost Sensitivity (0x / 1x / 2x)</div>
      {!rows.length ? <div className="text-xs text-slate-500">No cost sensitivity data.</div> : null}
      <div className="space-y-3">
        {rows.map((row, idx) => {
          const mult = asNumber(row.cost_multiplier, idx);
          const ret = asNumber(row.estimated_total_return_pct, 0);
          const finalEq = asNumber(row.estimated_final_equity, 0);
          const width = `${Math.max(4, (Math.abs(ret) / maxAbs) * 100)}%`;
          return (
            <div key={`cost-${idx}`}>
              <div className="mb-1 flex items-center justify-between text-xs">
                <span className="numeric text-slate-300">{mult.toFixed(1)}x costs</span>
                <span className={cls("numeric", ret >= 0 ? "text-terminal-green" : "text-terminal-red")}>{fmtPercent(ret)}</span>
              </div>
              <div className="h-3 w-full rounded bg-slate-900">
                <div
                  className={cls("h-3 rounded", ret >= 0 ? "bg-emerald-400/80" : "bg-rose-400/80")}
                  style={{ width }}
                />
              </div>
              <div className="numeric mt-1 text-[11px] text-slate-500">Estimated final equity: {fmtNumber(finalEq, 0)}</div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
