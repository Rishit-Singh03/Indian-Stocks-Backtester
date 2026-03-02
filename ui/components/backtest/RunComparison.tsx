"use client";

import type { BacktestCompareRun, BacktestHistoryRow } from "@/lib/backtest-api";

import { cls, fmtNumber, fmtPercent } from "./format";

export function RunComparison({
  history,
  selected,
  onToggle,
  onCompare,
  comparing,
  rows,
}: {
  history: BacktestHistoryRow[];
  selected: string[];
  onToggle: (runId: string) => void;
  onCompare: () => void;
  comparing: boolean;
  rows: BacktestCompareRun[];
}) {
  return (
    <section className="grid-panel p-4">
      <div className="mb-3 flex items-center justify-between">
        <div className="text-xs uppercase tracking-[0.14em] text-terminal-cyan">Run Comparison</div>
        <button
          onClick={onCompare}
          disabled={selected.length < 2 || comparing}
          className="numeric rounded border border-terminal-cyan px-2 py-1 text-xs text-terminal-cyan disabled:opacity-40"
        >
          {comparing ? "Comparing..." : "Compare Selected"}
        </button>
      </div>
      <div className="mb-3 text-[11px] text-slate-500">Select 2-4 runs from history.</div>
      <div className="mb-4 grid grid-cols-1 gap-2 md:grid-cols-2">
        {history.slice(0, 12).map((run) => {
          const isSelected = selected.includes(run.run_id);
          return (
            <label
              key={`cmp-pick-${run.run_id}`}
              className={cls(
                "flex cursor-pointer items-center justify-between rounded border px-2 py-2 text-xs",
                isSelected ? "border-terminal-cyan bg-terminal-cyan/10" : "border-terminal-border bg-black/40",
              )}
            >
              <span className="numeric truncate pr-2">{run.run_id}</span>
              <input type="checkbox" checked={isSelected} onChange={() => onToggle(run.run_id)} />
            </label>
          );
        })}
      </div>

      {!rows.length ? <div className="text-xs text-slate-500">No comparison results yet.</div> : null}
      {rows.length ? (
        <table className="w-full min-w-[760px] text-xs">
          <thead>
            <tr className="border-b border-terminal-border text-left uppercase tracking-[0.12em] text-slate-400">
              <th className="px-2 py-2">Run</th>
              <th className="px-2 py-2">Status</th>
              <th className="px-2 py-2">Trades</th>
              <th className="px-2 py-2">Total Return</th>
              <th className="px-2 py-2">Sharpe</th>
              <th className="px-2 py-2">Max DD</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((run) => (
              <tr key={`cmp-row-${run.run_id}`} className="border-b border-slate-900/80">
                <td className="numeric px-2 py-2">{run.run_id}</td>
                <td className="numeric px-2 py-2">{run.status}</td>
                <td className="numeric px-2 py-2">{fmtNumber(run.trade_count, 0)}</td>
                <td className={cls("numeric px-2 py-2", run.total_return >= 0 ? "text-terminal-green" : "text-terminal-red")}>
                  {fmtPercent(run.total_return)}
                </td>
                <td className="numeric px-2 py-2">{fmtNumber(run.sharpe, 3)}</td>
                <td className="numeric px-2 py-2 text-terminal-red">{fmtPercent(-Math.abs(run.max_drawdown))}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : null}
    </section>
  );
}
