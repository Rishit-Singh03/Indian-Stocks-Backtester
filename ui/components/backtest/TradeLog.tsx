"use client";

import type { BacktestTrade } from "@/lib/backtest-api";

import { cls, fmtNumber, fmtPercent } from "./format";

export function TradeLog({
  trades,
  total,
  pageSize,
  offset,
  onPrev,
  onNext,
}: {
  trades: BacktestTrade[];
  total: number;
  pageSize: number;
  offset: number;
  onPrev: () => void;
  onNext: () => void;
}) {
  return (
    <div className="grid-panel overflow-auto p-4">
      <div className="mb-3 flex items-center justify-between">
        <div className="text-xs uppercase tracking-[0.14em] text-terminal-cyan">Trade Log</div>
        <div className="numeric flex items-center gap-2 text-[11px] text-slate-400">
          <button onClick={onPrev} disabled={offset <= 0} className="rounded border border-terminal-border px-2 py-1 disabled:opacity-40">
            Prev
          </button>
          <button
            onClick={onNext}
            disabled={offset + pageSize >= total}
            className="rounded border border-terminal-border px-2 py-1 disabled:opacity-40"
          >
            Next
          </button>
          <span>
            {Math.min(offset + 1, Math.max(1, total))}-{Math.min(offset + pageSize, total)} / {total}
          </span>
        </div>
      </div>
      {!trades.length ? <div className="text-xs text-slate-500">No trades in current page.</div> : null}
      {trades.length ? (
        <table className="w-full min-w-[920px] text-xs">
          <thead>
            <tr className="border-b border-terminal-border text-left uppercase tracking-[0.12em] text-slate-400">
              <th className="px-2 py-2">Symbol</th>
              <th className="px-2 py-2">Entry</th>
              <th className="px-2 py-2">Exit</th>
              <th className="px-2 py-2">Shares</th>
              <th className="px-2 py-2">Entry Px</th>
              <th className="px-2 py-2">Exit Px</th>
              <th className="px-2 py-2">P&L</th>
              <th className="px-2 py-2">P&L %</th>
              <th className="px-2 py-2">Reason</th>
            </tr>
          </thead>
          <tbody>
            {trades.map((trade, idx) => (
              <tr key={`trade-${trade.trade_index ?? idx}-${trade.symbol}-${trade.entry_date}`} className="border-b border-slate-900/80">
                <td className="numeric px-2 py-2 text-slate-200">{trade.symbol}</td>
                <td className="numeric px-2 py-2">{trade.entry_date}</td>
                <td className="numeric px-2 py-2">{trade.exit_date}</td>
                <td className="numeric px-2 py-2">{fmtNumber(trade.shares, 0)}</td>
                <td className="numeric px-2 py-2">{fmtNumber(trade.entry_price)}</td>
                <td className="numeric px-2 py-2">{fmtNumber(trade.exit_price)}</td>
                <td className={cls("numeric px-2 py-2", trade.pnl >= 0 ? "text-terminal-green" : "text-terminal-red")}>
                  {fmtNumber(trade.pnl)}
                </td>
                <td className={cls("numeric px-2 py-2", trade.pnl_pct >= 0 ? "text-terminal-green" : "text-terminal-red")}>
                  {fmtPercent(trade.pnl_pct)}
                </td>
                <td className="numeric px-2 py-2 text-[11px] text-slate-400">{trade.exit_reason}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : null}
    </div>
  );
}
