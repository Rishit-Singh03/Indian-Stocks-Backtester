"use client";

import { asNumber, cls, fmtNumber, fmtPercent } from "./format";

type ResultPayload = {
  summary?: Record<string, unknown>;
  ratios?: Record<string, unknown>;
  risk?: Record<string, unknown>;
  trade_stats?: Record<string, unknown>;
};

export function MetricCards({ result }: { result: ResultPayload | null }) {
  const summary = result?.summary ?? {};
  const ratios = result?.ratios ?? {};
  const risk = result?.risk ?? {};
  const tradeStats = result?.trade_stats ?? {};

  const cards = [
    { label: "CAGR", value: fmtPercent(asNumber(summary.cagr_pct, NaN)), tone: asNumber(summary.cagr_pct, 0) >= 0 },
    { label: "Sharpe", value: fmtNumber(asNumber(ratios.sharpe, NaN), 3), tone: asNumber(ratios.sharpe, 0) >= 0 },
    {
      label: "Max Drawdown",
      value: fmtPercent(-Math.abs(asNumber(risk.max_drawdown_pct, NaN))),
      tone: false,
    },
    { label: "Win Rate", value: fmtPercent(asNumber(tradeStats.win_rate_pct, NaN)), tone: asNumber(tradeStats.win_rate_pct, 0) >= 50 },
    { label: "Trades", value: fmtNumber(asNumber(tradeStats.total_trades, NaN), 0), tone: true },
    { label: "Profit Factor", value: fmtNumber(asNumber(tradeStats.profit_factor, NaN), 3), tone: asNumber(tradeStats.profit_factor, 0) >= 1 },
  ];

  return (
    <section className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6">
      {cards.map((card) => (
        <article key={card.label} className="grid-panel p-3">
          <div className="text-[10px] uppercase tracking-[0.12em] text-slate-500">{card.label}</div>
          <div className={cls("numeric mt-2 text-xl", card.tone ? "text-terminal-green" : "text-terminal-red")}>{card.value}</div>
        </article>
      ))}
    </section>
  );
}
