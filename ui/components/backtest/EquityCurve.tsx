"use client";

import type { EquityPoint } from "@/lib/backtest-api";

import { fmtNumber } from "./format";

function linePath(points: EquityPoint[], width: number, height: number, min: number, max: number): string {
  if (points.length === 0) {
    return "";
  }
  const span = max - min || 1;
  return points
    .map((point, idx) => {
      const x = (idx / Math.max(points.length - 1, 1)) * width;
      const y = height - ((point.equity - min) / span) * height;
      return `${idx === 0 ? "M" : "L"}${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");
}

export function EquityCurve({ points }: { points: EquityPoint[] }) {
  const width = 920;
  const height = 260;
  if (points.length < 2) {
    return <div className="text-xs text-slate-500">Not enough data for equity curve.</div>;
  }
  const values = points.map((p) => p.equity);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const path = linePath(points, width, height, min, max);
  const ticks = Array.from({ length: 5 }, (_, i) => {
    const ratio = i / 4;
    return {
      y: ratio * height,
      value: max - ratio * (max - min),
    };
  });
  const start = points[0];
  const end = points[points.length - 1];

  return (
    <div className="grid-panel p-4">
      <div className="mb-3 flex items-center justify-between">
        <div className="text-xs uppercase tracking-[0.14em] text-terminal-cyan">Equity Curve</div>
        <div className="numeric text-[11px] text-slate-400">
          {start.date} {"->"} {end.date}
        </div>
      </div>
      <div className="flex gap-3">
        <div className="numeric flex w-20 flex-col justify-between text-xs text-slate-500">
          {ticks.map((tick) => (
            <span key={tick.y}>{fmtNumber(tick.value, 0)}</span>
          ))}
        </div>
        <div className="relative flex-1">
          <svg viewBox={`0 0 ${width} ${height}`} className="h-[260px] w-full">
            {ticks.map((tick) => (
              <line
                key={`grid-${tick.y}`}
                x1={0}
                x2={width}
                y1={tick.y}
                y2={tick.y}
                stroke="rgba(104, 128, 156, 0.16)"
                strokeWidth={1}
              />
            ))}
            <path
              d={path}
              fill="none"
              stroke="rgba(92, 209, 255, 1)"
              strokeWidth={2.4}
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        </div>
      </div>
    </div>
  );
}
