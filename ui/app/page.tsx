"use client";

import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";

import {
  CompareResponse,
  CorrelationResponse,
  IndexSnapshotRow,
  OhlcvResponse,
  SearchResult,
  SeriesPoint,
  fetchIndexSnapshot,
  fetchOhlcv,
  searchSymbols,
} from "@/lib/api";
import { TIMEFRAMES, TimeframeKey, rangeForTimeframe } from "@/lib/timeframe";

type LineSeries = {
  name: string;
  color: string;
  points: { date: string; value: number }[];
};

type Target = {
  type: "stock" | "index";
  code: string;
};

const CHART_COLORS = ["#f5a623", "#5cd1ff", "#7cff9e", "#ff6b6b", "#b18cff", "#ffd166"];
const NORMALIZED_BASE_OPTIONS = [100, 1000];

function targetKey(target: Target): string {
  return `${target.type}:${target.code}`;
}

function targetLabel(target: Target): string {
  return `${target.type === "index" ? "IDX" : "STK"}:${target.code}`;
}

function corr(x: number[], y: number[]): number | null {
  const n = x.length;
  if (n < 2 || n !== y.length) {
    return null;
  }
  const mx = x.reduce((acc, val) => acc + val, 0) / n;
  const my = y.reduce((acc, val) => acc + val, 0) / n;
  let num = 0;
  let denX = 0;
  let denY = 0;
  for (let i = 0; i < n; i += 1) {
    const dx = x[i] - mx;
    const dy = y[i] - my;
    num += dx * dy;
    denX += dx * dx;
    denY += dy * dy;
  }
  if (denX === 0 || denY === 0) {
    return null;
  }
  return num / Math.sqrt(denX * denY);
}

function cls(...parts: Array<string | false | null | undefined>): string {
  return parts.filter(Boolean).join(" ");
}

function percent(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "--";
  }
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function numberFmt(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "--";
  }
  return new Intl.NumberFormat("en-IN", { maximumFractionDigits: 2 }).format(value);
}

function firstLastChange(points: SeriesPoint[]): { abs: number | null; pct: number | null } {
  if (points.length < 2) {
    return { abs: null, pct: null };
  }
  const first = points[0].close;
  const last = points[points.length - 1].close;
  if (first === 0) {
    return { abs: null, pct: null };
  }
  return { abs: last - first, pct: ((last / first) - 1) * 100 };
}

function computeVolatility(points: SeriesPoint[], periodsPerYear: number): number | null {
  if (points.length < 3) {
    return null;
  }
  const returns: number[] = [];
  for (let i = 1; i < points.length; i += 1) {
    const prev = points[i - 1].close;
    const curr = points[i].close;
    if (prev <= 0) {
      continue;
    }
    returns.push((curr / prev) - 1);
  }
  if (returns.length < 2) {
    return null;
  }
  const mean = returns.reduce((acc, val) => acc + val, 0) / returns.length;
  const variance = returns.reduce((acc, val) => acc + (val - mean) ** 2, 0) / (returns.length - 1);
  return Math.sqrt(variance) * Math.sqrt(periodsPerYear) * 100;
}

function averageVolume(points: SeriesPoint[]): number | null {
  if (points.length === 0) {
    return null;
  }
  const total = points.reduce((acc, point) => acc + point.volume, 0);
  return total / points.length;
}

function useSearch(query: string): SearchResult[] {
  const [results, setResults] = useState<SearchResult[]>([]);

  useEffect(() => {
    if (!query.trim()) {
      setResults([]);
      return;
    }
    const timer = setTimeout(() => {
      searchSymbols(query, 50)
        .then((rows) => setResults(rows))
        .catch(() => setResults([]));
    }, 160);
    return () => clearTimeout(timer);
  }, [query]);

  return results;
}

function linePathAligned(values: Array<number | null>, width: number, height: number, min: number, max: number): string {
  if (values.length === 0) {
    return "";
  }
  const span = max - min || 1;
  let path = "";
  let started = false;
  values.forEach((value, idx) => {
    if (value === null || Number.isNaN(value)) {
      started = false;
      return;
    }
    const x = (idx / Math.max(values.length - 1, 1)) * width;
    const y = height - ((value - min) / span) * height;
    if (!started) {
      path += `M${x.toFixed(2)},${y.toFixed(2)}`;
      started = true;
    } else {
      path += ` L${x.toFixed(2)},${y.toFixed(2)}`;
    }
  });
  return path;
}

function MiniLineChart({
  series,
  valueFormatter = numberFmt,
}: {
  series: LineSeries[];
  valueFormatter?: (value: number) => string;
}) {
  const width = 920;
  const height = 280;

  const aligned = useMemo(() => {
    const dates = Array.from(new Set(series.flatMap((line) => line.points.map((point) => point.date)))).sort();
    const lines = series.map((line) => {
      const lookup = new Map(line.points.map((point) => [point.date, point.value]));
      const values = dates.map((date) => (lookup.has(date) ? lookup.get(date)! : null));
      return { ...line, values };
    });
    return { dates, lines };
  }, [series]);

  const allValues = aligned.lines.flatMap((line) => line.values.filter((value): value is number => value !== null));
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);

  if (allValues.length === 0 || aligned.dates.length === 0) {
    return (
      <div className="grid-panel flex h-[320px] items-center justify-center text-sm text-slate-400">
        No series data for this selection.
      </div>
    );
  }

  const min = Math.min(...allValues);
  const max = Math.max(...allValues);
  const span = max - min || 1;
  const grid = Array.from({ length: 5 }, (_, idx) => {
    const y = (idx / 4) * height;
    const val = max - (idx / 4) * span;
    return { y, val };
  });

  const hoverDate = hoverIndex !== null ? aligned.dates[hoverIndex] : null;
  const hoverRows =
    hoverIndex !== null
      ? aligned.lines
          .map((line) => ({ name: line.name, color: line.color, value: line.values[hoverIndex] }))
          .filter((item) => item.value !== null)
      : [];
  const hoverX = hoverIndex !== null ? (hoverIndex / Math.max(aligned.dates.length - 1, 1)) * width : null;

  return (
    <div
      className="grid-panel h-[320px] p-4"
      onMouseLeave={() => setHoverIndex(null)}
      onMouseMove={(event) => {
        const rect = (event.currentTarget as HTMLDivElement).getBoundingClientRect();
        const chartLeft = rect.left + 64;
        const chartWidth = Math.max(rect.width - 96, 1);
        const rawX = event.clientX - chartLeft;
        const ratio = Math.max(0, Math.min(1, rawX / chartWidth));
        const idx = Math.round(ratio * Math.max(aligned.dates.length - 1, 1));
        setHoverIndex(idx);
      }}
    >
      <div className="flex h-full gap-4">
        <div className="numeric flex w-20 flex-col justify-between text-xs text-slate-400">
          {grid.map((tick) => (
            <span key={tick.y}>{valueFormatter(tick.val)}</span>
          ))}
        </div>
        <div className="relative flex-1">
          <svg viewBox={`0 0 ${width} ${height}`} className="h-full w-full">
            {grid.map((tick) => (
              <line
                key={tick.y}
                x1={0}
                x2={width}
                y1={tick.y}
                y2={tick.y}
                stroke="rgba(104,128,156,0.16)"
                strokeWidth={1}
              />
            ))}
            {aligned.lines.map((line) => {
              const path = linePathAligned(line.values, width, height, min, max);
              return (
                <path
                  key={line.name}
                  d={path}
                  fill="none"
                  stroke={line.color}
                  strokeWidth={2.2}
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              );
            })}
            {hoverX !== null ? (
              <line x1={hoverX} x2={hoverX} y1={0} y2={height} stroke="rgba(140, 220, 255, 0.4)" strokeWidth={1} />
            ) : null}
            {hoverIndex !== null
              ? aligned.lines.map((line) => {
                  const value = line.values[hoverIndex];
                  if (value === null) {
                    return null;
                  }
                  const y = height - ((value - min) / span) * height;
                  const x = (hoverIndex / Math.max(aligned.dates.length - 1, 1)) * width;
                  return <circle key={`${line.name}-dot`} cx={x} cy={y} r={3.5} fill={line.color} />;
                })
              : null}
          </svg>
          <div className="absolute bottom-2 left-2 flex flex-wrap gap-3 text-xs">
            {aligned.lines.map((line) => (
              <div key={line.name} className="numeric flex items-center gap-2 text-slate-300">
                <span className="inline-block h-2 w-2 rounded-full" style={{ backgroundColor: line.color }} />
                <span>{line.name}</span>
              </div>
            ))}
          </div>
          {hoverDate ? (
            <div className="pointer-events-none absolute right-2 top-2 min-w-[150px] rounded border border-terminal-border bg-black/85 p-2 text-xs">
              <div className="numeric mb-1 text-slate-300">{hoverDate}</div>
              <div className="space-y-1">
                {hoverRows.map((row) => (
                  <div key={`tip-${row.name}`} className="numeric flex items-center justify-between gap-2">
                    <span className="flex items-center gap-2 text-slate-300">
                      <span className="inline-block h-2 w-2 rounded-full" style={{ backgroundColor: row.color }} />
                      {row.name}
                    </span>
                    <span className="text-white">{valueFormatter(row.value!)}</span>
                  </div>
                ))}
              </div>
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}

function correlationCellColor(value: number | null): string {
  if (value === null || Number.isNaN(value)) {
    return "rgba(15, 23, 42, 0.35)";
  }
  const clamped = Math.max(-1, Math.min(1, value));
  if (clamped >= 0) {
    const alpha = 0.15 + clamped * 0.45;
    return `rgba(34, 197, 94, ${alpha.toFixed(3)})`;
  }
  const alpha = 0.15 + Math.abs(clamped) * 0.45;
  return `rgba(239, 68, 68, ${alpha.toFixed(3)})`;
}

function rollingCorrelationSeries(
  aPoints: SeriesPoint[],
  bPoints: SeriesPoint[],
  window: number,
): { date: string; value: number }[] {
  if (window < 2) {
    return [];
  }
  const aReturns = new Map<string, number>();
  const bReturns = new Map<string, number>();

  const aSorted = [...aPoints].sort((x, y) => x.date.localeCompare(y.date));
  const bSorted = [...bPoints].sort((x, y) => x.date.localeCompare(y.date));

  for (let i = 1; i < aSorted.length; i += 1) {
    const prev = aSorted[i - 1].close;
    const curr = aSorted[i].close;
    if (prev > 0) {
      aReturns.set(aSorted[i].date, (curr / prev) - 1);
    }
  }
  for (let i = 1; i < bSorted.length; i += 1) {
    const prev = bSorted[i - 1].close;
    const curr = bSorted[i].close;
    if (prev > 0) {
      bReturns.set(bSorted[i].date, (curr / prev) - 1);
    }
  }

  const common = Array.from(aReturns.keys()).filter((date) => bReturns.has(date)).sort();
  const output: { date: string; value: number }[] = [];
  for (let i = window - 1; i < common.length; i += 1) {
    const frame = common.slice(i - window + 1, i + 1);
    const x = frame.map((date) => aReturns.get(date)!);
    const y = frame.map((date) => bReturns.get(date)!);
    const value = corr(x, y);
    if (value !== null) {
      output.push({ date: common[i], value });
    }
  }
  return output;
}

function CorrelationMatrix({
  symbols,
  data,
}: {
  symbols: string[];
  data: CorrelationResponse | null;
}) {
  if (!data || symbols.length < 2) {
    return <div className="text-xs text-slate-500">Need at least 2 symbols to compute correlation.</div>;
  }

  const lookup = new Map<string, number | null>();
  for (const cell of data.matrix) {
    lookup.set(`${cell.symbol_a}::${cell.symbol_b}`, cell.correlation);
  }

  return (
    <div className="overflow-auto">
      <table className="w-full min-w-[320px] text-xs">
        <thead>
          <tr className="border-b border-terminal-border text-left uppercase tracking-[0.12em] text-slate-400">
            <th className="px-2 py-2">Symbol</th>
            {symbols.map((symbol) => (
              <th key={`h-${symbol}`} className="numeric px-2 py-2">
                {symbol}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {symbols.map((rowSymbol) => (
            <tr key={`r-${rowSymbol}`} className="border-b border-slate-900/80">
              <td className="numeric px-2 py-2 text-slate-300">{rowSymbol}</td>
              {symbols.map((colSymbol) => {
                const value = lookup.get(`${rowSymbol}::${colSymbol}`) ?? null;
                return (
                  <td
                    key={`${rowSymbol}-${colSymbol}`}
                    className="numeric px-2 py-2 text-slate-100"
                    style={{ backgroundColor: correlationCellColor(value) }}
                  >
                    {value === null ? "--" : value.toFixed(3)}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function SymbolDetailDrawer({
  open,
  symbol,
  interval,
  range,
  data,
  loading,
  error,
  onClose,
}: {
  open: boolean;
  symbol: string;
  interval: "1d" | "1w";
  range: { startDate: string; endDate: string };
  data: OhlcvResponse | null;
  loading: boolean;
  error: string | null;
  onClose: () => void;
}) {
  if (!open) {
    return null;
  }

  const points = data?.points ?? [];
  const latest = points.length ? points[points.length - 1] : null;
  const high = points.length ? Math.max(...points.map((p) => p.high)) : null;
  const low = points.length ? Math.min(...points.map((p) => p.low)) : null;
  const avgVol = averageVolume(points);

  return (
    <div className="fixed inset-0 z-50 flex justify-end bg-black/65">
      <aside className="h-full w-full max-w-[460px] border-l border-terminal-border bg-[#050505] p-4 text-terminal-text shadow-2xl">
        <div className="flex items-center justify-between">
          <div>
            <div className="text-xs uppercase tracking-[0.18em] text-terminal-cyan">Symbol Detail</div>
            <h2 className="numeric mt-1 text-xl text-white">{symbol}</h2>
          </div>
          <button
            onClick={onClose}
            className="numeric rounded border border-terminal-border px-2 py-1 text-xs text-slate-300 hover:bg-slate-900"
          >
            Esc Close
          </button>
        </div>

        <div className="mt-2 text-[11px] uppercase tracking-[0.12em] text-slate-400">
          {range.startDate} to {range.endDate} | interval {interval}
        </div>

        {loading ? <div className="mt-4 text-sm text-slate-400">Loading...</div> : null}
        {error ? (
          <div className="mt-4 rounded border border-terminal-red/40 bg-terminal-red/10 p-3 text-sm text-terminal-red">
            {error}
          </div>
        ) : null}

        {!loading && !error ? (
          <>
            <div className="mt-4 grid grid-cols-2 gap-2">
              <div className="rounded border border-terminal-border bg-black/70 p-2">
                <div className="text-[10px] uppercase tracking-[0.12em] text-slate-400">Last Close</div>
                <div className="numeric mt-1 text-lg text-white">{numberFmt(latest?.close ?? null)}</div>
              </div>
              <div className="rounded border border-terminal-border bg-black/70 p-2">
                <div className="text-[10px] uppercase tracking-[0.12em] text-slate-400">Last Date</div>
                <div className="numeric mt-1 text-sm text-slate-200">{latest?.date ?? "--"}</div>
              </div>
              <div className="rounded border border-terminal-border bg-black/70 p-2">
                <div className="text-[10px] uppercase tracking-[0.12em] text-slate-400">Period High</div>
                <div className="numeric mt-1 text-sm text-slate-200">{numberFmt(high)}</div>
              </div>
              <div className="rounded border border-terminal-border bg-black/70 p-2">
                <div className="text-[10px] uppercase tracking-[0.12em] text-slate-400">Period Low</div>
                <div className="numeric mt-1 text-sm text-slate-200">{numberFmt(low)}</div>
              </div>
              <div className="rounded border border-terminal-border bg-black/70 p-2">
                <div className="text-[10px] uppercase tracking-[0.12em] text-slate-400">Avg Volume</div>
                <div className="numeric mt-1 text-sm text-slate-200">
                  {avgVol === null ? "--" : new Intl.NumberFormat("en-IN", { maximumFractionDigits: 0 }).format(avgVol)}
                </div>
              </div>
              <div className="rounded border border-terminal-border bg-black/70 p-2">
                <div className="text-[10px] uppercase tracking-[0.12em] text-slate-400">Bars</div>
                <div className="numeric mt-1 text-sm text-slate-200">{points.length}</div>
              </div>
            </div>

            <div className="mt-4 overflow-auto rounded border border-terminal-border">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-terminal-border bg-slate-950 text-left uppercase tracking-[0.12em] text-slate-400">
                    <th className="px-2 py-2">Date</th>
                    <th className="px-2 py-2">Close</th>
                    <th className="px-2 py-2">Volume</th>
                  </tr>
                </thead>
                <tbody>
                  {points
                    .slice(-20)
                    .reverse()
                    .map((point) => (
                      <tr key={`drawer-${point.date}`} className="border-b border-slate-900/80">
                        <td className="numeric px-2 py-2">{point.date}</td>
                        <td className="numeric px-2 py-2">{numberFmt(point.close)}</td>
                        <td className="numeric px-2 py-2">
                          {new Intl.NumberFormat("en-IN", { maximumFractionDigits: 0 }).format(point.volume)}
                        </td>
                      </tr>
                    ))}
                </tbody>
              </table>
            </div>
          </>
        ) : null}
      </aside>
    </div>
  );
}

export default function DashboardPage() {
  const [timeframe, setTimeframe] = useState<TimeframeKey>("1Y");
  const [interval, setInterval] = useState<"1d" | "1w">("1w");

  const [primaryInput, setPrimaryInput] = useState("RELIANCE");
  const [primaryUniverse, setPrimaryUniverse] = useState<"stock" | "index">("stock");
  const [primarySymbol, setPrimarySymbol] = useState("RELIANCE");

  const [compareEnabled, setCompareEnabled] = useState(false);
  const [compareInput, setCompareInput] = useState("");
  const [compareTargets, setCompareTargets] = useState<Target[]>([]);
  const [normalizedBase, setNormalizedBase] = useState<number>(100);
  const [rollingWindow, setRollingWindow] = useState<number>(26);

  const [indexRows, setIndexRows] = useState<IndexSnapshotRow[]>([]);
  const [primaryData, setPrimaryData] = useState<OhlcvResponse | null>(null);
  const [compareData, setCompareData] = useState<CompareResponse | null>(null);
  const [corrData, setCorrData] = useState<CorrelationResponse | null>(null);
  const [compareRawSeries, setCompareRawSeries] = useState<Array<{ symbol: string; points: SeriesPoint[] }>>([]);

  const [loadingPrimary, setLoadingPrimary] = useState(false);
  const [loadingCompare, setLoadingCompare] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [compareError, setCompareError] = useState<string | null>(null);
  const [rangeError, setRangeError] = useState<string | null>(null);
  const [rangeOverride, setRangeOverride] = useState<{ startDate: string; endDate: string } | null>(null);

  const [detailOpen, setDetailOpen] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);

  const primaryInputRef = useRef<HTMLInputElement>(null);
  const compareInputRef = useRef<HTMLInputElement>(null);

  const primaryMatches = useSearch(primaryInput);
  const compareMatches = useSearch(compareInput);
  const primaryTarget = useMemo<Target>(
    () => ({ type: primaryUniverse, code: primarySymbol }),
    [primaryUniverse, primarySymbol],
  );

  const timeframeRange = useMemo(() => rangeForTimeframe(timeframe), [timeframe]);
  const range = rangeOverride ?? timeframeRange;
  const [startInput, setStartInput] = useState(timeframeRange.startDate);
  const [endInput, setEndInput] = useState(timeframeRange.endDate);

  useEffect(() => {
    if (!rangeOverride) {
      setStartInput(timeframeRange.startDate);
      setEndInput(timeframeRange.endDate);
    }
  }, [timeframeRange, rangeOverride]);

  function setTimeframeSelection(tf: TimeframeKey): void {
    setTimeframe(tf);
    setRangeOverride(null);
    setRangeError(null);
  }

  function applyCustomRange(): void {
    if (!startInput || !endInput) {
      setRangeError("Both start and end dates are required.");
      return;
    }
    if (startInput > endInput) {
      setRangeError("Start date must be before end date.");
      return;
    }
    setRangeError(null);
    setRangeOverride({ startDate: startInput, endDate: endInput });
  }

  function resetToTimeframeRange(): void {
    setRangeError(null);
    setRangeOverride(null);
    setStartInput(timeframeRange.startDate);
    setEndInput(timeframeRange.endDate);
  }

  function addCompareTarget(target: Target): void {
    if (!target.code || targetKey(target) === targetKey(primaryTarget)) {
      return;
    }
    setCompareTargets((prev) => {
      if (prev.length >= 5) {
        return prev;
      }
      const exists = prev.some((item) => targetKey(item) === targetKey(target));
      if (exists) {
        return prev;
      }
      return [...prev, target];
    });
    setCompareInput("");
    setCompareEnabled(true);
  }

  function removeCompareTarget(target: Target): void {
    setCompareTargets((prev) => prev.filter((item) => targetKey(item) !== targetKey(target)));
  }

  useEffect(() => {
    setCompareTargets((prev) => prev.filter((item) => targetKey(item) !== targetKey(primaryTarget)));
  }, [primaryTarget]);

  useEffect(() => {
    fetchIndexSnapshot()
      .then((rows) => setIndexRows(rows))
      .catch(() => setIndexRows([]));
  }, []);

  useEffect(() => {
    setLoadingPrimary(true);
    setError(null);
    fetchOhlcv({
      symbol: primarySymbol,
      universe: primaryUniverse,
      interval,
      startDate: range.startDate,
      endDate: range.endDate,
    })
      .then((rows) => setPrimaryData(rows))
      .catch((err) => {
        setPrimaryData(null);
        setError(err instanceof Error ? err.message : "Failed to load primary OHLCV");
      })
      .finally(() => setLoadingPrimary(false));
  }, [primarySymbol, primaryUniverse, interval, range.endDate, range.startDate]);

  useEffect(() => {
    if (!detailOpen) {
      return;
    }
    setDetailLoading(true);
    setDetailError(null);
    fetchOhlcv({
      symbol: primarySymbol,
      universe: primaryUniverse,
      interval,
      startDate: range.startDate,
      endDate: range.endDate,
    })
      .then((rows) => setPrimaryData(rows))
      .catch((err) => setDetailError(err instanceof Error ? err.message : "Failed to load symbol detail"))
      .finally(() => setDetailLoading(false));
  }, [detailOpen, primarySymbol, primaryUniverse, interval, range.endDate, range.startDate]);

  const compareUniverse = useMemo(() => {
    const map = new Map<string, Target>();
    [primaryTarget, ...compareTargets].forEach((target) => map.set(targetKey(target), target));
    return Array.from(map.values()).slice(0, 6);
  }, [primaryTarget, compareTargets]);

  useEffect(() => {
    if (!compareEnabled) {
      setCompareRawSeries([]);
      setCompareData(null);
      setCorrData(null);
      setCompareError(null);
      return;
    }
    if (compareUniverse.length < 2) {
      setCompareRawSeries([]);
      setCompareData(null);
      setCorrData(null);
      setCompareError(null);
      return;
    }

    setLoadingCompare(true);
    setCompareError(null);
    Promise.all(
      compareUniverse.map(async (target) => {
        const payload = await fetchOhlcv({
          symbol: target.code,
          universe: target.type,
          interval,
          startDate: range.startDate,
          endDate: range.endDate,
        });
        return { target, payload };
      }),
    )
      .then((rows) => {
        const raw = rows.map(({ target, payload }) => ({
          symbol: targetLabel(target),
          points: payload.points,
        }));
        setCompareRawSeries(raw);

        const universeSet = new Set(rows.map((row) => row.target.type));
        const universeType: "stock" | "index" | "mixed" = universeSet.size > 1 ? "mixed" : rows[0].target.type;

        const series = raw.map((row) => {
          const points = [...row.points].sort((a, b) => a.date.localeCompare(b.date));
          if (points.length === 0 || points[0].close === 0) {
            return {
              symbol: row.symbol,
              base_close: 0,
              normalized_base: normalizedBase,
              period_return_pct: 0,
              normalized: [],
            };
          }
          const base = points[0].close;
          const normalized = points.map((point) => ({
            date: point.date,
            value: (point.close / base) * normalizedBase,
          }));
          return {
            symbol: row.symbol,
            base_close: base,
            normalized_base: normalizedBase,
            period_return_pct: ((points[points.length - 1].close / base) - 1) * 100,
            normalized,
          };
        });
        setCompareData({
          universe: universeType,
          interval,
          normalized_base: normalizedBase,
          start_date: range.startDate,
          end_date: range.endDate,
          series,
        });

        const returnsMap = new Map<string, Map<string, number>>();
        raw.forEach((row) => {
          const ret = new Map<string, number>();
          const points = [...row.points].sort((a, b) => a.date.localeCompare(b.date));
          for (let i = 1; i < points.length; i += 1) {
            const prev = points[i - 1].close;
            const curr = points[i].close;
            if (prev > 0) {
              ret.set(points[i].date, (curr / prev) - 1);
            }
          }
          returnsMap.set(row.symbol, ret);
        });

        const symbols = raw.map((row) => row.symbol);
        const matrix: CorrelationResponse["matrix"] = [];
        for (const a of symbols) {
          for (const b of symbols) {
            const da = returnsMap.get(a) ?? new Map<string, number>();
            const db = returnsMap.get(b) ?? new Map<string, number>();
            const common = Array.from(da.keys()).filter((d) => db.has(d)).sort();
            const x = common.map((d) => da.get(d)!);
            const y = common.map((d) => db.get(d)!);
            matrix.push({
              symbol_a: a,
              symbol_b: b,
              correlation: corr(x, y),
              observations: common.length,
            });
          }
        }
        setCorrData({
          universe: universeType,
          interval,
          window: interval === "1w" ? 52 : 252,
          start_date: range.startDate,
          end_date: range.endDate,
          matrix,
        });
      })
      .catch((err) => {
        setCompareRawSeries([]);
        setCompareData(null);
        setCorrData(null);
        setCompareError(err instanceof Error ? err.message : "Failed to load compare analytics");
      })
      .finally(() => setLoadingCompare(false));
  }, [compareEnabled, compareUniverse, interval, normalizedBase, range.endDate, range.startDate]);

  useEffect(() => {
    setRollingWindow(interval === "1w" ? 26 : 60);
  }, [interval]);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      const keyLower = event.key.toLowerCase();
      if ((event.ctrlKey || event.metaKey) && keyLower === "k") {
        event.preventDefault();
        primaryInputRef.current?.focus();
        primaryInputRef.current?.select();
        return;
      }
      if (keyLower === "escape" && detailOpen) {
        event.preventDefault();
        setDetailOpen(false);
        return;
      }

      const target = event.target as HTMLElement | null;
      const isTyping =
        target !== null &&
        (target.tagName === "INPUT" ||
          target.tagName === "TEXTAREA" ||
          target.tagName === "SELECT" ||
          target.isContentEditable);
      if (isTyping) {
        return;
      }

      if (event.key >= "1" && event.key <= "7") {
        const idx = Number(event.key) - 1;
        const tf = TIMEFRAMES[idx];
        if (tf) {
          event.preventDefault();
          setTimeframeSelection(tf);
        }
        return;
      }

      if (keyLower === "c") {
        event.preventDefault();
        setCompareEnabled((prev) => {
          const next = !prev;
          if (next) {
            setTimeout(() => compareInputRef.current?.focus(), 0);
          }
          return next;
        });
      }
    };

    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [detailOpen]);

  const primaryPoints = primaryData?.points ?? [];
  const primaryTargetLabel = targetLabel(primaryTarget);
  const primaryLast = primaryPoints.length ? primaryPoints[primaryPoints.length - 1] : null;
  const change = firstLastChange(primaryPoints);
  const vol = computeVolatility(primaryPoints, interval === "1w" ? 52 : 252);

  const chartLines = useMemo<LineSeries[]>(() => {
    if (compareEnabled && compareData?.series?.length) {
      return compareData.series
        .filter((row) => row.normalized.length > 0)
        .map((row, idx) => ({
          name: row.symbol,
          color: CHART_COLORS[idx % CHART_COLORS.length],
          points: row.normalized.map((point) => ({ date: point.date, value: point.value })),
        }));
    }
    if (primaryPoints.length === 0) {
      return [];
    }
    return [
      {
        name: primaryTargetLabel,
        color: CHART_COLORS[0],
        points: primaryPoints.map((point) => ({ date: point.date, value: point.close })),
      },
    ];
  }, [compareEnabled, compareData, primaryPoints, primaryTargetLabel]);

  const correlationSymbols = useMemo(() => {
    if (compareData?.series?.length) {
      return compareData.series.map((row) => row.symbol);
    }
    return compareUniverse.map((target) => targetLabel(target));
  }, [compareData, compareUniverse]);

  const rollingCompareLines = useMemo<LineSeries[]>(() => {
    if (!compareEnabled || compareRawSeries.length < 2) {
      return [];
    }
    const primarySeries =
      compareRawSeries.find((series) => series.symbol === primaryTargetLabel) ?? compareRawSeries[0];
    const secondarySeries = compareRawSeries.find((series) => series.symbol !== primarySeries.symbol);
    if (!secondarySeries) {
      return [];
    }
    const points = rollingCorrelationSeries(primarySeries.points, secondarySeries.points, rollingWindow);
    if (points.length === 0) {
      return [];
    }
    return [
      {
        name: `Rolling Corr ${primarySeries.symbol} vs ${secondarySeries.symbol}`,
        color: "#5cd1ff",
        points: points.map((point) => ({ date: point.date, value: point.value })),
      },
    ];
  }, [compareEnabled, compareRawSeries, primaryTargetLabel, rollingWindow]);

  const canAddMoreCompare = compareTargets.length < 5;

  return (
    <main className="min-h-screen p-4 text-terminal-text md:p-5">
      <div className="mx-auto flex max-w-[1700px] flex-col gap-4">
        <header className="grid-panel flex flex-wrap items-center justify-between gap-4 px-4 py-3">
          <div>
            <div className="text-xs font-medium uppercase tracking-[0.26em] text-terminal-cyan">Indian Market Terminal</div>
            <h1 className="mt-1 text-xl font-semibold text-white md:text-2xl">Historic Analytics Dashboard</h1>
          </div>
          <div className="numeric flex items-center gap-2 text-xs md:text-sm">
            <Link href="/backtest" className="rounded border border-terminal-border px-2 py-1 text-slate-300 hover:bg-slate-900/70">
              Backtest Builder
            </Link>
            <button
              onClick={() => setInterval("1w")}
              className={cls(
                "rounded border px-2 py-1",
                interval === "1w"
                  ? "border-terminal-cyan bg-terminal-cyan/20 text-terminal-cyan"
                  : "border-terminal-border text-slate-300",
              )}
            >
              Weekly
            </button>
            <button
              onClick={() => setInterval("1d")}
              className={cls(
                "rounded border px-2 py-1",
                interval === "1d"
                  ? "border-terminal-accent bg-terminal-accent/20 text-terminal-accent"
                  : "border-terminal-border text-slate-300",
              )}
            >
              Daily
            </button>
            <span className="rounded border border-terminal-border bg-slate-950 px-2 py-1 text-slate-300">
              Ticker: {primaryTargetLabel}
            </span>
            <span className="rounded border border-terminal-border bg-slate-950 px-2 py-1 text-slate-300">
              {range.startDate} to {range.endDate}
            </span>
          </div>
        </header>

        <section className="grid grid-cols-1 gap-3 md:grid-cols-4">
          {indexRows.map((row) => (
            <button
              key={row.index_name}
              className="grid-panel p-3 text-left transition-colors hover:bg-slate-900/70"
              onClick={() => {
                setPrimaryUniverse("index");
                setPrimarySymbol(row.index_name);
                setPrimaryInput(row.index_name);
              }}
            >
              <div className="text-[11px] uppercase tracking-[0.18em] text-slate-400">{row.index_name}</div>
              <div className="numeric mt-2 text-2xl font-semibold text-white">{numberFmt(row.close)}</div>
              <div
                className={cls(
                  "numeric mt-1 text-sm font-medium",
                  (row.pct_change ?? 0) >= 0 ? "text-terminal-green" : "text-terminal-red",
                )}
              >
                {percent(row.pct_change)}
              </div>
            </button>
          ))}
        </section>

        <section className="grid grid-cols-1 gap-4 xl:grid-cols-[320px_1fr_320px]">
          <aside className="grid-panel p-4">
            <div className="text-xs uppercase tracking-[0.2em] text-terminal-cyan">Symbol Console</div>

            <label className="mt-4 block text-xs uppercase tracking-[0.14em] text-slate-400">Primary (Ctrl+K)</label>
            <input
              ref={primaryInputRef}
              className="numeric mt-2 w-full rounded border border-terminal-border bg-black/70 px-3 py-2 text-sm text-white outline-none focus:border-terminal-accent"
              value={primaryInput}
              onChange={(e) => setPrimaryInput(e.target.value.toUpperCase())}
              placeholder="Search stock or index"
            />
            <div className="mt-2 max-h-52 overflow-auto">
              {primaryMatches.slice(0, 20).map((item) => (
                <button
                  key={`${item.type}-${item.code}-primary`}
                  className="w-full border-b border-slate-800 px-2 py-2 text-left text-xs hover:bg-slate-800/50"
                  onClick={() => {
                    setPrimaryUniverse(item.type);
                    setPrimarySymbol(item.code);
                    setPrimaryInput(item.code);
                  }}
                >
                  <span className={cls("numeric", item.type === "index" ? "text-terminal-cyan" : "text-terminal-accent")}>
                    {item.type === "index" ? "IDX" : "STK"}:{item.code}
                  </span>{" "}
                  <span className="text-slate-400">{item.name}</span>
                </button>
              ))}
            </div>

            <div className="mt-4 border-t border-slate-800 pt-4">
              <div className="flex items-center justify-between">
                <label className="block text-xs uppercase tracking-[0.14em] text-slate-400">Compare (C)</label>
                <button
                  onClick={() => setCompareEnabled((prev) => !prev)}
                  className={cls(
                    "numeric rounded border px-2 py-1 text-[11px]",
                    compareEnabled
                      ? "border-terminal-cyan bg-terminal-cyan/15 text-terminal-cyan"
                      : "border-terminal-border text-slate-300",
                  )}
                >
                  {compareEnabled ? "ON" : "OFF"}
                </button>
              </div>

              <div className="numeric mt-2 text-[11px] text-slate-500">
                Symbols: {compareUniverse.length}/6 (2-6 supported, 3-6 recommended)
              </div>

              <input
                ref={compareInputRef}
                disabled={!compareEnabled || !canAddMoreCompare}
                className="numeric mt-2 w-full rounded border border-terminal-border bg-black/70 px-3 py-2 text-sm text-white outline-none focus:border-terminal-cyan disabled:cursor-not-allowed disabled:opacity-40"
                value={compareInput}
                onChange={(e) => setCompareInput(e.target.value.toUpperCase())}
                placeholder={canAddMoreCompare ? "Add comparison symbol" : "Max 6 symbols reached"}
              />
              <div className="mt-2 max-h-40 overflow-auto">
                {compareEnabled &&
                  compareMatches.slice(0, 20).map((item) => (
                    <button
                      key={`${item.type}-${item.code}-compare`}
                      className="w-full border-b border-slate-800 px-2 py-2 text-left text-xs hover:bg-slate-800/50"
                      onClick={() => addCompareTarget({ type: item.type, code: item.code })}
                    >
                      <span className={cls("numeric", item.type === "index" ? "text-terminal-cyan" : "text-terminal-accent")}>
                        {item.type === "index" ? "IDX" : "STK"}:{item.code}
                      </span>{" "}
                      <span className="text-slate-400">{item.name}</span>
                    </button>
                  ))}
              </div>

              <div className="mt-2 flex flex-wrap gap-2">
                <span className="numeric rounded border border-terminal-accent bg-terminal-accent/15 px-2 py-1 text-[11px] text-terminal-accent">
                  {primaryTargetLabel} (primary)
                </span>
                {compareTargets.map((target) => (
                  <button
                    key={`chip-${targetKey(target)}`}
                    onClick={() => removeCompareTarget(target)}
                    className="numeric rounded border border-terminal-cyan/60 bg-terminal-cyan/10 px-2 py-1 text-[11px] text-terminal-cyan"
                    title="Remove"
                  >
                    {targetLabel(target)} x
                  </button>
                ))}
              </div>

              <div className="mt-3">
                <label className="block text-[11px] uppercase tracking-[0.12em] text-slate-400">Normalized Base</label>
                <select
                  value={normalizedBase}
                  onChange={(e) => setNormalizedBase(Number(e.target.value))}
                  className="numeric mt-1 w-full rounded border border-terminal-border bg-black/70 px-2 py-1 text-xs text-white outline-none focus:border-terminal-cyan"
                >
                  {NORMALIZED_BASE_OPTIONS.map((option) => (
                    <option key={option} value={option}>
                      {option}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            <div className="mt-4 border-t border-slate-800 pt-4">
              <div className="text-xs uppercase tracking-[0.16em] text-slate-400">Timeframe (1..7)</div>
              <div className="mt-2 flex flex-wrap gap-2">
                {TIMEFRAMES.map((tf, idx) => (
                  <button
                    key={tf}
                    onClick={() => setTimeframeSelection(tf)}
                    className={cls(
                      "numeric rounded border px-2 py-1 text-xs",
                      tf === timeframe
                        ? "border-terminal-accent bg-terminal-accent/20 text-terminal-accent"
                        : "border-terminal-border text-slate-300 hover:bg-slate-800/60",
                    )}
                  >
                    {idx + 1}:{tf}
                  </button>
                ))}
              </div>

              <div className="mt-4 space-y-2">
                <div className="text-[11px] uppercase tracking-[0.14em] text-slate-400">Manual Date Range</div>
                <div className="grid grid-cols-2 gap-2">
                  <input
                    type="date"
                    value={startInput}
                    onChange={(e) => setStartInput(e.target.value)}
                    className="numeric w-full rounded border border-terminal-border bg-black/70 px-2 py-1 text-xs text-white outline-none focus:border-terminal-cyan"
                  />
                  <input
                    type="date"
                    value={endInput}
                    onChange={(e) => setEndInput(e.target.value)}
                    className="numeric w-full rounded border border-terminal-border bg-black/70 px-2 py-1 text-xs text-white outline-none focus:border-terminal-cyan"
                  />
                </div>
                <div className="flex gap-2">
                  <button
                    onClick={applyCustomRange}
                    className="numeric rounded border border-terminal-cyan bg-terminal-cyan/10 px-2 py-1 text-[11px] text-terminal-cyan"
                  >
                    Apply
                  </button>
                  <button
                    onClick={resetToTimeframeRange}
                    className="numeric rounded border border-terminal-border px-2 py-1 text-[11px] text-slate-300"
                  >
                    Reset
                  </button>
                </div>
                {rangeError ? <div className="text-[11px] text-terminal-red">{rangeError}</div> : null}
              </div>
            </div>
          </aside>

          <section className="space-y-4">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-xs uppercase tracking-[0.18em] text-slate-400">Chart View</div>
                <div className="numeric mt-1 text-lg text-white">
                  {compareEnabled ? compareUniverse.map((target) => targetLabel(target)).join(" vs ") : primaryTargetLabel}
                </div>
              </div>
              <div className="numeric flex items-center gap-2 text-xs text-slate-400">
                {loadingPrimary ? <span>Primary loading...</span> : null}
                {loadingCompare ? <span>Compare loading...</span> : null}
              </div>
            </div>

            {error ? (
              <div className="rounded border border-terminal-red/40 bg-terminal-red/10 p-3 text-sm text-terminal-red">{error}</div>
            ) : null}
            {compareError ? (
              <div className="rounded border border-terminal-red/40 bg-terminal-red/10 p-3 text-sm text-terminal-red">{compareError}</div>
            ) : null}
            {compareEnabled && compareUniverse.length < 2 ? (
              <div className="rounded border border-terminal-cyan/40 bg-terminal-cyan/10 p-3 text-xs text-terminal-cyan">
                Add one comparison symbol to start normalized compare charts.
              </div>
            ) : null}
            {compareEnabled && compareUniverse.length === 2 ? (
              <div className="rounded border border-terminal-cyan/40 bg-terminal-cyan/10 p-3 text-xs text-terminal-cyan">
                Two-symbol compare is active. Add more symbols for broader cross-sectional analysis.
              </div>
            ) : null}

            <MiniLineChart series={chartLines} />
            {compareEnabled ? (
              <div className="grid-panel p-3">
                <div className="mb-2 flex items-center justify-between">
                  <div className="text-xs uppercase tracking-[0.16em] text-terminal-cyan">Rolling Correlation</div>
                  <div className="flex items-center gap-2">
                    <span className="numeric text-[11px] text-slate-400">Window</span>
                    <select
                      value={rollingWindow}
                      onChange={(e) => setRollingWindow(Number(e.target.value))}
                      className="numeric rounded border border-terminal-border bg-black/70 px-2 py-1 text-xs text-white outline-none focus:border-terminal-cyan"
                    >
                      {(interval === "1w" ? [12, 26, 52] : [20, 60, 120]).map((w) => (
                        <option key={`roll-${w}`} value={w}>
                          {w}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>
                {rollingCompareLines.length ? (
                  <MiniLineChart series={rollingCompareLines} valueFormatter={(value) => value.toFixed(3)} />
                ) : (
                  <div className="text-xs text-slate-500">Not enough overlapping history for rolling correlation yet.</div>
                )}
              </div>
            ) : null}

            <div className="grid-panel overflow-auto">
              <table className="w-full min-w-[640px] text-xs">
                <thead>
                  <tr className="border-b border-terminal-border text-left uppercase tracking-[0.14em] text-slate-400">
                    <th className="px-3 py-2">Date</th>
                    <th className="px-3 py-2">Open</th>
                    <th className="px-3 py-2">High</th>
                    <th className="px-3 py-2">Low</th>
                    <th className="px-3 py-2">Close</th>
                    <th className="px-3 py-2">Volume</th>
                  </tr>
                </thead>
                <tbody>
                  {primaryPoints
                    .slice(-25)
                    .reverse()
                    .map((point) => (
                      <tr key={point.date} className="border-b border-slate-900/80 hover:bg-slate-900/40">
                        <td className="numeric px-3 py-2">{point.date}</td>
                        <td className="numeric px-3 py-2">{numberFmt(point.open)}</td>
                        <td className="numeric px-3 py-2">{numberFmt(point.high)}</td>
                        <td className="numeric px-3 py-2">{numberFmt(point.low)}</td>
                        <td className="numeric px-3 py-2 text-white">{numberFmt(point.close)}</td>
                        <td className="numeric px-3 py-2">{new Intl.NumberFormat("en-IN").format(point.volume)}</td>
                      </tr>
                    ))}
                </tbody>
              </table>
            </div>
          </section>

          <aside className="space-y-4">
            <article className="grid-panel p-4">
              <div className="flex items-center justify-between">
                <div className="text-xs uppercase tracking-[0.16em] text-terminal-cyan">Snapshot</div>
                <button
                  onClick={() => setDetailOpen(true)}
                  className="numeric rounded border border-terminal-border px-2 py-1 text-[11px] text-slate-300 hover:bg-slate-900"
                >
                  Detail
                </button>
              </div>
              <div className="mt-3 space-y-3">
                <div>
                  <div className="text-[11px] uppercase tracking-[0.12em] text-slate-400">Last Close</div>
                  <div className="numeric mt-1 text-xl text-white">{numberFmt(primaryLast?.close ?? null)}</div>
                </div>
                <div className="grid grid-cols-2 gap-2">
                  <div className="rounded border border-terminal-border bg-black/70 p-2">
                    <div className="text-[10px] uppercase tracking-[0.12em] text-slate-400">Change</div>
                    <div
                      className={cls(
                        "numeric mt-1 text-sm",
                        (change.abs ?? 0) >= 0 ? "text-terminal-green" : "text-terminal-red",
                      )}
                    >
                      {numberFmt(change.abs)}
                    </div>
                  </div>
                  <div className="rounded border border-terminal-border bg-black/70 p-2">
                    <div className="text-[10px] uppercase tracking-[0.12em] text-slate-400">Return</div>
                    <div
                      className={cls(
                        "numeric mt-1 text-sm",
                        (change.pct ?? 0) >= 0 ? "text-terminal-green" : "text-terminal-red",
                      )}
                    >
                      {percent(change.pct)}
                    </div>
                  </div>
                </div>
                <div className="rounded border border-terminal-border bg-black/70 p-2">
                  <div className="text-[10px] uppercase tracking-[0.12em] text-slate-400">Annualized Volatility</div>
                  <div className="numeric mt-1 text-sm text-slate-200">{percent(vol)}</div>
                </div>
              </div>
            </article>

            <article className="grid-panel p-4">
              <div className="text-xs uppercase tracking-[0.16em] text-terminal-cyan">Correlation Matrix</div>
              <div className="numeric mt-2 text-[11px] text-slate-500">Interval {interval} | pairwise overlap basis</div>
              <div className="mt-3">
                {compareEnabled ? (
                  <CorrelationMatrix symbols={correlationSymbols} data={corrData} />
                ) : (
                  <div className="text-xs text-slate-500">Enable compare mode to view matrix.</div>
                )}
              </div>
            </article>

            <article className="grid-panel p-4">
              <div className="text-xs uppercase tracking-[0.16em] text-terminal-cyan">Compare Performance</div>
              <div className="mt-3 space-y-2">
                {(compareData?.series ?? []).map((row) => (
                  <div
                    key={row.symbol}
                    className="flex items-center justify-between rounded border border-terminal-border bg-black/70 px-2 py-2"
                  >
                    <span className="numeric text-xs text-slate-300">{row.symbol}</span>
                    <span
                      className={cls(
                        "numeric text-xs",
                        row.period_return_pct >= 0 ? "text-terminal-green" : "text-terminal-red",
                      )}
                    >
                      {percent(row.period_return_pct)}
                    </span>
                  </div>
                ))}
                {!compareData?.series?.length ? (
                  <div className="text-xs text-slate-500">No compare series loaded.</div>
                ) : null}
              </div>
            </article>
          </aside>
        </section>
      </div>

      <SymbolDetailDrawer
        open={detailOpen}
        symbol={primaryTargetLabel}
        interval={interval}
        range={range}
        data={primaryData}
        loading={detailLoading}
        error={detailError}
        onClose={() => setDetailOpen(false)}
      />
    </main>
  );
}
