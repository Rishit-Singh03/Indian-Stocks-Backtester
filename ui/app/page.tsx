"use client";

import { useEffect, useMemo, useRef, useState } from "react";

import {
  CompareResponse,
  CorrelationResponse,
  IndexSnapshotRow,
  OhlcvResponse,
  SearchResult,
  SeriesPoint,
  fetchCompare,
  fetchCorrelation,
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

const CHART_COLORS = ["#f5a623", "#5cd1ff", "#7cff9e", "#ff6b6b", "#b18cff", "#ffd166"];
const NORMALIZED_BASE_OPTIONS = [100, 1000];

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
      searchSymbols(query)
        .then((rows) => setResults(rows))
        .catch(() => setResults([]));
    }, 160);
    return () => clearTimeout(timer);
  }, [query]);

  return results;
}

function linePath(values: number[], width: number, height: number): string {
  if (values.length === 0) {
    return "";
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  return values
    .map((value, idx) => {
      const x = (idx / Math.max(values.length - 1, 1)) * width;
      const y = height - ((value - min) / span) * height;
      return `${idx === 0 ? "M" : "L"}${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");
}

function MiniLineChart({ series }: { series: LineSeries[] }) {
  const width = 920;
  const height = 280;
  const allValues = series.flatMap((s) => s.points.map((p) => p.value));
  if (allValues.length === 0) {
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

  return (
    <div className="grid-panel h-[320px] p-4">
      <div className="flex h-full gap-4">
        <div className="numeric flex w-20 flex-col justify-between text-xs text-slate-400">
          {grid.map((tick) => (
            <span key={tick.y}>{numberFmt(tick.val)}</span>
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
            {series.map((line) => {
              const path = linePath(
                line.points.map((point) => point.value),
                width,
                height,
              );
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
          </svg>
        </div>
      </div>
    </div>
  );
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
                  <td key={`${rowSymbol}-${colSymbol}`} className="numeric px-2 py-2 text-slate-200">
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
  const [primarySymbol, setPrimarySymbol] = useState("RELIANCE");

  const [compareEnabled, setCompareEnabled] = useState(false);
  const [compareInput, setCompareInput] = useState("");
  const [compareSymbols, setCompareSymbols] = useState<string[]>([]);
  const [normalizedBase, setNormalizedBase] = useState<number>(100);

  const [indexRows, setIndexRows] = useState<IndexSnapshotRow[]>([]);
  const [primaryData, setPrimaryData] = useState<OhlcvResponse | null>(null);
  const [compareData, setCompareData] = useState<CompareResponse | null>(null);
  const [corrData, setCorrData] = useState<CorrelationResponse | null>(null);

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

  const primaryMatches = useSearch(primaryInput).filter((item) => item.type === "stock");
  const compareMatches = useSearch(compareInput).filter((item) => item.type === "stock");

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

  function addCompareSymbol(symbol: string): void {
    const normalized = symbol.trim().toUpperCase();
    if (!normalized || normalized === primarySymbol) {
      return;
    }
    setCompareSymbols((prev) => {
      if (prev.includes(normalized) || prev.length >= 5) {
        return prev;
      }
      return [...prev, normalized];
    });
    setCompareInput("");
    setCompareEnabled(true);
  }

  function removeCompareSymbol(symbol: string): void {
    setCompareSymbols((prev) => prev.filter((item) => item !== symbol));
  }

  useEffect(() => {
    setCompareSymbols((prev) => prev.filter((item) => item !== primarySymbol));
  }, [primarySymbol]);

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
      universe: "stock",
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
  }, [primarySymbol, interval, range.endDate, range.startDate]);

  useEffect(() => {
    if (!detailOpen) {
      return;
    }
    setDetailLoading(true);
    setDetailError(null);
    fetchOhlcv({
      symbol: primarySymbol,
      universe: "stock",
      interval,
      startDate: range.startDate,
      endDate: range.endDate,
    })
      .then((rows) => setPrimaryData(rows))
      .catch((err) => setDetailError(err instanceof Error ? err.message : "Failed to load symbol detail"))
      .finally(() => setDetailLoading(false));
  }, [detailOpen, primarySymbol, interval, range.endDate, range.startDate]);

  const compareUniverse = useMemo(() => {
    const unique = [primarySymbol, ...compareSymbols].filter((value, idx, arr) => arr.indexOf(value) === idx);
    return unique.slice(0, 6);
  }, [primarySymbol, compareSymbols]);

  useEffect(() => {
    if (!compareEnabled) {
      setCompareData(null);
      setCorrData(null);
      setCompareError(null);
      return;
    }
    if (compareUniverse.length < 3) {
      setCompareData(null);
      setCorrData(null);
      setCompareError(null);
      return;
    }

    setLoadingCompare(true);
    setCompareError(null);
    Promise.all([
      fetchCompare({
        symbols: compareUniverse,
        universe: "stock",
        interval,
        normalizedBase,
        startDate: range.startDate,
        endDate: range.endDate,
      }),
      fetchCorrelation({
        symbols: compareUniverse,
        universe: "stock",
        interval,
        window: interval === "1w" ? 52 : 252,
        startDate: range.startDate,
        endDate: range.endDate,
      }),
    ])
      .then(([compareRows, corrRows]) => {
        setCompareData(compareRows);
        setCorrData(corrRows);
      })
      .catch((err) => {
        setCompareData(null);
        setCorrData(null);
        setCompareError(err instanceof Error ? err.message : "Failed to load compare analytics");
      })
      .finally(() => setLoadingCompare(false));
  }, [compareEnabled, compareUniverse, interval, normalizedBase, range.endDate, range.startDate]);

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
        name: primarySymbol,
        color: CHART_COLORS[0],
        points: primaryPoints.map((point) => ({ date: point.date, value: point.close })),
      },
    ];
  }, [compareEnabled, compareData, primaryPoints, primarySymbol]);

  const correlationSymbols = useMemo(() => {
    if (compareData?.series?.length) {
      return compareData.series.map((row) => row.symbol);
    }
    return compareUniverse;
  }, [compareData, compareUniverse]);

  const canAddMoreCompare = compareSymbols.length < 5;

  return (
    <main className="min-h-screen p-4 text-terminal-text md:p-5">
      <div className="mx-auto flex max-w-[1700px] flex-col gap-4">
        <header className="grid-panel flex flex-wrap items-center justify-between gap-4 px-4 py-3">
          <div>
            <div className="text-xs font-medium uppercase tracking-[0.26em] text-terminal-cyan">Indian Market Terminal</div>
            <h1 className="mt-1 text-xl font-semibold text-white md:text-2xl">Historic Analytics Dashboard</h1>
          </div>
          <div className="numeric flex items-center gap-2 text-xs md:text-sm">
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
            <span className="rounded border border-terminal-border bg-slate-950 px-2 py-1 text-slate-300">Ticker: {primarySymbol}</span>
            <span className="rounded border border-terminal-border bg-slate-950 px-2 py-1 text-slate-300">
              {range.startDate} to {range.endDate}
            </span>
          </div>
        </header>

        <section className="grid grid-cols-1 gap-3 md:grid-cols-4">
          {indexRows.map((row) => (
            <article key={row.index_name} className="grid-panel p-3">
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
            </article>
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
              placeholder="Search stock"
            />
            <div className="mt-2 max-h-36 overflow-auto">
              {primaryMatches.slice(0, 8).map((item) => (
                <button
                  key={`${item.code}-primary`}
                  className="w-full border-b border-slate-800 px-2 py-2 text-left text-xs hover:bg-slate-800/50"
                  onClick={() => {
                    setPrimarySymbol(item.code);
                    setPrimaryInput(item.code);
                  }}
                >
                  <span className="numeric text-terminal-accent">{item.code}</span>{" "}
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
                Symbols: {compareUniverse.length}/6 (target 3-6 for full analytics)
              </div>

              <input
                ref={compareInputRef}
                disabled={!compareEnabled || !canAddMoreCompare}
                className="numeric mt-2 w-full rounded border border-terminal-border bg-black/70 px-3 py-2 text-sm text-white outline-none focus:border-terminal-cyan disabled:cursor-not-allowed disabled:opacity-40"
                value={compareInput}
                onChange={(e) => setCompareInput(e.target.value.toUpperCase())}
                placeholder={canAddMoreCompare ? "Add comparison symbol" : "Max 6 symbols reached"}
              />
              <div className="mt-2 max-h-28 overflow-auto">
                {compareEnabled &&
                  compareMatches.slice(0, 8).map((item) => (
                    <button
                      key={`${item.code}-compare`}
                      className="w-full border-b border-slate-800 px-2 py-2 text-left text-xs hover:bg-slate-800/50"
                      onClick={() => addCompareSymbol(item.code)}
                    >
                      <span className="numeric text-terminal-cyan">{item.code}</span>{" "}
                      <span className="text-slate-400">{item.name}</span>
                    </button>
                  ))}
              </div>

              <div className="mt-2 flex flex-wrap gap-2">
                <span className="numeric rounded border border-terminal-accent bg-terminal-accent/15 px-2 py-1 text-[11px] text-terminal-accent">
                  {primarySymbol} (primary)
                </span>
                {compareSymbols.map((symbol) => (
                  <button
                    key={`chip-${symbol}`}
                    onClick={() => removeCompareSymbol(symbol)}
                    className="numeric rounded border border-terminal-cyan/60 bg-terminal-cyan/10 px-2 py-1 text-[11px] text-terminal-cyan"
                    title="Remove"
                  >
                    {symbol} x
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
                  {compareEnabled ? compareUniverse.join(" vs ") : primarySymbol}
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
            {compareEnabled && compareUniverse.length < 3 ? (
              <div className="rounded border border-terminal-cyan/40 bg-terminal-cyan/10 p-3 text-xs text-terminal-cyan">
                Add symbols until total is at least 3 to unlock compare analytics.
              </div>
            ) : null}

            <MiniLineChart series={chartLines} />

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
              <div className="numeric mt-2 text-[11px] text-slate-500">
                Interval {interval} | window {interval === "1w" ? 52 : 252}
              </div>
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
        symbol={primarySymbol}
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
