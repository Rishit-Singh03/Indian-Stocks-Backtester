"use client";

import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";

import {
  type BacktestCompareRun,
  type BacktestDetailResponse,
  type BacktestHistoryRow,
  type BacktestStatusResponse,
  type StrategySpec,
  type ToolCategory,
  type ToolSpec,
  compareBacktestRuns,
  fetchBacktestDetail,
  fetchBacktestEquity,
  fetchBacktestHistory,
  fetchBacktestStatus,
  fetchBacktestTrades,
  fetchToolsRegistry,
  runBacktest,
  validateBacktestSpec,
} from "@/lib/backtest-api";

import { CostSensitivity } from "./CostSensitivity";
import { EquityCurve } from "./EquityCurve";
import { cls, fmtNumber, fmtPercent } from "./format";
import { MetricCards } from "./MetricCards";
import { MonthlyHeatmap } from "./MonthlyHeatmap";
import { RunComparison } from "./RunComparison";
import { TradeLog } from "./TradeLog";

type BuilderStep = { id: string; tool: string; paramsText: string };

const dateBefore = (days: number) => {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString().slice(0, 10);
};

const sid = () => `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

const makeStep = (tool: string, params: Record<string, unknown>): BuilderStep => ({
  id: sid(),
  tool,
  paramsText: JSON.stringify(params, null, 2),
});

const parseObj = (text: string, label: string): Record<string, unknown> => {
  if (!text.trim()) return {};
  const parsed = JSON.parse(text);
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(`${label} must be a JSON object`);
  }
  return parsed as Record<string, unknown>;
};

const defaultsForTool = (tool: ToolSpec | undefined) => {
  const out: Record<string, unknown> = {};
  if (!tool) return out;
  for (const [k, m] of Object.entries(tool.params ?? {})) {
    if (m.default !== undefined) out[k] = m.default;
    else if (Array.isArray(m.enum) && m.enum.length) out[k] = m.enum[0];
  }
  return out;
};

function StepEditor({
  title,
  category,
  steps,
  onChange,
  tools,
}: {
  title: string;
  category: ToolCategory;
  steps: BuilderStep[];
  onChange: (rows: BuilderStep[]) => void;
  tools: ToolSpec[];
}) {
  const add = () => {
    if (!tools.length) return;
    onChange([...steps, makeStep(tools[0].name, defaultsForTool(tools[0]))]);
  };
  const update = (id: string, patch: Partial<BuilderStep>) => onChange(steps.map((s) => (s.id === id ? { ...s, ...patch } : s)));
  const remove = (id: string) => onChange(steps.filter((s) => s.id !== id));
  return (
    <div className="grid-panel p-3">
      <div className="mb-2 flex items-center justify-between">
        <div className="text-[11px] uppercase tracking-[0.14em] text-terminal-cyan">{title}</div>
        <button onClick={add} className="numeric rounded border border-terminal-border px-2 py-1 text-[11px]">
          + Add
        </button>
      </div>
      <div className="space-y-2">
        {steps.map((s, i) => (
          <div key={s.id} className="rounded border border-terminal-border bg-black/30 p-2">
            <div className="mb-2 flex items-center justify-between text-[10px] uppercase tracking-[0.12em] text-slate-500">
              <span>
                {category} #{i + 1}
              </span>
              <button onClick={() => remove(s.id)} disabled={steps.length <= 1} className="rounded border border-terminal-border px-2 py-1 disabled:opacity-40">
                Remove
              </button>
            </div>
            <select
              value={s.tool}
              onChange={(e) => {
                const tool = tools.find((t) => t.name === e.target.value);
                update(s.id, { tool: e.target.value, paramsText: JSON.stringify(defaultsForTool(tool), null, 2) });
              }}
              className="numeric mb-2 w-full rounded border border-terminal-border bg-black/60 px-2 py-2 text-xs"
            >
              {tools.map((t) => (
                <option key={`${category}-${t.name}`} value={t.name}>
                  {t.name}
                </option>
              ))}
            </select>
            <textarea
              rows={4}
              value={s.paramsText}
              onChange={(e) => update(s.id, { paramsText: e.target.value })}
              className="numeric w-full rounded border border-terminal-border bg-black/60 px-2 py-2 text-xs"
            />
          </div>
        ))}
      </div>
    </div>
  );
}

export function BacktestWorkbench() {
  const [tools, setTools] = useState<ToolSpec[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const [name, setName] = useState("Manual Builder Strategy");
  const [symbolsInput, setSymbolsInput] = useState("RELIANCE,TCS,INFY,HDFCBANK");
  const [benchmark, setBenchmark] = useState("SENSEX");
  const [startDate, setStartDate] = useState(dateBefore(365 * 3));
  const [endDate, setEndDate] = useState(dateBefore(1));
  const [entryCombine, setEntryCombine] = useState<"AND" | "OR">("AND");
  const [exitCombine, setExitCombine] = useState<"FIRST_HIT" | "ALL_REQUIRED">("FIRST_HIT");
  const [rankBy, setRankBy] = useState("");
  const [maxSignalsPerPeriod, setMaxSignalsPerPeriod] = useState(5);
  const [initialCapital, setInitialCapital] = useState(1_000_000);
  const [maxPositions, setMaxPositions] = useState(15);
  const [rebalance, setRebalance] = useState<"weekly" | "monthly">("weekly");
  const [slippageBps, setSlippageBps] = useState(30);
  const [costPct, setCostPct] = useState(0.1);
  const [filters, setFilters] = useState<BuilderStep[]>([]);
  const [signals, setSignals] = useState<BuilderStep[]>([makeStep("price_change", { period: "1m", direction: "down", threshold_pct: 8 })]);
  const [exits, setExits] = useState<BuilderStep[]>([makeStep("stop_loss", { stop_loss_pct: 10 })]);
  const [sizing, setSizing] = useState<BuilderStep>(makeStep("fixed_amount", { amount: 50_000 }));
  const [message, setMessage] = useState<string | null>(null);
  const [runId, setRunId] = useState<string | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [history, setHistory] = useState<BacktestHistoryRow[]>([]);
  const [detail, setDetail] = useState<BacktestDetailResponse | null>(null);
  const [equity, setEquity] = useState<Array<{ date: string; equity: number }>>([]);
  const [trades, setTrades] = useState(detail?.result.trades ?? []);
  const [tradeTotal, setTradeTotal] = useState(0);
  const [tradeOffset, setTradeOffset] = useState(0);
  const [compareSelected, setCompareSelected] = useState<string[]>([]);
  const [compareRows, setCompareRows] = useState<BacktestCompareRun[]>([]);
  const pollRef = useRef<number | null>(null);

  const byCategory = useMemo(() => {
    const grouped: Record<ToolCategory, ToolSpec[]> = { signal: [], filter: [], exit: [], sizing: [] };
    tools.forEach((t) => grouped[t.category].push(t));
    return grouped;
  }, [tools]);

  const buildSpec = (): StrategySpec => {
    const symbols = symbolsInput.split(",").map((s) => s.trim().toUpperCase()).filter(Boolean);
    if (!symbols.length) throw new Error("At least one symbol is required.");
    const mapSteps = (rows: BuilderStep[], label: string) =>
      rows.map((r, i) => ({ tool: r.tool, params: parseObj(r.paramsText, `${label}[${i}]`) }));
    return {
      name,
      description: "Built from manual algorithm builder UI.",
      universe: { type: "stock", symbols, filters: mapSteps(filters, "filters") },
      entry: { signals: mapSteps(signals, "signals"), combine: entryCombine, rank_by: rankBy || null, max_signals_per_period: maxSignalsPerPeriod },
      exit: { conditions: mapSteps(exits, "exits"), combine: exitCombine },
      sizing: { tool: sizing.tool, params: parseObj(sizing.paramsText, "sizing") },
      execution: { initial_capital: initialCapital, entry_timing: "next_open", rebalance, max_positions: maxPositions, costs: { slippage_bps: slippageBps, round_trip_pct: costPct } },
      benchmark: benchmark || null,
      date_range: { start: startDate, end: endDate },
    };
  };

  const loadHistory = async () => {
    const h = await fetchBacktestHistory(80, 0);
    setHistory(h.runs ?? []);
  };

  const loadRun = async (id: string) => {
    const [d, t, e] = await Promise.all([fetchBacktestDetail(id), fetchBacktestTrades(id, 40, 0), fetchBacktestEquity(id, 5000, 0)]);
    setDetail(d);
    setTrades(t.trades ?? []);
    setTradeTotal(t.total ?? 0);
    setTradeOffset(0);
    setEquity((e.equity_curve ?? []) as Array<{ date: string; equity: number }>);
  };

  useEffect(() => {
    void (async () => {
      try {
        const list = await fetchToolsRegistry();
        setTools(list);
        const firstFilter = list.find((t) => t.category === "filter");
        if (firstFilter) setFilters([makeStep(firstFilter.name, defaultsForTool(firstFilter))]);
      } catch (e) {
        setErr(e instanceof Error ? e.message : "Failed to load tools");
      }
      await loadHistory();
    })();
  }, []);

  useEffect(() => {
    if (!runId || status !== "running") return;
    const poll = async () => {
      const s = await fetchBacktestStatus(runId);
      setStatus(s.status);
      if (s.is_terminal) {
        if (pollRef.current !== null) window.clearInterval(pollRef.current);
        pollRef.current = null;
        await loadRun(runId);
        await loadHistory();
      }
    };
    void poll();
    pollRef.current = window.setInterval(() => void poll(), 2000);
    return () => {
      if (pollRef.current !== null) window.clearInterval(pollRef.current);
      pollRef.current = null;
    };
  }, [runId, status]);

  const validateNow = async () => {
    try {
      const res = await validateBacktestSpec(buildSpec());
      setMessage(`Validation ok (${res.spec_format}).`);
      setErr(null);
    } catch (e) {
      setErr(e instanceof Error ? e.message : "Validation failed");
    }
  };

  const runNow = async () => {
    try {
      const res = await runBacktest(buildSpec());
      setRunId(res.run_id);
      setStatus(res.status);
      setMessage(`Run submitted: ${res.run_id}`);
      setErr(null);
    } catch (e) {
      setErr(e instanceof Error ? e.message : "Run failed");
    }
  };

  const summary = (detail?.result.summary ?? {}) as Record<string, unknown>;
  const monthlyGrid = (detail?.result.monthly_pnl_grid ?? []) as Array<Record<string, number | null>>;
  const costRows = (detail?.result.cost_sensitivity ?? []) as Array<Record<string, number>>;

  return (
    <main className="min-h-screen bg-terminal-bg text-terminal-text">
      <div className="mx-auto max-w-[1600px] p-4 md:p-6">
        <header className="mb-4 grid-panel p-4">
          <div className="flex flex-col justify-between gap-3 md:flex-row md:items-center">
            <div>
              <div className="text-xs uppercase tracking-[0.18em] text-terminal-cyan">Phase 5 Manual Builder</div>
              <h1 className="mt-1 text-2xl text-white">Backtest Workbench</h1>
            </div>
            <div className="flex items-center gap-2 text-xs">
              <Link href="/" className="numeric rounded border border-terminal-border px-2 py-1">Market Dashboard</Link>
              <span className={cls("numeric rounded border px-2 py-1", status === "running" ? "border-terminal-cyan text-terminal-cyan" : "border-terminal-border text-slate-400")}>Run: {status ?? "idle"}</span>
            </div>
          </div>
        </header>

        <section className="grid grid-cols-1 gap-4 xl:grid-cols-[560px_1fr]">
          <aside className="space-y-4">
            <div className="grid-panel p-4 space-y-2">
              <input value={name} onChange={(e) => setName(e.target.value)} className="numeric w-full rounded border border-terminal-border bg-black/60 px-2 py-2 text-sm" />
              <input value={symbolsInput} onChange={(e) => setSymbolsInput(e.target.value.toUpperCase())} className="numeric w-full rounded border border-terminal-border bg-black/60 px-2 py-2 text-sm" />
              <div className="grid grid-cols-2 gap-2">
                <input type="date" value={startDate} onChange={(e) => setStartDate(e.target.value)} className="numeric rounded border border-terminal-border bg-black/60 px-2 py-2 text-xs" />
                <input type="date" value={endDate} onChange={(e) => setEndDate(e.target.value)} className="numeric rounded border border-terminal-border bg-black/60 px-2 py-2 text-xs" />
              </div>
              <input value={benchmark} onChange={(e) => setBenchmark(e.target.value.toUpperCase())} className="numeric w-full rounded border border-terminal-border bg-black/60 px-2 py-2 text-sm" />
            </div>
            <StepEditor title="Universe Filters" category="filter" steps={filters} onChange={setFilters} tools={byCategory.filter} />
            <StepEditor title="Entry Signals" category="signal" steps={signals} onChange={setSignals} tools={byCategory.signal} />
            <StepEditor title="Exit Conditions" category="exit" steps={exits} onChange={setExits} tools={byCategory.exit} />
            <div className="grid-panel p-4 space-y-2">
              <div className="text-xs uppercase tracking-[0.12em] text-terminal-cyan">Sizing</div>
              <select value={sizing.tool} onChange={(e) => setSizing(makeStep(e.target.value, defaultsForTool(byCategory.sizing.find((t) => t.name === e.target.value))))} className="numeric rounded border border-terminal-border bg-black/60 px-2 py-2 text-xs">
                {byCategory.sizing.map((t) => <option key={`s-${t.name}`} value={t.name}>{t.name}</option>)}
              </select>
              <textarea rows={4} value={sizing.paramsText} onChange={(e) => setSizing({ ...sizing, paramsText: e.target.value })} className="numeric w-full rounded border border-terminal-border bg-black/60 px-2 py-2 text-xs" />
              <div className="grid grid-cols-2 gap-2">
                <select value={entryCombine} onChange={(e) => setEntryCombine(e.target.value as "AND" | "OR")} className="numeric rounded border border-terminal-border bg-black/60 px-2 py-2 text-xs"><option value="AND">AND</option><option value="OR">OR</option></select>
                <select value={exitCombine} onChange={(e) => setExitCombine(e.target.value as "FIRST_HIT" | "ALL_REQUIRED")} className="numeric rounded border border-terminal-border bg-black/60 px-2 py-2 text-xs"><option value="FIRST_HIT">FIRST_HIT</option><option value="ALL_REQUIRED">ALL_REQUIRED</option></select>
                <input value={rankBy} onChange={(e) => setRankBy(e.target.value)} placeholder="rank_by tool" className="numeric rounded border border-terminal-border bg-black/60 px-2 py-2 text-xs" />
                <select value={rebalance} onChange={(e) => setRebalance(e.target.value as "weekly" | "monthly")} className="numeric rounded border border-terminal-border bg-black/60 px-2 py-2 text-xs"><option value="weekly">weekly</option><option value="monthly">monthly</option></select>
                <input type="number" value={maxSignalsPerPeriod} onChange={(e) => setMaxSignalsPerPeriod(Number(e.target.value))} placeholder="max entries" className="numeric rounded border border-terminal-border bg-black/60 px-2 py-2 text-xs" />
                <input type="number" value={maxPositions} onChange={(e) => setMaxPositions(Number(e.target.value))} placeholder="max positions" className="numeric rounded border border-terminal-border bg-black/60 px-2 py-2 text-xs" />
                <input type="number" value={initialCapital} onChange={(e) => setInitialCapital(Number(e.target.value))} placeholder="capital" className="numeric rounded border border-terminal-border bg-black/60 px-2 py-2 text-xs" />
                <input type="number" value={slippageBps} onChange={(e) => setSlippageBps(Number(e.target.value))} placeholder="slippage bps" className="numeric rounded border border-terminal-border bg-black/60 px-2 py-2 text-xs" />
                <input type="number" step={0.01} value={costPct} onChange={(e) => setCostPct(Number(e.target.value))} placeholder="cost %" className="numeric rounded border border-terminal-border bg-black/60 px-2 py-2 text-xs" />
              </div>
              <div className="flex gap-2">
                <button onClick={() => void validateNow()} className="numeric rounded border border-terminal-cyan px-3 py-2 text-xs text-terminal-cyan">Validate Spec</button>
                <button onClick={() => void runNow()} className="numeric rounded border border-terminal-accent px-3 py-2 text-xs text-terminal-accent">Run Backtest</button>
              </div>
              {message ? <div className="text-xs text-terminal-green">{message}</div> : null}
              {err ? <div className="text-xs text-terminal-red">{err}</div> : null}
            </div>
          </aside>

          <section className="space-y-4">
            <div className="grid-panel p-4">
              <div className="text-xs uppercase tracking-[0.14em] text-terminal-cyan">Run History</div>
              <div className="mt-2 space-y-2">
                {history.slice(0, 12).map((h) => (
                  <button key={h.run_id} onClick={() => void loadRun(h.run_id)} className="w-full rounded border border-terminal-border bg-black/20 px-2 py-2 text-left text-xs hover:bg-black/40">
                    <div className="numeric truncate">{h.run_id}</div>
                    <div className="mt-1 text-[11px] text-slate-500">Status {h.status} | Return {fmtPercent(h.total_return)} | Sharpe {fmtNumber(h.sharpe, 3)}</div>
                  </button>
                ))}
              </div>
            </div>

            <RunComparison
              history={history}
              selected={compareSelected}
              onToggle={(id) => setCompareSelected((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : prev.length >= 4 ? prev : [...prev, id]))}
              onCompare={() => void (async () => setCompareRows((await compareBacktestRuns(compareSelected)).runs ?? []))()}
              comparing={false}
              rows={compareRows}
            />

            {detail ? (
              <>
                <MetricCards result={detail.result} />
                <EquityCurve points={equity as Array<{ date: string; equity: number }>} />
                <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
                  <MonthlyHeatmap rows={monthlyGrid} />
                  <CostSensitivity rows={costRows} />
                </div>
                <TradeLog
                  trades={trades}
                  total={tradeTotal}
                  pageSize={40}
                  offset={tradeOffset}
                  onPrev={() => void (async () => {
                    if (!detail?.run_id) return;
                    const nextOffset = Math.max(0, tradeOffset - 40);
                    const data = await fetchBacktestTrades(detail.run_id, 40, nextOffset);
                    setTrades(data.trades ?? []);
                    setTradeTotal(data.total ?? 0);
                    setTradeOffset(nextOffset);
                  })()}
                  onNext={() => void (async () => {
                    if (!detail?.run_id) return;
                    const nextOffset = tradeOffset + 40;
                    const data = await fetchBacktestTrades(detail.run_id, 40, nextOffset);
                    setTrades(data.trades ?? []);
                    setTradeTotal(data.total ?? 0);
                    setTradeOffset(nextOffset);
                  })()}
                />
                <div className="grid-panel p-4 text-xs text-slate-400">
                  Disclaimer: Simulated backtest results. Past performance is not indicative of future returns.
                  <div className="mt-1">Trades: {fmtNumber(summary.trades as number, 0)} | Win rate: {fmtPercent(summary.win_rate_pct as number)}</div>
                </div>
              </>
            ) : (
              <div className="grid-panel p-8 text-center text-sm text-slate-500">Run or select a backtest to view full results.</div>
            )}
          </section>
        </section>
      </div>
    </main>
  );
}
