export type ToolCategory = "signal" | "filter" | "exit" | "sizing";

export type ToolParamMeta = {
  type?: string;
  required?: boolean;
  default?: unknown;
  enum?: unknown[];
  min?: number;
  max?: number;
};

export type ToolSpec = {
  name: string;
  category: ToolCategory;
  description: string;
  params: Record<string, ToolParamMeta>;
};

export type ToolRegistryResponse = {
  count: number;
  tools: ToolSpec[];
};

export type StrategyStep = {
  tool: string;
  params: Record<string, unknown>;
};

export type StrategySpec = {
  name: string;
  description: string;
  universe: {
    type: "stock" | "index";
    symbols: string[];
    filters: StrategyStep[];
  };
  entry: {
    signals: StrategyStep[];
    combine: "AND" | "OR";
    rank_by: string | null;
    max_signals_per_period: number;
  };
  exit: {
    conditions: StrategyStep[];
    combine: "FIRST_HIT" | "ALL_REQUIRED";
  };
  sizing: StrategyStep;
  execution: {
    initial_capital: number;
    entry_timing: "next_open" | "same_close";
    rebalance: "weekly" | "monthly";
    max_positions: number;
    costs: {
      slippage_bps: number;
      round_trip_pct: number;
    };
  };
  benchmark: string | null;
  date_range: {
    start: string;
    end: string;
  };
};

export type ValidateBacktestResponse = {
  status: "ok";
  spec_format: "full" | "lite";
  strategy_spec: StrategySpec;
  lite_payload: Record<string, unknown>;
};

export type RunBacktestResponse = {
  run_id: string;
  status: "running" | "completed" | "failed";
  spec_format: "full" | "lite";
  status_url?: string;
  result_url?: string;
  trade_count?: number;
  equity_points?: number;
  summary?: Record<string, unknown>;
};

export type BacktestStatusResponse = {
  run_id: string;
  status: "running" | "completed" | "failed" | string;
  is_terminal: boolean;
  created_at: string;
  updated_at: string;
  error_msg: string;
  trade_count: number;
};

export type EquityPoint = {
  date: string;
  cash?: number;
  market_value?: number;
  equity: number;
  open_positions?: number;
};

export type BacktestTrade = {
  run_id?: string;
  trade_index?: number;
  symbol: string;
  entry_date: string;
  exit_date: string;
  entry_price: number;
  exit_price: number;
  shares: number;
  entry_cost: number;
  exit_cost: number;
  pnl: number;
  pnl_pct: number;
  exit_reason: string;
};

export type BacktestDetailResponse = {
  run_id: string;
  status: string;
  created_at: string;
  updated_at: string;
  error_msg: string;
  trade_count: number;
  total_return: number;
  sharpe: number;
  max_drawdown: number;
  spec: StrategySpec | Record<string, unknown>;
  metrics: Record<string, unknown>;
  result: {
    summary?: Record<string, unknown>;
    equity_curve?: EquityPoint[];
    trades?: BacktestTrade[];
    returns?: Record<string, unknown>;
    risk?: Record<string, unknown>;
    ratios?: Record<string, unknown>;
    trade_stats?: Record<string, unknown>;
    monthly_pnl_grid?: Array<Record<string, number | null>>;
    cost_sensitivity?: Array<Record<string, number>>;
    benchmark_comparison?: Record<string, unknown> | null;
    [key: string]: unknown;
  };
};

export type BacktestHistoryRow = {
  run_id: string;
  created_at: string;
  updated_at: string;
  status: string;
  trade_count: number;
  total_return: number;
  sharpe: number;
  max_drawdown: number;
  error_msg: string;
};

export type BacktestHistoryResponse = {
  limit: number;
  offset: number;
  count: number;
  runs: BacktestHistoryRow[];
};

export type BacktestTradesResponse = {
  run_id: string;
  total: number;
  limit: number;
  offset: number;
  trades: BacktestTrade[];
};

export type BacktestEquityResponse = {
  run_id: string;
  total: number;
  limit: number;
  offset: number;
  equity_curve: EquityPoint[];
};

export type BacktestCompareRun = {
  run_id: string;
  status: string;
  created_at: string;
  trade_count: number;
  total_return: number;
  sharpe: number;
  max_drawdown: number;
  metrics: Record<string, unknown>;
};

export type BacktestCompareResponse = {
  run_ids: string[];
  missing_run_ids: string[];
  runs: BacktestCompareRun[];
};

export type BacktestDeleteResponse = {
  run_id: string;
  status: "deleted" | string;
  message: string;
};

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, { cache: "no-store" });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`API ${response.status}: ${text}`);
  }
  return (await response.json()) as T;
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`API ${response.status}: ${text}`);
  }
  return (await response.json()) as T;
}

export async function fetchToolsRegistry(): Promise<ToolSpec[]> {
  const data = await getJson<ToolRegistryResponse>("/api/v1/tools/registry");
  return data.tools ?? [];
}

export async function validateBacktestSpec(strategySpec: StrategySpec): Promise<ValidateBacktestResponse> {
  return postJson<ValidateBacktestResponse>("/api/v1/backtest/validate", { strategy_spec: strategySpec });
}

export async function runBacktest(strategySpec: StrategySpec): Promise<RunBacktestResponse> {
  return postJson<RunBacktestResponse>("/api/v1/backtest/run", { strategy_spec: strategySpec });
}

export async function fetchBacktestStatus(runId: string): Promise<BacktestStatusResponse> {
  return getJson<BacktestStatusResponse>(`/api/v1/backtest/${encodeURIComponent(runId)}/status`);
}

export async function fetchBacktestDetail(runId: string): Promise<BacktestDetailResponse> {
  return getJson<BacktestDetailResponse>(`/api/v1/backtest/${encodeURIComponent(runId)}`);
}

export async function fetchBacktestHistory(limit = 50, offset = 0): Promise<BacktestHistoryResponse> {
  const qs = new URLSearchParams({ limit: String(limit), offset: String(offset) });
  return getJson<BacktestHistoryResponse>(`/api/v1/backtest/history?${qs.toString()}`);
}

export async function fetchBacktestTrades(runId: string, limit = 200, offset = 0): Promise<BacktestTradesResponse> {
  const qs = new URLSearchParams({ limit: String(limit), offset: String(offset) });
  return getJson<BacktestTradesResponse>(`/api/v1/backtest/${encodeURIComponent(runId)}/trades?${qs.toString()}`);
}

export async function fetchBacktestEquity(runId: string, limit = 5000, offset = 0): Promise<BacktestEquityResponse> {
  const qs = new URLSearchParams({ limit: String(limit), offset: String(offset) });
  return getJson<BacktestEquityResponse>(`/api/v1/backtest/${encodeURIComponent(runId)}/equity-curve?${qs.toString()}`);
}

export async function compareBacktestRuns(runIds: string[]): Promise<BacktestCompareResponse> {
  return postJson<BacktestCompareResponse>("/api/v1/backtest/compare", { run_ids: runIds });
}

export async function deleteBacktestRun(runId: string): Promise<BacktestDeleteResponse> {
  const response = await fetch(`${API_BASE}/api/v1/backtest/${encodeURIComponent(runId)}`, { method: "DELETE" });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`API ${response.status}: ${text}`);
  }
  return (await response.json()) as BacktestDeleteResponse;
}
