export type SearchResult = {
  type: "stock" | "index";
  code: string;
  name: string;
  meta: string;
};

export type IndexSnapshotRow = {
  index_name: string;
  date: string;
  close: number;
  prev_close: number | null;
  abs_change: number | null;
  pct_change: number | null;
};

export type SeriesPoint = {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

export type SeriesResponse = {
  universe: "stock" | "index";
  interval: "1d" | "1w";
  start_date: string;
  end_date: string;
  series: { symbol: string; points: SeriesPoint[] }[];
};

export type CompareSeriesRow = {
  symbol: string;
  base_close: number;
  normalized_base: number;
  period_return_pct: number;
  normalized: { date: string; value: number }[];
};

export type CompareResponse = {
  universe: "stock" | "index" | "mixed";
  interval: "1d" | "1w";
  normalized_base: number;
  start_date: string;
  end_date: string;
  series: CompareSeriesRow[];
};

export type OhlcvResponse = {
  symbol: string;
  universe: "stock" | "index";
  interval: "1d" | "1w";
  start_date: string;
  end_date: string;
  points: SeriesPoint[];
};

export type CorrelationCell = {
  symbol_a: string;
  symbol_b: string;
  correlation: number | null;
  observations: number;
};

export type CorrelationResponse = {
  universe: "stock" | "index" | "mixed";
  interval: "1d" | "1w";
  window: number;
  start_date: string;
  end_date: string;
  matrix: CorrelationCell[];
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

export async function searchSymbols(query: string, limit = 50): Promise<SearchResult[]> {
  const encoded = encodeURIComponent(query);
  const data = await getJson<{ results: SearchResult[] }>(`/api/v1/search?q=${encoded}&limit=${limit}`);
  return data.results ?? [];
}

export async function fetchIndexSnapshot(): Promise<IndexSnapshotRow[]> {
  const data = await getJson<{ items: IndexSnapshotRow[] }>("/api/v1/indexes/snapshot");
  return data.items ?? [];
}

export async function fetchSeries(params: {
  symbols: string[];
  universe: "stock" | "index";
  interval: "1d" | "1w";
  startDate: string;
  endDate: string;
}): Promise<SeriesResponse> {
  const qs = new URLSearchParams({
    symbols: params.symbols.join(","),
    universe: params.universe,
    interval: params.interval,
    start_date: params.startDate,
    end_date: params.endDate,
  });
  return getJson<SeriesResponse>(`/api/v1/series?${qs.toString()}`);
}

export async function fetchCompare(params: {
  symbols: string[];
  universe: "stock" | "index";
  interval: "1d" | "1w";
  normalizedBase?: number;
  startDate: string;
  endDate: string;
}): Promise<CompareResponse> {
  const qs = new URLSearchParams({
    symbols: params.symbols.join(","),
    universe: params.universe,
    interval: params.interval,
    normalized_base: String(params.normalizedBase ?? 100),
    start_date: params.startDate,
    end_date: params.endDate,
  });
  return getJson<CompareResponse>(`/api/v1/compare?${qs.toString()}`);
}

export async function fetchOhlcv(params: {
  symbol: string;
  universe: "stock" | "index";
  interval: "1d" | "1w";
  startDate: string;
  endDate: string;
}): Promise<OhlcvResponse> {
  const symbol = encodeURIComponent(params.symbol.trim().toUpperCase());
  const qs = new URLSearchParams({
    universe: params.universe,
    interval: params.interval,
    start_date: params.startDate,
    end_date: params.endDate,
  });
  return getJson<OhlcvResponse>(`/api/v1/ohlcv/${symbol}?${qs.toString()}`);
}

export async function fetchCorrelation(params: {
  symbols: string[];
  universe: "stock" | "index";
  interval: "1d" | "1w";
  window: number;
  startDate: string;
  endDate: string;
}): Promise<CorrelationResponse> {
  const qs = new URLSearchParams({
    symbols: params.symbols.join(","),
    universe: params.universe,
    interval: params.interval,
    window: String(params.window),
    start_date: params.startDate,
    end_date: params.endDate,
  });
  return getJson<CorrelationResponse>(`/api/v1/correlation?${qs.toString()}`);
}
