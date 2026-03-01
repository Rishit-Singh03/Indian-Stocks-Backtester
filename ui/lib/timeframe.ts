export type TimeframeKey = "1W" | "1M" | "3M" | "6M" | "1Y" | "3Y" | "MAX";

export const TIMEFRAMES: TimeframeKey[] = ["1W", "1M", "3M", "6M", "1Y", "3Y", "MAX"];

export function timeframeDays(key: TimeframeKey): number {
  switch (key) {
    case "1W":
      return 7;
    case "1M":
      return 30;
    case "3M":
      return 90;
    case "6M":
      return 180;
    case "1Y":
      return 365;
    case "3Y":
      return 1095;
    case "MAX":
      return 3650;
    default:
      return 365;
  }
}

export function timeframeInterval(key: TimeframeKey): "1d" | "1w" {
  const _ = key;
  return "1w";
}

export function rangeForTimeframe(key: TimeframeKey): { startDate: string; endDate: string } {
  const days = timeframeDays(key);
  const end = new Date();
  const start = new Date(end);
  start.setDate(start.getDate() - days);
  return {
    startDate: start.toISOString().slice(0, 10),
    endDate: end.toISOString().slice(0, 10),
  };
}
