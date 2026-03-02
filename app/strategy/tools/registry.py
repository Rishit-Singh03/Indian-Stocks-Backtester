from __future__ import annotations

from app.strategy.tools.base import ToolRegistry
from app.strategy.tools.combined_signal import COMBINED_SIGNAL_SPEC, combined_signal
from app.strategy.tools.combined_exit import COMBINED_EXIT_SPEC, combined_exit
from app.strategy.tools.consecutive_moves import CONSECUTIVE_MOVES_SPEC, consecutive_moves_signal
from app.strategy.tools.distance_from_high_low import DISTANCE_FROM_HIGH_LOW_SPEC, distance_from_high_low_signal
from app.strategy.tools.equal_weight_sizing import EQUAL_WEIGHT_SIZING_SPEC, equal_weight_sizing
from app.strategy.tools.fixed_amount_sizing import FIXED_AMOUNT_SIZING_SPEC, fixed_amount_sizing
from app.strategy.tools.index_relative import INDEX_RELATIVE_SPEC, index_relative_signal
from app.strategy.tools.index_membership_filter import INDEX_MEMBERSHIP_FILTER_SPEC, index_membership_filter
from app.strategy.tools.inverse_volatility_sizing import INVERSE_VOLATILITY_SIZING_SPEC, inverse_volatility_sizing
from app.strategy.tools.liquidity_filter import LIQUIDITY_FILTER_SPEC, liquidity_filter
from app.strategy.tools.listing_age_filter import LISTING_AGE_FILTER_SPEC, listing_age_filter
from app.strategy.tools.market_cap_filter import MARKET_CAP_FILTER_SPEC, market_cap_filter
from app.strategy.tools.max_positions_sizing import MAX_POSITIONS_SIZING_SPEC, max_positions_sizing
from app.strategy.tools.mean_reversion_zscore import MEAN_REVERSION_ZSCORE_SPEC, mean_reversion_zscore_signal
from app.strategy.tools.moving_average_crossover import MOVING_AVERAGE_CROSSOVER_SPEC, moving_average_crossover_signal
from app.strategy.tools.price_filter import PRICE_FILTER_SPEC, price_filter
from app.strategy.tools.price_change import PRICE_CHANGE_SPEC, price_change_signal
from app.strategy.tools.relative_strength import RELATIVE_STRENGTH_SPEC, relative_strength_signal
from app.strategy.tools.rsi import RSI_SPEC, rsi_signal
from app.strategy.tools.signal_reversal_exit import SIGNAL_REVERSAL_EXIT_SPEC, signal_reversal_exit
from app.strategy.tools.stop_loss_exit import STOP_LOSS_EXIT_SPEC, stop_loss_exit
from app.strategy.tools.sector_filter import SECTOR_FILTER_SPEC, sector_filter
from app.strategy.tools.target_profit_exit import TARGET_PROFIT_EXIT_SPEC, target_profit_exit
from app.strategy.tools.time_based_exit import TIME_BASED_EXIT_SPEC, time_based_exit
from app.strategy.tools.trailing_stop_exit import TRAILING_STOP_EXIT_SPEC, trailing_stop_exit
from app.strategy.tools.volatility_rank import VOLATILITY_RANK_SPEC, volatility_rank_signal
from app.strategy.tools.volume_spike import VOLUME_SPIKE_SPEC, volume_spike_signal


def build_registry() -> ToolRegistry:
    registry = ToolRegistry()
    registry.register_signal(COMBINED_SIGNAL_SPEC, combined_signal)
    registry.register_signal(PRICE_CHANGE_SPEC, price_change_signal)
    registry.register_signal(MOVING_AVERAGE_CROSSOVER_SPEC, moving_average_crossover_signal)
    registry.register_signal(DISTANCE_FROM_HIGH_LOW_SPEC, distance_from_high_low_signal)
    registry.register_signal(RELATIVE_STRENGTH_SPEC, relative_strength_signal)
    registry.register_signal(VOLUME_SPIKE_SPEC, volume_spike_signal)
    registry.register_signal(CONSECUTIVE_MOVES_SPEC, consecutive_moves_signal)
    registry.register_signal(MEAN_REVERSION_ZSCORE_SPEC, mean_reversion_zscore_signal)
    registry.register_signal(VOLATILITY_RANK_SPEC, volatility_rank_signal)
    registry.register_signal(INDEX_RELATIVE_SPEC, index_relative_signal)
    registry.register_signal(RSI_SPEC, rsi_signal)

    registry.register_filter(LIQUIDITY_FILTER_SPEC, liquidity_filter)
    registry.register_filter(PRICE_FILTER_SPEC, price_filter)
    registry.register_filter(LISTING_AGE_FILTER_SPEC, listing_age_filter)
    registry.register_filter(MARKET_CAP_FILTER_SPEC, market_cap_filter)
    registry.register_filter(INDEX_MEMBERSHIP_FILTER_SPEC, index_membership_filter)
    registry.register_filter(SECTOR_FILTER_SPEC, sector_filter)

    registry.register_exit(TARGET_PROFIT_EXIT_SPEC, target_profit_exit)
    registry.register_exit(STOP_LOSS_EXIT_SPEC, stop_loss_exit)
    registry.register_exit(TIME_BASED_EXIT_SPEC, time_based_exit)
    registry.register_exit(TRAILING_STOP_EXIT_SPEC, trailing_stop_exit)
    registry.register_exit(SIGNAL_REVERSAL_EXIT_SPEC, signal_reversal_exit)
    registry.register_exit(COMBINED_EXIT_SPEC, combined_exit)

    registry.register_sizing(FIXED_AMOUNT_SIZING_SPEC, fixed_amount_sizing)
    registry.register_sizing(EQUAL_WEIGHT_SIZING_SPEC, equal_weight_sizing)
    registry.register_sizing(MAX_POSITIONS_SIZING_SPEC, max_positions_sizing)
    registry.register_sizing(INVERSE_VOLATILITY_SIZING_SPEC, inverse_volatility_sizing)
    return registry


TOOL_REGISTRY = build_registry()
