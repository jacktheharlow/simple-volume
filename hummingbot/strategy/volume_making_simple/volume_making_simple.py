import asyncio
import logging
import math
import random as rnd
import statistics
from collections import deque
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

import urllib3

from hummingbot.connector.derivative.position import Position
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.common import PositionAction, PositionMode, PositionSide, PriceType, TradeType
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderExpiredEvent,
    OrderFilledEvent,
    OrderType,
    SellOrderCompletedEvent,
)
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.conditional_execution_state import ConditionalExecutionState, RunAlwaysExecutionState
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.strategy_py_base import StrategyPyBase
from hummingbot.strategy.volume_making_simple.ma_price_band import MovingAveragePriceBand
from hummingbot.strategy.volume_making_simple.volume_maker_randomized_factor import random_volume_generator

logger = None


class VolumeMakingSimpleStrategy(StrategyPyBase):

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global logger
        if logger is None:
            logger = logging.getLogger(__name__)
        return logger

    def __init__(self,
                 market_infos: List[MarketTradingPairTuple],
                 order_delay_time: float = 10.0,
                 execution_state: ConditionalExecutionState = None,
                 cancel_order_wait_time: Optional[float] = 60.0,
                 status_report_interval: float = 900,
                 perpetual_max_position_value: Decimal = Decimal(1000000000000),
                 safe_volume_making: bool = False,
                 volume_maker_ma_price_band_enabled: bool = True,
                 volume_maker_ma_price_band_length: int = int(100),
                 volume_maker_ma_price_band_bound_pct: Decimal = Decimal(1),
                 safe_volume_maker_std: float = 0.0,
                 safe_volume_maker_vol_length: int = 3,
                 volume_maker_order_size: Decimal = 10.0,
                 volume_maker_long_tick_interval: float = float(300),
                 volume_maker_short_tick_interval: float = float(100),
                 volume_maker_short_tick_range: float = float(0.1),
                 volume_maker_price_type: str = str('mid_price'),
                 volume_maker_fixed_price_ticks: Decimal = Decimal(1),
                 volume_maker_small_tick_placement: bool = True,
                 volume_maker_minimum_ticks: Decimal = Decimal(1),
                 volume_maker_rebalance: bool = False,
                 volume_maker_max_spread: Decimal = Decimal(0.03),
                 volume_maker_max_base_order_amount: Decimal = Decimal(1000000000000),
                 volume_maker_price_delegate_acceptable_difference_decimal: Decimal = Decimal(0.02),
                 volume_maker_asset_price_delegate=None,
                 volume_maker_order_side: str = "both",
                 volume_maker_execution_steps: int = int(1),
                 volume_maker_execution_steps_sleeping_time: int = int(15),
                 volume_maker_randomization_factor: float = float(0.5),
                 volume_maker_randomize_volume: bool = False,
                 volume_maker_min_required_balance_quote: Decimal = Decimal(100),
                 volume_maker_force_orderbook_update: bool = True, ):

        if len(market_infos) < 1:
            raise ValueError("market_infos must not be empty.")

        super().__init__()
        self._market_infos = {(market_info.market, market_info.trading_pair): market_info for market_info in market_infos}
        self._execution_state = execution_state or RunAlwaysExecutionState()
        self._cancel_order_wait_time = cancel_order_wait_time if cancel_order_wait_time is not None else 0
        self._volume_maker_min_required_balance_quote = volume_maker_min_required_balance_quote
        self._all_markets_ready = False
        self._place_orders = True
        self._first_order = True
        self._status_report_interval = status_report_interval
        self._time_to_cancel = {}
        self._volume_maker_next_timestamp = 0
        self._last_timestamp = 0
        self._new_price_record_timestamp = 0
        self._cancel_outdated_orders_task = None
        self._last_recorded_mid_price = None
        self._public_buys = Decimal(0)
        self._public_sells = Decimal(0)
        self._orderbook_update_timestamp = float(0)
        self._volume_maker_order_size = volume_maker_order_size
        self._volume_maker_next_timestamp = 0
        self._volume_maker_min_required_balance_quote = volume_maker_min_required_balance_quote
        self._safe_volume_making = safe_volume_making
        self._safe_volume_maker_std = safe_volume_maker_std
        self._volume_maker_short_timer = float(0)
        self._volume_maker_long_timer = float(0)
        self._volume_maker_long_tick_interval = volume_maker_long_tick_interval
        self._volume_maker_short_tick_interval = volume_maker_short_tick_interval
        self._volume_maker_short_tick_range = volume_maker_short_tick_range
        self._volume_maker_price_type = PriceType.MidPrice
        self._volume_maker_fixed_price_ticks = volume_maker_fixed_price_ticks
        self._volume_maker_small_tick_placement = volume_maker_small_tick_placement
        self._volume_maker_minimum_ticks = volume_maker_minimum_ticks
        self._volume_maker_max_spread = volume_maker_max_spread
        self._volume_maker_max_base_order_amount = volume_maker_max_base_order_amount
        self._volume_maker_rebalance: bool = volume_maker_rebalance
        self._volume_maker_price_delegate_acceptable_difference_decimal = volume_maker_price_delegate_acceptable_difference_decimal
        self._volume_maker_asset_price_delegate = volume_maker_asset_price_delegate
        self._volume_maker_order_side = volume_maker_order_side
        self._volume_maker_last_order_side = True
        self._volume_maker_order_side_counter = int(0)
        self._volume_maker_execution_steps = volume_maker_execution_steps
        self._volume_maker_execution_steps_sleeping_time = volume_maker_execution_steps_sleeping_time
        self._volume_maker_randomization_factor = volume_maker_randomization_factor
        self._volume_maker_randomize_volume = volume_maker_randomize_volume
        self._last_volume_maker_price = None
        self._safe_volume_maker_price = None
        self._volume_maker_ma_price_band_enabled = volume_maker_ma_price_band_enabled
        self._volume_maker_ma_price_band_bound_pct = volume_maker_ma_price_band_bound_pct
        self._volume_maker_ma_price_band_length = volume_maker_ma_price_band_length
        self.best_bid_volatility = deque(maxlen=safe_volume_maker_vol_length)
        self.best_ask_volatility = deque(maxlen=safe_volume_maker_vol_length)
        self.volume_maker_filled_check = {}
        self._volume_maker_filled_buys = Decimal(0)
        self._volume_maker_filled_sells = Decimal(0)
        self._public_buys = Decimal(0)
        self._public_sells = Decimal(0)
        self._random_volume_generator = random_volume_generator() if self._volume_maker_randomize_volume else None
        self._volume_maker_executed: Decimal = Decimal(0.0)
        self._first_order = True
        self._volume_making_short_adjustment = Decimal(0)  # todo: make this a parameter?
        self._volume_making_long_adjustment = Decimal(0.25)
        self._volume_maker_force_orderbook_update = volume_maker_force_orderbook_update
        self._is_perpetual = True if self.is_perpetual_market(list(self._market_infos.values())[0]) else False
        self._perpetual_max_position_value = perpetual_max_position_value
        self._order_delay_time = order_delay_time
        self._status_ready = False
        self._ma_price_band = MovingAveragePriceBand(
            interval=60,
            length=volume_maker_ma_price_band_length,
        ) if volume_maker_ma_price_band_enabled else None

        all_markets = set([market_info.market for market_info in market_infos])
        self.add_markets(list(all_markets))

    @property
    def active_bids(self) -> List[Tuple[ExchangeBase, LimitOrder]]:
        return self.order_tracker.active_bids

    @property
    def active_asks(self) -> List[Tuple[ExchangeBase, LimitOrder]]:
        return self.order_tracker.active_asks

    @property
    def active_limit_orders(self) -> List[Tuple[ExchangeBase, LimitOrder]]:
        return self.order_tracker.active_limit_orders

    @property
    def in_flight_cancels(self) -> Dict[str, float]:
        return self.order_tracker.in_flight_cancels

    @property
    def market_info_to_active_orders(self) -> Dict[MarketTradingPairTuple, List[LimitOrder]]:
        return self.order_tracker.market_pair_to_active_orders

    @property
    def place_orders(self):
        return self._place_orders

    def is_perpetual_market(self, market_info: MarketTradingPairTuple) -> bool:
        return True if "perpetual" in market_info.market.name else False

    async def get_mid_price(self, market_info):
        return market_info.get_mid_price()

    @property
    def active_positions(self) -> Dict[str, Position]:
        return list(self._market_infos.values())[0].market.account_positions

    async def format_status(self) -> str:
        lines: list = []
        warning_lines: list = []

        for market_info in self._market_infos.values():

            active_orders = self.market_info_to_active_orders.get(market_info, [])

            warning_lines.extend(self.network_warning([market_info]))

            markets_df = self.market_status_data_frame([market_info])
            lines.extend(["", "  Markets:"] + ["    " + line for line in markets_df.to_string().split("\n")])

            if self._volume_maker_asset_price_delegate:
                lines.extend(["", "  External_market:"] + ["    "] + [
                    f"Mid price {self._volume_maker_asset_price_delegate.market.name}: {self._volume_maker_asset_price_delegate.get_mid_price()} Difference {(self._volume_maker_asset_price_delegate.get_mid_price() - market_info.get_mid_price()) / market_info.get_mid_price()}"])

            if self._ma_price_band:
                lines.extend(["", "  External_market:"] + ["    "] + [
                    f"Ma_price_band 100 {self._ma_price_band.calculate_ma(100)} Difference: {(self._ma_price_band.calculate_ma(100) - market_info.get_mid_price()) / market_info.get_mid_price()} "])

            assets_df = self.wallet_balance_data_frame([market_info])
            lines.extend(["", "  Assets:"] + ["    " + line for line in assets_df.to_string().split("\n")])

            # See if there are any open orders.
            if len(active_orders) > 0:
                price_provider = None
                for market_info in self._market_infos.values():
                    price_provider = market_info
                if price_provider is not None:
                    df = LimitOrder.to_pandas(active_orders, mid_price=float(price_provider.get_mid_price()))

                    df = df.sort_values(by=['Price'], ascending=False)

                    df = df.reset_index(drop=True)
                    df_lines = df.to_string().split("\n")
                    lines.extend(["", "  Active orders:"] +
                                 ["    " + line for line in df_lines])
                    lines.extend([""])
            else:
                lines.extend(["", "  No active maker orders. Or Main market is a DEX"])
                lines.extend([""])

            lines.extend([f"  Volume_maker total executed: {self._volume_maker_executed}"])
            lines.extend([""])
            lines.extend([f"  Volume_maker public buys: {self._public_buys} public sells {self._public_buys}"])
            lines.extend([f"  Volume_maker private buys: {self._volume_maker_filled_buys} private sells {self._volume_maker_filled_sells}"])

            warning_lines.extend(self.balance_warning([market_info]))

        if warning_lines:
            lines.extend(["", "*** WARNINGS ***"] + warning_lines)

        return "\n".join(lines)

    def did_fail_order(self, order_failed_event: MarketOrderFailureEvent):
        """
        Output log for failed order.
        :param order_failed_event: Order failed event
        """
        self.log_with_clock(logging.INFO, f"order: {order_failed_event.order_id} failed.")

    def did_fill_order(self, order_filled_event: OrderFilledEvent):
        """
        Output log for filled order.
        :param order_filled_event: Order filled event
        """
        order_id = order_filled_event.order_id
        # market_info = list(self._market_infos.values())[0]

        self.log_with_clock(logging.INFO,
                            f"({order_filled_event.trading_pair}) Limit {order_filled_event.trade_type.name.lower()} order of "
                            f"{order_filled_event.amount} filled.")

        side = str('buy') if order_filled_event.trade_type is TradeType.BUY else str('sell')

        if side == str('buy'):
            self._volume_maker_filled_buys += Decimal((order_filled_event.amount * order_filled_event.price))
        else:
            self._volume_maker_filled_sells += Decimal((order_filled_event.amount * order_filled_event.price))

        self.volume_maker_filled_check[order_id] = + Decimal((order_filled_event.amount * order_filled_event.price))

    def process_market(self, market_info):
        """
        Checks if enough time has elapsed from previous order to place order and if so, calls self.volume_maker_orders() and
        cancels orders if they are older than self._cancel_order_wait_time.

        :param market_info: a market trading pair
        """
        self.add_indicator_measures(market_info)
        safe_ensure_future(self.volume_maker_logic(market_info))
        self.cancel_orders_above_expiration_limit(market_info)

    def add_indicator_measures(self, market_info):
        self._last_recorded_mid_price = market_info.get_mid_price()
        self.add_bid_ask_volatility_measures(market_info)

        if self._ma_price_band:
            price = market_info.get_mid_price()
            self._ma_price_band.check_and_update(self.current_timestamp, price)

    def should_create_volume(self, market_info):
        # This function checks if:
        # 1. The spread between bid and ask prices is within an acceptable range.
        # 2. The current timestamp is beyond the next allowed timestamp.
        # 3. The mid price is within an acceptable difference from a delegate price (if provided).
        # 4. for perpetual markets, if the max position limit is not reached
        # It returns True if all conditions are met, otherwise False.
        current_bid_price = market_info.market.get_price(market_info.trading_pair, False)
        current_ask_price = market_info.market.get_price(market_info.trading_pair, True)
        mid_price = (current_bid_price + current_ask_price) / Decimal(2)
        spread = (current_ask_price - current_bid_price) / mid_price

        within_acceptable_spread = spread < self._volume_maker_max_spread
        within_acceptable_time_interval = self.current_timestamp > self._volume_maker_next_timestamp

        if not self._volume_maker_asset_price_delegate:
            within_acceptable_price_source_price = True

        else:
            price_delegate_price = self._volume_maker_asset_price_delegate.get_price_by_type(PriceType.MidPrice)
            within_acceptable_price_source_price = abs((price_delegate_price - mid_price) / mid_price) < self._volume_maker_price_delegate_acceptable_difference_decimal

            if not within_acceptable_price_source_price:
                self.logger().info(
                    f"Volume maker: not within price source price of {price_delegate_price} market mid {mid_price}, acceptable difference {self._volume_maker_price_delegate_acceptable_difference_decimal * Decimal(100)} %. Will not create orders")

        if not within_acceptable_spread:
            self.logger().info(f"spread of {spread} is not within max acceptable spread of {self._volume_maker_max_spread}. Will not create orders")

        if not self._volume_maker_ma_price_band_enabled:
            within_ma_price_band = True
        else:
            price = market_info.get_mid_price()
            breached_ceiling = self._ma_price_band.is_ceiling_breached(self._volume_maker_ma_price_band_length, price, self._volume_maker_ma_price_band_bound_pct)
            breached_floor = self._ma_price_band.is_floor_breached(self._volume_maker_ma_price_band_length, price, self._volume_maker_ma_price_band_bound_pct)
            if not breached_ceiling and not breached_floor:
                within_ma_price_band = True
            else:
                within_ma_price_band = False
                self.logger().info(f"Volume maker: price of {price} not within ceiling {breached_ceiling} or floor {breached_floor} will not create order")

        within_max_position_limit = True
        if self._is_perpetual:
            position_long_val = sum(pos.entry_price * pos.amount for pos in self.active_positions.values() if pos.position_side == PositionSide.LONG)
            position_short_val = sum(pos.entry_price * pos.amount for pos in self.active_positions.values() if pos.position_side == PositionSide.SHORT)
            p_net = position_long_val + position_short_val  # this includes positions values in other perp markets

            within_max_position_limit = self._max_position_value > abs(p_net) + self._volume_maker_order_size

            if not within_max_position_limit:
                self.logger().info(f"Volume maker: position value of {abs(p_net)} is greater than max position value of {self._max_position_value}. Will not create order")

        return all([within_acceptable_spread, within_acceptable_time_interval, within_acceptable_price_source_price, within_ma_price_band, within_max_position_limit])

    def set_new_volume_maker_time(self):
        rnd_num = rnd.uniform((self._volume_maker_randomization_factor * -1), self._volume_maker_randomization_factor)
        factor = (1 + rnd_num)
        self._volume_maker_next_timestamp = self.current_timestamp + (self._order_delay_time * factor)
        self.logger().info(
            f"Volume maker: New timestamp set for new cycle. Delay {(self._order_delay_time * factor)} - current timestamp {self.current_timestamp} - next timestamp {self._volume_maker_next_timestamp}")

    async def volume_maker_logic(self, market_info):

        # If current timestamp is greater than the start timestamp and its the first order
        if self._first_order and self.should_create_volume(market_info):
            self.set_new_volume_maker_time()
            self.logger().info("Trying to place orders now. ")
            self.volume_maker_orders(market_info)
            self._first_order = False

        # If current timestamp is greater than the start timestamp + time delay place orders
        elif self.should_create_volume(market_info):
            self.set_new_volume_maker_time()

            if self._volume_maker_force_orderbook_update and hasattr(market_info.market, "force_update_order_book"):  # paper trade exchanges don't have the method for updating the orderbook
                await self.update_orderbook(market_info)

            self.volume_maker_orders(market_info)

    def process_tick(self, timestamp: float):
        """
        Clock tick entry point.
        For the strategy, this function simply checks for the readiness and connection status of markets, and
        then delegates the processing of each market info to process_market().
        """
        if not self._status_ready:
            if all(market_info.market.ready for market_info in self._market_infos.values()):
                self._status_ready = True
            else:
                self.logger().info(f"Some exchanges are not ready: {[market_info.market.name for market_info in self._market_infos.values() if not market_info.market.ready]}. Please wait...")

        for market_info in self._market_infos.values():
            self.process_market(market_info)

    def volume_maker_check_if_success(self, market_info):
        if self.volume_maker_filled_check == {}:
            return

        try:
            if len(list(self.volume_maker_filled_check.keys())) > 1:
                first_order = self.volume_maker_filled_check.get(list(self.volume_maker_filled_check.keys())[0])
                second_order = self.volume_maker_filled_check.get(list(self.volume_maker_filled_check.keys())[1])

                if (first_order == Decimal(0) and second_order != Decimal(0) or first_order != Decimal(0) and second_order == Decimal(0)) or abs(first_order - second_order) > Decimal(10):
                    self.logger().info(f"Previous volume making FAILED, first order filled size {first_order}, second order filled size {second_order}")

                else:
                    self.logger().info(f"Previous volume making SUCCESSFUL, first order filled size {first_order}, "  f"second order filled size {second_order}")

            else:
                self.logger().info("Previous volume making FAILED, only one side filled")

        except Exception as e:
            self.logger().info(f"Error in the volume maker success checker {e}")

        self.volume_maker_filled_check = {}

    def get_volume_maker_price(self, market_info):
        mid_price = market_info.get_mid_price()
        market = market_info.market
        spread_price_step = None

        current_bid_price = market_info.market.get_price(market_info.trading_pair, False)
        current_ask_price = market_info.market.get_price(market_info.trading_pair, True)

        price_quantum = market_info.market.get_order_price_quantum(market_info.trading_pair, mid_price)

        spread_price_steps = ((current_ask_price - current_bid_price) / price_quantum) - Decimal(1)

        if spread_price_steps >= int(self._volume_maker_minimum_ticks):

            # Get the mid of the price steps and add the current long price pct increase to it + round to an int
            spread_price_step = round(Decimal(math.ceil(spread_price_steps / 2) * (Decimal(1) + Decimal(self._volume_making_long_adjustment))), 0)
            # Add the short adjustment to it and round to an int
            spread_price_step = round(Decimal(spread_price_step) + (self._volume_making_short_adjustment * spread_price_steps), 0)
            # Make sure it is not lower than the minimum ticks available -> just to be sure
            spread_price_step = max(Decimal(self._volume_maker_minimum_ticks), spread_price_step)
            # Make sure it is not gigher than the max spread ticks available
            spread_price_step = min(spread_price_steps, spread_price_step)
            # Subtract the ticks from the best ask price
            volume_maker_price = current_ask_price - (spread_price_step * price_quantum)
            # Quantize it
            volume_maker_price = market.quantize_order_price(market_info.trading_pair, Decimal(volume_maker_price))
            # just to be sure, the order cannot cross the book, extra protection
            if (volume_maker_price >= current_ask_price) or (volume_maker_price <= current_bid_price):
                self.logger().info(
                    f"Should not place volume making order as it will cross the book from the start, will adjust the price. volume_maker_price: {volume_maker_price}, current_bid_price {current_bid_price}, current_ask_price {current_ask_price}")
                volume_maker_price = current_ask_price - price_quantum

        else:
            # If there are not enough ticks available, do we still place an order
            if self._volume_maker_small_tick_placement:
                volume_maker_price = mid_price
                volume_maker_price = market.quantize_order_price(market_info.trading_pair, Decimal(volume_maker_price))
                if mid_price == current_bid_price:
                    volume_maker_price = market.quantize_order_price(market_info.trading_pair, Decimal(current_bid_price))
            else:
                # Reduce the self._volume_maker_next_timestamp, so we check again next tick
                self._volume_maker_next_timestamp = self.current_timestamp
                self.logger().info(
                    f" Volume_maker - Ticks too small, mid price {mid_price}, best bid {current_bid_price}, best ask price {current_ask_price}, price quantum {price_quantum}, spread_price_steps {spread_price_steps}, minimum {self._volume_maker_minimum_ticks}")
                self.logger().info(" Volume_maker - Will try again next tick")
                return None

        return volume_maker_price

    def volume_maker_orders(self, market_info):
        """
        places volume maker orders. Logic is as follow:

        1. get random order size within 15% of original size
        2. if enough balance -> conitnue, else buy or sell enough to make a next transaction
        3. calculate how many ticks there are within the spread - there needs to be more than 1 in order for there to be a mid price
        4. we take the mid of all steps and add amultiplier (self._volume_making_long_adjustment) to that - the multipler can be max 50% increase or decrease of the total amount of ticks
        5. make sure the tick at which it should place the order is not larger than the max steps or lower than 1
        6. update the randomizers for the 1 minute and the 5 minute after placing orders
        """

        def log_best_bid_and_ask(best_bid, best_ask):
            best_bid_provided = best_bid
            best_ask_provided = best_ask

            best_bid_now = market_info.market.get_price(market_info.trading_pair, False)
            best_ask_now = market_info.market.get_price(market_info.trading_pair, True)

            self.log_with_clock(logging.INFO, f"Bid (provided / now): {best_bid_provided} / {best_bid_now}"
                                              f"Ask (provided / now): {best_ask_provided} / {best_ask_now}")

        market: ExchangeBase = market_info.market
        curr_order_amount = self._volume_maker_order_size
        # spread_price_step = None

        enough_base, enough_quote = self.has_enough_balance(market_info, curr_order_amount)

        mid_price = market_info.get_mid_price()
        curr_order_amount /= mid_price

        # place_buy_first = False

        self.volume_maker_check_if_success(market_info)

        if self._random_volume_generator:
            factor = Decimal(1) + (self._random_volume_generator.get_trajectory_factor() * (Decimal(1) + Decimal(rnd.uniform(-0.25, 0.25))))

        else:
            factor = Decimal(1) + Decimal(rnd.uniform((self._volume_maker_randomization_factor * -1), self._volume_maker_randomization_factor))

        curr_order_amount = Decimal(curr_order_amount) * Decimal(factor)

        quantized_amount = market.quantize_order_amount(market_info.trading_pair, Decimal(curr_order_amount))

        if quantized_amount != 0:

            quantized_amount = market.quantize_order_amount(market_info.trading_pair, (Decimal(curr_order_amount)))

            if enough_base and enough_quote:
                volume_maker_price = self.get_volume_maker_price(market_info)

                if volume_maker_price:

                    if self._safe_volume_making:
                        # override to make sure that we
                        current_bid_price = market_info.market.get_price(market_info.trading_pair, False)
                        current_ask_price = market_info.market.get_price(market_info.trading_pair, True)

                        if not self.calculate_bid_ask_volatility():
                            self.logger().info("Volatility on bid and ask too large, will try next tick")
                            self._volume_maker_next_timestamp = self.current_timestamp
                            return

                    place_buy_first = self.determine_volume_maker_buy_or_sell()

                    price_quantum = market_info.market.get_order_price_quantum(market_info.trading_pair, mid_price)
                    current_bid_price = market_info.market.get_price(market_info.trading_pair, False)
                    current_ask_price = market_info.market.get_price(market_info.trading_pair, True)

                    self.logger().info(
                        f" Volume_maker - Going to place a sell order of {quantized_amount} at {volume_maker_price}, mid price {mid_price}, best bid {current_bid_price}, best ask price {current_ask_price}, price quantum {price_quantum}, place buy first {place_buy_first}")
                    self.logger().info(
                        f" Volume_maker - Going to place a buy order of {quantized_amount} at {volume_maker_price} , mid price {mid_price}, best bid {current_bid_price}, best ask price {current_ask_price}, price quantum {price_quantum}, place buy first {place_buy_first}")

                    if quantized_amount < self._volume_maker_max_base_order_amount:

                        if place_buy_first:

                            if self._volume_maker_execution_steps == 1:
                                log_best_bid_and_ask(current_bid_price, current_ask_price)
                                buy_order_id = self.buy_with_specific_market(
                                    market_info,
                                    amount=Decimal(quantized_amount),
                                    order_type=OrderType.LIMIT,
                                    price=Decimal(volume_maker_price),
                                    position_action=PositionAction.OPEN
                                )

                                log_best_bid_and_ask(current_bid_price, current_ask_price)
                                sell_order_id = self.sell_with_specific_market(
                                    market_info,
                                    amount=Decimal(quantized_amount),
                                    order_type=OrderType.LIMIT,
                                    price=Decimal(volume_maker_price),
                                    position_action=PositionAction.OPEN
                                )

                                self._time_to_cancel[buy_order_id] = self.current_timestamp + self._cancel_order_wait_time
                                self._time_to_cancel[sell_order_id] = self.current_timestamp + self._cancel_order_wait_time

                                self.volume_maker_filled_check[buy_order_id] = Decimal(0)
                                self.volume_maker_filled_check[sell_order_id] = Decimal(0)

                            else:
                                safe_ensure_future(
                                    self.place_volume_maker_split_orders(market_info=market_info, buy_first=place_buy_first, amount=Decimal(quantized_amount), price=Decimal(volume_maker_price)))

                            self._volume_maker_last_order_side = True

                            self._volume_maker_order_side_counter = self._volume_maker_order_side_counter + 1

                        else:

                            if self._volume_maker_execution_steps == 1:
                                log_best_bid_and_ask(current_bid_price, current_ask_price)
                                sell_order_id = self.sell_with_specific_market(
                                    market_info,
                                    amount=Decimal(quantized_amount),
                                    order_type=OrderType.LIMIT,
                                    price=Decimal(volume_maker_price),
                                    position_action=PositionAction.OPEN
                                )

                                log_best_bid_and_ask(current_bid_price, current_ask_price)
                                buy_order_id = self.buy_with_specific_market(
                                    market_info,
                                    amount=Decimal(quantized_amount),
                                    order_type=OrderType.LIMIT,
                                    price=Decimal(volume_maker_price),
                                    position_action=PositionAction.OPEN
                                )

                                self._volume_maker_last_order_side = False
                                self._volume_maker_order_side_counter = self._volume_maker_order_side_counter + 1

                                self._time_to_cancel[buy_order_id] = self.current_timestamp + self._cancel_order_wait_time
                                self._time_to_cancel[sell_order_id] = self.current_timestamp + self._cancel_order_wait_time

                                self.volume_maker_filled_check[buy_order_id] = Decimal(0)
                                self.volume_maker_filled_check[sell_order_id] = Decimal(0)

                            else:
                                safe_ensure_future(
                                    self.place_volume_maker_split_orders(market_info=market_info, buy_first=place_buy_first, amount=Decimal(quantized_amount), price=Decimal(volume_maker_price)))

                        self._volume_maker_executed += Decimal(quantized_amount * volume_maker_price)

                    else:

                        self.logger().info(
                            f" Volume_maker - proposed volume maker order size of {quantized_amount} tokens is higher than the volume_maker_max_base_order_amount of {self._volume_maker_max_base_order_amount}. Volume will not be created.")

                    self.update_volume_maker_timers()
            else:
                # Not enough balance, so we should rebalance if we allow for it. This logic is not optimized as we place orders at a fixed 2% spread to cross the book, not configurable
                if self._volume_maker_rebalance:
                    self.logger().info(f"Enough base {enough_base} enough quote {enough_quote} will rebalance")

                    if not enough_base:
                        mid_price = market_info.get_mid_price()

                        buy_price = mid_price * Decimal(1.02)

                        quantized_buy_price = market.quantize_order_price(market_info.trading_pair, Decimal(buy_price))
                        order_id = self.buy_with_specific_market(
                            market_info,
                            amount=quantized_amount,
                            order_type=OrderType.LIMIT,
                            price=quantized_buy_price,
                            position_action=PositionAction.OPEN
                        )
                        self._time_to_cancel[order_id] = self.current_timestamp + self._cancel_order_wait_time

                    else:
                        mid_price = market_info.get_mid_price()

                        sell_price = mid_price * Decimal(0.98)

                        quantized_sell_price = market.quantize_order_price(market_info.trading_pair, Decimal(sell_price))
                        order_id = self.sell_with_specific_market(
                            market_info,
                            amount=quantized_amount,
                            order_type=OrderType.LIMIT,
                            price=quantized_sell_price,
                            position_action=PositionAction.OPEN
                        )
                        self._time_to_cancel[order_id] = self.current_timestamp + self._cancel_order_wait_time
                else:
                    self.logger().info(f"Not enough Balance, will not rebalance because feature is turned off, check again next tick, Enough base: {enough_base} Enough quote: {enough_quote}")
                    self._volume_maker_next_timestamp = self.current_timestamp
        else:
            self.logger().warning("Order amount is to small, will not create volume.")

    def determine_volume_maker_buy_or_sell(self):

        if self._volume_maker_order_side == "sell":
            return False

        if self._volume_maker_order_side == "buy":
            return True

        else:
            if self._volume_maker_order_side_counter > int(3):
                return False

            if self._volume_maker_order_side_counter < int(-3):
                return True

            else:
                buy_sell = int(rnd.randint(0, 1))
                return True if buy_sell == int(1) else False

    async def place_volume_maker_split_orders(self, market_info, buy_first: bool, amount: Decimal, price: Decimal):
        amount_left = amount
        step_size = amount / self._volume_maker_execution_steps
        step_size_oid = []

        self.logger().info(f"Volume maker in order steps: Going to place buy first: {buy_first} in {self._volume_maker_execution_steps} orders to fill {amount} orders at {step_size}")

        if buy_first:
            first_order_id = self.buy_with_specific_market(market_info,
                                                           amount=Decimal(amount),
                                                           order_type=OrderType.LIMIT,
                                                           price=Decimal(price),
                                                           position_action=PositionAction.OPEN)
        else:
            first_order_id = self.sell_with_specific_market(market_info,
                                                            amount=Decimal(amount),
                                                            order_type=OrderType.LIMIT,
                                                            price=Decimal(price),
                                                            position_action=PositionAction.OPEN)

        self._time_to_cancel[first_order_id] = self.current_timestamp + self._cancel_order_wait_time

        self.logger().info("Volume maker in order steps: buy order placed")

        for step_size_order in range(0, self._volume_maker_execution_steps):

            limit_order_record = self.order_tracker.get_limit_order(market_info, first_order_id)

            self.logger().info(f"Volume maker in order steps: Order step {step_size_order}, amount left {amount_left}")

            if (amount_left > Decimal(0)) and limit_order_record:

                if step_size_order == range(0, self._volume_maker_execution_steps):
                    filling_order_amount = amount_left
                else:
                    filling_order_amount = step_size * (Decimal(1) + Decimal(rnd.uniform(-0.20, 0.20)))

                if buy_first:

                    filling_order_id = self.sell_with_specific_market(market_info,
                                                                      amount=Decimal(filling_order_amount),
                                                                      order_type=OrderType.LIMIT,
                                                                      price=Decimal(price),
                                                                      position_action=PositionAction.OPEN)
                else:
                    filling_order_id = self.buy_with_specific_market(market_info,
                                                                     amount=Decimal(filling_order_amount),
                                                                     order_type=OrderType.LIMIT,
                                                                     price=Decimal(price),
                                                                     position_action=PositionAction.OPEN)

                step_size_oid.append(filling_order_id)

                self._time_to_cancel[filling_order_id] = self.current_timestamp + self._cancel_order_wait_time

                amount_left = amount_left - filling_order_amount

                sleeping_time = self._volume_maker_execution_steps_sleeping_time * (Decimal(1) + Decimal(rnd.uniform(-0.20, 0.20)))

                self.logger().info(f"Volume maker in order steps: amount left after {amount_left}, sleeping for {sleeping_time} seconds")

            else:
                self.logger().info(f"Volume maker in order steps: did not place a filling order. Limit order record {limit_order_record}, amount left {amount_left}")

            await asyncio.sleep(float(sleeping_time))

        if self.order_tracker.get_limit_order(market_info, first_order_id):
            self.logger().info("Volume maker in order steps: had to cancel the buy order as it was not fully filled")
            self.cancel_order(market_info, first_order_id)
        else:
            self.logger().info("Volume maker in order steps: did not have to cancel the volume maker order")

        for order_id in step_size_oid:
            if self.order_tracker.get_limit_order(market_info, order_id):
                self.logger().info("Volume maker in order steps: had to cancel the sell orders as they were not fully filled")
                self.cancel_order(market_info, order_id)
                self.logger().info("Volume maker in order steps: did not have to cancel the volume maker filling orders")

    def has_enough_balance(self, market_info, amount: Decimal):
        """
        Checks to make sure the user has the sufficient balance in order to place the specified order

        :param market_info: a market trading pair tuple
        :param amount: the amount to check
        :return: a tuple of booleans indicating if the user has enough balance in the base and quote assets
        """
        market: ExchangeBase = market_info.market
        base_asset_balance = market.get_available_balance(market_info.base_asset)
        quote_asset_balance = market.get_available_balance(market_info.quote_asset)

        if not self._is_perpetual:
            mid_price = market_info.get_mid_price()
            enough_base = ((base_asset_balance * mid_price) - self._volume_maker_min_required_balance_quote) > amount
            enough_quote = (quote_asset_balance - self._volume_maker_min_required_balance_quote) > amount
            return enough_base, enough_quote
        else:
            enough_quote = ((quote_asset_balance / Decimal("2")) - self._volume_maker_min_required_balance_quote) > amount
            return enough_quote, enough_quote

    def update_volume_maker_timers(self):
        if self.current_timestamp > self._volume_maker_short_timer:
            self._volume_maker_short_timer = self.current_timestamp + float(self._volume_maker_short_tick_interval)
            self._volume_making_short_adjustment = Decimal(rnd.uniform((self._volume_maker_short_tick_range * -1), self._volume_maker_short_tick_range))

        if self.current_timestamp > self._volume_maker_long_timer:
            self._volume_maker_long_timer = (self.current_timestamp + (float(self._volume_maker_long_tick_interval) * rnd.uniform(-0.4, 0.4)))
            self._volume_making_long_adjustment = Decimal(rnd.uniform(-0.45, 0.45))

    def calculate_bid_ask_volatility(self):
        bid_volatility = statistics.stdev(self.best_bid_volatility) if len(self.best_bid_volatility) > 1 else Decimal(0)
        ask_volatility = statistics.stdev(self.best_ask_volatility) if len(self.best_ask_volatility) > 1 else Decimal(0)
        self.logger().info(f"Volume_maker volatility bid: {bid_volatility} ask {ask_volatility}")
        return (bid_volatility <= self._safe_volume_maker_std) and (ask_volatility <= self._safe_volume_maker_std)

    def add_bid_ask_volatility_measures(self, market_info):
        market: ExchangeBase = market_info.market
        current_bid_price = market.get_price(market_info.trading_pair, False)
        current_ask_price = market.get_price(market_info.trading_pair, True)
        self.best_bid_volatility.append(current_bid_price)
        self.best_ask_volatility.append(current_ask_price)

    def start(self, clock: Clock, timestamp: float):
        super().start(clock, timestamp)
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.logger().info("Strategy started")
        self._last_timestamp = timestamp

        market_info = list(self._market_infos.values())[0]
        if self._is_perpetual:
            market_info.market.set_leverage(market_info.trading_pair, 1)
            market_info.market.set_position_mode(PositionMode.ONEWAY)

    def did_order_book_trade(self, event_tag, order_book, event):

        side = str('buy') if event.type is TradeType.BUY else str('sell')

        if side == str('buy'):
            self._public_buys += Decimal((event.amount * event.price))
        else:
            self._public_sells += Decimal((event.amount * event.price))

    def did_complete_buy_order(self, order_completed_event: BuyOrderCompletedEvent):
        """
        Output log for completed buy order.

        :param order_completed_event: Order completed event
        """
        order_id: str = order_completed_event.order_id

        self.log_with_clock(logging.INFO, f"Buy order completed {order_id}")
        self.notify_hb_app_with_timestamp(f"Bought {order_completed_event.base_asset_amount:.8f} at {(order_completed_event.quote_asset_amount / order_completed_event.base_asset_amount)} "
                                          f"{order_completed_event.base_asset}-{order_completed_event.quote_asset}")

    def did_complete_sell_order(self, order_completed_event: SellOrderCompletedEvent):
        """
        Output log for completed sell order.

        :param order_completed_event: Order completed event
        """
        order_id: str = order_completed_event.order_id

        self.log_with_clock(logging.INFO, f"Sell order completed {order_id}")
        self.notify_hb_app_with_timestamp(f"Sold {order_completed_event.base_asset_amount:.8f} at {(order_completed_event.quote_asset_amount / order_completed_event.base_asset_amount)} "
                                          f"{order_completed_event.base_asset}-{order_completed_event.quote_asset}")

    def did_cancel_order(self, cancelled_event: OrderCancelledEvent):
        """
        Output log for cancelled order.

        :param cancelled_event: Order cancelled event
        """
        self.update_remaining_after_removing_order(cancelled_event.order_id, 'cancel')
        try:
            del self._time_to_cancel[cancelled_event.order_id]
        except Exception:
            self.log_with_clock(logging.INFO, "order_id already removed from cancel timer dictionary")

    def did_expire_order(self, expired_event: OrderExpiredEvent):
        """
        Runs update_remaining_after_removing_order() after an order has expired.

        :param expired_event: Order expired event
        """
        self.update_remaining_after_removing_order(expired_event.order_id, 'expire')

    def update_remaining_after_removing_order(self, order_id: str, event_type: str):
        """
        Logs the event type

        :param order_id: Order ID
        :param event_type: Event type
        """
        market_info = self.order_tracker.get_market_pair_from_order_id(order_id)

        if market_info is not None:
            limit_order_record = self.order_tracker.get_limit_order(market_info, order_id)
            if limit_order_record is not None:
                self.log_with_clock(logging.INFO, f"Updating status after order {event_type} (id: {order_id})")

    async def update_orderbook(self, market_info):
        if self.current_timestamp > self._orderbook_update_timestamp:
            await market_info.market.force_update_order_book(market_info.trading_pair)
            self._orderbook_update_timestamp = self.current_timestamp + float(5)

    def cancel_orders_above_expiration_limit(self, market_info):
        """
        Cancels orders above the expiration limit

        :param market_info: a market trading pair tuple
        """
        active_orders = self.market_info_to_active_orders.get(market_info, [])
        active_order_ids = [order.client_order_id for order in active_orders]

        time_to_cancel_keys = self._time_to_cancel.copy().keys()
        for cancel_id in time_to_cancel_keys:
            if cancel_id not in active_order_ids:
                del self._time_to_cancel[cancel_id]

        orders_to_cancel = []
        for active_order in active_orders:
            try:
                if self.current_timestamp >= self._time_to_cancel[active_order.client_order_id]:
                    orders_to_cancel.append(active_order)
            except Exception:
                orders_to_cancel.append(active_order)

        orders_to_cancel = set(orders_to_cancel)

        for order in orders_to_cancel:
            if order not in self.order_tracker.in_flight_pending_created:
                self.cancel_order(market_info, order.client_order_id)

                if order.client_order_id not in active_order_ids:
                    try:
                        del self._time_to_cancel[order.client_order_id]
                    except Exception:
                        self.logger().info(f"Could not delete {order.client_order_id} from the time_to_cancel_dict")

        for order_id in active_order_ids:
            if order_id not in time_to_cancel_keys:
                self.logger().info(f"order not in active order ids {order_id}")
                self.cancel_order(market_info, order_id)

    def tick(self, timestamp: float):
        """
        Clock tick entry point.
        For the strategy, this function simply checks for the readiness and connection status of markets, and
        then delegates the processing of each market info to process_market().

        :param timestamp: current tick timestamp
        """

        self._last_timestamp = timestamp
        current_tick = timestamp // self._status_report_interval
        last_tick = (self._last_timestamp // self._status_report_interval)
        should_report_warnings = current_tick > last_tick
        if should_report_warnings and not all([market.network_status is NetworkStatus.CONNECTED for market in self.active_markets]):
            self.logger().warning("WARNING: Some markets are not connected or are down at the moment. Market making may be dangerous when markets or networks are unstable.")

        if not self._all_markets_ready:
            for market in self.active_markets:
                self.logger().info(f"{market.name} Not Ready yet: {market.status_dict}")

            self._all_markets_ready = all([market.ready for market in self.active_markets])
            if not self._all_markets_ready:
                # Markets not ready yet. Don't do anything.
                if should_report_warnings:
                    self.logger().warning("Markets are not ready. No market making trades are permitted.")
                return

        self._execution_state.process_tick(timestamp, self)
