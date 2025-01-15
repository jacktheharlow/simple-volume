from typing import List, Tuple

from hummingbot.strategy.conditional_execution_state import RunAlwaysExecutionState
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.volume_making_simple import VolumeMakingSimpleStrategy
from hummingbot.strategy.volume_making_simple.volume_making_simple_config_map import volume_making_simple_config_map


def start(self):
    try:
        exchange = volume_making_simple_config_map.get("connector").value.lower()
        raw_market_trading_pair = volume_making_simple_config_map.get("trading_pair").value
        order_delay_time = volume_making_simple_config_map.get("order_delay_time").value
        safe_volume_making = volume_making_simple_config_map.get("safe_volume_making").value
        volume_maker_order_size = volume_making_simple_config_map.get("volume_maker_order_size").value
        execution_state = RunAlwaysExecutionState()

        try:
            assets: Tuple[str, str] = self._initialize_market_assets(exchange, [raw_market_trading_pair])[0]
        except ValueError as e:
            self.notify(str(e))
            return

        market_names: List[Tuple[str, List[str]]] = [(exchange, [raw_market_trading_pair])]
        self._initialize_markets(market_names)
        maker_data = [self.markets[exchange], raw_market_trading_pair] + list(assets)
        self.market_trading_pair_tuples = [MarketTradingPairTuple(*maker_data)]

        self.strategy = VolumeMakingSimpleStrategy(market_infos=self.market_trading_pair_tuples,
                                                   volume_maker_order_size=volume_maker_order_size,
                                                   order_delay_time=order_delay_time,
                                                   execution_state=execution_state,
                                                   safe_volume_making=safe_volume_making,
                                                   )
    except Exception as e:
        self.notify(str(e))
        self.logger().error("Unknown error during initialization.", exc_info=True)
