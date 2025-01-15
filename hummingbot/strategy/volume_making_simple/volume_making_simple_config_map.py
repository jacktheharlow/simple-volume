from decimal import Decimal
from typing import Optional

from hummingbot.client.config.config_validators import validate_bool, validate_decimal, validate_market_trading_pair
from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.settings import AllConnectorSettings, required_exchanges
from hummingbot.logger import HummingbotLogger

logger = HummingbotLogger(__name__)


def validate_exchange_or_derivative(value: str) -> Optional[str]:
    """
    Restrict valid trading modes to spot and derivative
    """
    from hummingbot.client.settings import AllConnectorSettings
    all_exchange_names = AllConnectorSettings.get_exchange_names().union(AllConnectorSettings.get_derivative_names())
    if value not in all_exchange_names:
        return f"Invalid derivative, please choose value from {all_exchange_names}"


def trading_pair_prompt():
    exchange = volume_making_simple_config_map.get("connector").value
    example = AllConnectorSettings.get_example_pairs().get(exchange)
    return "Enter the token trading pair you would like to trade on %s%s >>> " \
        % (exchange, f" (e.g. {example})" if example else "")


def str2bool(value: str):
    return str(value).lower() in ("yes", "y", "true", "t", "1")


# checks if the trading pair is valid
def validate_market_trading_pair_tuple(value: str) -> Optional[str]:
    exchange = volume_making_simple_config_map.get("connector").value
    return validate_market_trading_pair(exchange, value)


def validate_volume_maker_order_side(value: str) -> Optional[str]:
    if value not in {"both", "sell", "buy"}:
        return "Invalid volume maker order side, choose between: both, buy, sell."


volume_making_simple_config_map = {
    "strategy":
        ConfigVar(key="strategy",
                  prompt=None,
                  default="volume_making_simple"),

    "connector":
        ConfigVar(key="connector",
                  prompt="Enter the name of spot or derivative connector >>> ",
                  validator=validate_exchange_or_derivative,
                  on_validated=lambda value: required_exchanges.add(value),
                  prompt_on_new=True),

    "trading_pair":
        ConfigVar(key="trading_pair",
                  prompt=trading_pair_prompt,
                  validator=validate_market_trading_pair_tuple,
                  prompt_on_new=True),

    "safe_volume_making":
        ConfigVar(key="safe_volume_making",
                  prompt="Should the bot only create volume if bid ask volatility is low? (Yes/No) >>> ",
                  type_str="bool",
                  default=False,
                  validator=validate_bool,
                  prompt_on_new=True),

    "volume_maker_order_size":
        ConfigVar(key="volume_maker_order_size",
                  prompt="What is the average order size that should be placed each interval in Quote? "
                         ">>> ",
                  default=Decimal(1.0),
                  type_str="decimal",
                  prompt_on_new=True),

    "order_delay_time":
        ConfigVar(key="order_delay_time",
                  prompt="How many seconds do you want to wait between each individual order?"
                         " (Enter 10 to indicate 10 seconds)? >>> ",
                  type_str="float",
                  default=10,
                  validator=lambda v: validate_decimal(v, 0, inclusive=False),
                  prompt_on_new=True),

}
