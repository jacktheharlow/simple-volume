import logging
from collections import deque
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from statistics import mean
from typing import Deque

mpb_logger = None


@dataclass
class MovingAveragePriceBand:
    """
    Calculate price moving average and its band (ceiling and floor).
    :param ceiling_pct: the price band ceiling percentage
    :param floor_pct: the price band floor percentage
    :param interval: the price sampling interval
    :param length: the price sample length to calculate moving average
    """

    interval: int = 60
    length: int = 10

    def __post_init__(self):
        self._prices: Deque = deque(maxlen=self.length)
        self._last_update_ts: float = 0

    @classmethod
    def logger(cls):
        global mpb_logger
        if mpb_logger is None:
            mpb_logger = logging.getLogger(__name__)
        return mpb_logger

    def update(self, timestamp: float, price: Decimal) -> None:
        """
        Updates the price band.

        :param timestamp: current timestamp of the strategy/connector
        :param price: reference price to set price band
        """
        self._prices.append(price)
        self._last_update_ts = timestamp

    def check_and_update(self, timestamp: float, price: Decimal) -> None:
        """
        check if the timestamp has passed the defined interval before updating
        :param timestamp: current timestamp of the strategy/connector
        :param price: reference price to set price band
        """
        if timestamp >= self._last_update_ts + self.interval:
            self.update(timestamp, price)

    def calculate_ma(self, ma_length):
        ma_list_respective_length = list(self._prices)[0:ma_length]

        if len(ma_list_respective_length) > 0:

            return Decimal(mean(ma_list_respective_length))
        else:
            return None

    def is_floor_breached(self, ma_length: int, price: Decimal, floor_pct: Decimal) -> bool:
        """
        check if the given price is at or below the floor
        :param price: price to check
        """
        ma_price = self.calculate_ma(ma_length)
        floor_price = ma_price * (Decimal(1) - (floor_pct / Decimal(100)))
        return price <= floor_price

    def is_ceiling_breached(self, ma_length: int, price: Decimal, ceiling_pct: Decimal) -> bool:
        """
        check if the given price is at or above the ceiling
        :param price: price to check
        """
        ma_price = self.calculate_ma(ma_length)
        ceiling_price = ma_price * (Decimal(1) + (ceiling_pct / Decimal(100)))
        try:
            return price >= ceiling_price
        except InvalidOperation as e:
            self.logger().error(f"is_ceiling_breached() error: "
                                f"price = {price} (type {type(price)}), "
                                f"ceiling_price = {ceiling_price} (type {type(ceiling_price)}) "
                                f"{e}")
