import random
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass
class random_volume_generator:
    '''
    This code generates a random increase and decrease at x refresh_interval from a specific price. It returns the current increase or decrease PCT that should be applied to the fixed price setting.
    refresh_interval: refresh_interval in seconds between each increase or decrease
    '''
    refresh_interval: int = 600
    volume_maker_factor_range: float = float(0.6)
    volume_maker_max_incremental_difference: float = float(0.2)

    def __post_init__(self):
        self._next_change_timestamp: float = datetime.utcnow().timestamp() + int(self.refresh_interval)
        self._sequence = self.generate_trajectory(48)
        self._current_iteration = 0

    def generate_trajectory(self, n_points: int) -> list:
        trajectory = [0]  # Start at zero
        sum_points = 0  # Running sum

        for _ in range(1, n_points):
            delta = random.uniform((self.volume_maker_max_incremental_difference * -1), self.volume_maker_max_incremental_difference)  # defines the max incremental price increase or decrease
            next_point = trajectory[-1] + delta
            next_point = max(min(next_point, self.volume_maker_factor_range), (self.volume_maker_factor_range * -1))  # sets the boundries of the trajectory
            trajectory.append(next_point)
            sum_points += next_point

        # Adjust to have an average of zero
        average = sum_points / n_points
        trajectory = [x - average for x in trajectory]

        return trajectory

    def get_trajectory_factor(self) -> Decimal:
        if datetime.utcnow().timestamp() > self._next_change_timestamp:
            self._current_iteration += int(1)
            if (len(self._sequence) - self._current_iteration) < 2:
                self._sequence = self._sequence[(self._current_iteration - int(1)):]  # delete the first records of the sequence
                self._sequence += self.generate_trajectory(48)  # generate another 48 records
                self._current_iteration  # reset the sequence number
            self._next_change_timestamp = datetime.utcnow().timestamp() + (self.refresh_interval * random.uniform(0.90, 1.10))
        if (self._sequence is None or len(self._sequence) == 0) or ((len(self._sequence) - self._current_iteration) < 2):
            self._sequence = self.generate_trajectory(48)
            self._current_iteration = 0
        return Decimal(self._sequence[self._current_iteration])
