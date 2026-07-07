# MIT License
#
# Copyright (c) 2025-2026 Brockmann Consult GmbH
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NON INFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import datetime
import time
from collections.abc import Iterable

import xarray as xr
from pystac_client.exceptions import APIError
from requests.exceptions import ConnectionError, Timeout
from urllib3.exceptions import ProtocolError

from xcube_multistore.accessor import Accessor
from xcube_multistore.visualization import GeneratorState

_NB_PIXELS = int(2e4 * 2e4) * 10
_MAX_DAYS = {
    "sentinel-2-l1c": 50,
    "sentinel-2-l2a": 50,
    "sentinel-3-syn-2-syn-ntc": 2,
    "sentinel-3-sl-2-lst-ntc": 2,
    "sentinel-3-synergy-syn-l2-netcdf": 2,
    "sentinel-3-slstr-lst-l2-netcdf": 2,
}

_NUM_BANDS = {
    "sentinel-2-l1c": 13,
    "sentinel-2-l2a": 13,
    "sentinel-3-syn-2-syn-ntc": 25,
    "sentinel-3-sl-2-lst-ntc": 2,
    "sentinel-3-synergy-syn-l2-netcdf": 25,
    "sentinel-3-slstr-lst-l2-netcdf": 2,
}

_RETRYABLE_ERRORS = (
    APIError,
    ConnectionError,
    Timeout,
    ProtocolError,
    ConnectionResetError,
)
_NO_ITEMS_FOUND_PREFIX = "No items found in collection"


class StacAccessor(Accessor):
    """Provides methods for accessing dataset from xcube-cds data store"""

    def open_data(self, data_id: str, **open_params) -> xr.Dataset:
        time_ranges = self._split_time_range(data_id, open_params)
        nb_requests = len(time_ranges)
        temp_paths = [f"{self.identifier}/{i}.zarr" for i in range(nb_requests)]

        self.notify(
            GeneratorState(
                self.identifier,
                message=f"Open dataset {self.identifier!r} 0%.",
            )
        )
        valid_paths = []
        for i, time_range in enumerate(time_ranges):
            params = dict(open_params)
            params["time_range"] = time_range
            path = self._open_and_store_with_retry(data_id, params, temp_paths[i])
            if path is not None:
                valid_paths.append(path)
            self.notify(
                GeneratorState(
                    self.identifier,
                    message=(
                        f"Open dataset {self.identifier!r} "
                        f"{(i+1) / nb_requests * 100:.0f}%."
                    ),
                )
            )

        dss = [self.storage_temp.open_data(path) for path in valid_paths]
        ds = xr.concat(dss, dim="time", combine_attrs="drop_conflicts")
        return ds

    def _open_and_store_with_retry(
        self,
        data_id: str,
        params: dict,
        temp_path: str,
        *,
        max_retries: int = 5,
    ) -> str | None:
        for attempt in range(max_retries):
            try:
                ds = self.store.open_data(data_id, **params)

                if ds is None:
                    self.notify(
                        GeneratorState(
                            self.identifier,
                            message=(
                                f"No items found for time slice "
                                f"{params.get('time_range')}. "
                                "Skipping request."
                            ),
                        )
                    )
                    return None

                self.storage_temp.write_data(ds, temp_path, replace=True)
                return temp_path

            except _RETRYABLE_ERRORS as e:
                if attempt == max_retries - 1:
                    raise

                self.notify(
                    GeneratorState(
                        self.identifier,
                        message=(
                            f"Temporary data access error ({type(e).__name__}). "
                            f"Retrying ({attempt + 1}/{max_retries})..."
                        ),
                    )
                )

                time.sleep(2**attempt)

    @staticmethod
    def _split_time_range(data_id: str, open_params: dict) -> list[tuple[str, str]]:
        start_str, end_str = open_params["time_range"]
        start = datetime.date.fromisoformat(start_str)
        end = datetime.date.fromisoformat(end_str)

        spatial_res = open_params["spatial_res"]
        if not isinstance(spatial_res, Iterable):
            spatial_res = (spatial_res, spatial_res)

        if "bbox" in open_params:
            min_x, min_y, max_x, max_y = open_params["bbox"]
            width = max_x - min_x
            height = max_y - min_y
        else:
            width = height = open_params["bbox_width"]

        nb_pixels_spatial = int(width / spatial_res[0]) * int(height / spatial_res[1])

        nb_vars = len(open_params.get("asset_names", [])) or _NUM_BANDS[data_id]

        pixels_per_day = nb_pixels_spatial * nb_vars
        max_days_by_pixels = max(1, _NB_PIXELS // pixels_per_day)
        max_days = min(_MAX_DAYS[data_id], max_days_by_pixels)

        time_ranges = []
        current = start

        while current <= end:
            chunk_end = min(current + datetime.timedelta(days=max_days - 1), end)
            time_ranges.append(
                (
                    current.strftime("%Y-%m-%d"),
                    chunk_end.strftime("%Y-%m-%d"),
                )
            )
            current = chunk_end + datetime.timedelta(days=1)

        return time_ranges
