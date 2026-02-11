# MIT License
#
# Copyright (c) 2025 Brockmann Consult GmbH
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
from requests.exceptions import HTTPError

import xarray as xr
from xcube.core.store import DataStoreError

from xcube_multistore.accessor import Accessor
from xcube_multistore.visualization import GeneratorState


class CdsAccessor(Accessor):
    """Provides methods for accessing dataset from xcube-cds data store"""

    def open_data(self, data_id: str, **open_params) -> xr.Dataset:
        time_series = "era5" in data_id and "point" in open_params
        if time_series:
            open_params = self._convert_point_to_bbox(data_id, open_params)
            point = open_params.pop("point")
        time_range = open_params.pop("time_range")
        ds = self._open_with_split(data_id, time_range, open_params)

        if time_series:
            # noinspection PyUnboundLocalVariable
            ds = ds.interp(lat=point[1], lon=point[0], method="linear")
        return ds

    def _convert_point_to_bbox(self, data_id: str, open_params: dict):
        lon, lat = open_params["point"]
        if "spatial_res" not in open_params:
            schema = self.store.get_open_data_params_schema(data_id=data_id)
            open_params["spatial_res"] = schema.properties["spatial_res"].minimum
        open_params["bbox"] = [
            lon - 2 * open_params["spatial_res"],
            lat - 2 * open_params["spatial_res"],
            lon + 2 * open_params["spatial_res"],
            lat + 2 * open_params["spatial_res"],
        ]
        return open_params

    def _open_with_split(
        self,
        data_id: str,
        time_range: tuple[str, str],
        open_params: dict,
    ) -> xr.Dataset | None:
        """
        Recursively fetch data by splitting time_range into smaller ranges
        until store.open_data() succeeds.
        """
        try:
            open_params["time_range"] = time_range
            ds = self.store.open_data(data_id, **open_params)
            self.notify(
                GeneratorState(
                    self.identifier,
                    message=(
                        f"Open dataset {self.identifier!r} "
                        f"time range: {open_params['time_range']}"
                    ),
                )
            )
            return ds

        except HTTPError as e:
            # Only split if this is the CDS "request too large" error
            if (
                e.response.status_code == 403
                and "cost limits exceeded" in str(e).lower()
            ):
                # Split the request into two halves
                start, end = open_params["time_range"]
                start = datetime.date.fromisoformat(start)
                end = datetime.date.fromisoformat(end)
                mid = start + (end - start) / 2

                # Base case: prevent infinite recursion if the time range gets too tiny
                if mid <= start or mid >= end:
                    raise DataStoreError(
                        f"Cannot further split time range {start} to {end}: "
                        "minimum interval reached."
                    )

                # Recursively fetch both halves
                time_range_left = (
                    datetime.datetime.strftime(start, "%Y-%m-%d"),
                    datetime.datetime.strftime(mid, "%Y-%m-%d"),
                )
                time_range_right = (
                    datetime.datetime.strftime(
                        mid + datetime.timedelta(days=1), "%Y-%m-%d"
                    ),
                    datetime.datetime.strftime(end, "%Y-%m-%d"),
                )
                left = self._open_with_split(
                    data_id,
                    time_range_left,
                    open_params,
                )
                right = self._open_with_split(
                    data_id,
                    time_range_right,
                    open_params,
                )

                return xr.concat((left, right), dim="time")

            # Not the size-limit error but a HTTPError → propagate it
            raise


def get_timedelta(time_range: tuple[str, str]) -> datetime.timedelta:
    start, end = time_range
    start = datetime.date.fromisoformat(start)
    end = datetime.date.fromisoformat(end)
    return end - start
