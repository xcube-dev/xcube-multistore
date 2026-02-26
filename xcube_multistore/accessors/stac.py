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
from collections.abc import Iterable

import xarray as xr

from xcube_multistore.accessor import Accessor
from xcube_multistore.visualization import GeneratorState

_NB_PIXELS = int(2e4 * 2e4) * 5 * 4
_MAX_DAYS = {
    "sentinel-2-l1c": 100,
    "sentinel-2-l2a": 100,
    "sentinel-3-syn-2-syn-ntc": 7,
    "sentinel-3-sl-2-lst-ntc": 7,
    "sentinel-3-synergy-syn-l2-netcdf": 7,
    "sentinel-3-slstr-lst-l2-netcdf": 7,
}

_NUM_BANDS = {
    "sentinel-2-l1c": 13,
    "sentinel-2-l2a": 13,
    "sentinel-3-syn-2-syn-ntc": 25,
    "sentinel-3-sl-2-lst-ntc": 2,
    "sentinel-3-synergy-syn-l2-netcdf": 25,
    "sentinel-3-slstr-lst-l2-netcdf": 2,
}


class StacAccessor(Accessor):
    """Provides methods for accessing dataset from xcube-cds data store"""

    def open_data(self, data_id: str, **open_params) -> xr.Dataset:
        time_ranges = self._split_time_range(data_id, open_params)
        nb_requests = len(time_ranges)
        self.notify(
            GeneratorState(
                self.identifier,
                message=f"Open dataset {self.identifier!r} 0%.",
            )
        )
        for i, time_range in enumerate(time_ranges):
            open_params["time_range"] = time_range
            ds = self.store.open_data(data_id, **open_params)
            self.storage.write_data(ds, f"stac_temp_{i}.zarr", replace=True)
            self.notify(
                GeneratorState(
                    self.identifier,
                    message=(
                        f"Open dataset {self.identifier!r} "
                        f"{(i+1) / nb_requests * 100:.0f}%."
                    ),
                )
            )

        dss = []
        for i, _ in enumerate(time_ranges):
            dss.append(self.storage.open_data(f"stac_temp_{i}.zarr"))
        ds = xr.concat(dss, dim="time", combine_attrs="drop_conflicts")
        return ds

    @staticmethod
    def _split_time_range(data_id: str, open_params: dict):
        if "asset_names" in open_params:
            nb_vars = len(open_params["asset_names"])
        else:
            nb_vars = _NUM_BANDS[data_id]
        start, end = open_params["time_range"]
        start = datetime.date.fromisoformat(start)
        end = datetime.date.fromisoformat(end)
        total_days = (end - start).days
        nb_ts = total_days // 2
        spatial_res = open_params["spatial_res"]
        if not isinstance(spatial_res, Iterable):
            spatial_res = (spatial_res, spatial_res)
        if "bbox" in open_params:
            bbox = open_params["bbox"]
            nb_pixels_spatial = int((bbox[2] - bbox[0]) / spatial_res[0]) * int(
                (bbox[3] - bbox[1]) / spatial_res[1]
            )
        else:
            bbox_width = open_params["bbox_width"]
            nb_pixels_spatial = int(bbox_width / spatial_res[0]) * int(
                bbox_width / spatial_res[1]
            )
        nb_pixels = nb_pixels_spatial * nb_ts * nb_vars
        nb_splits = nb_pixels // _NB_PIXELS
        if nb_splits == 0:
            nb_splits = 1

        base = total_days // nb_splits
        max_days = _MAX_DAYS[data_id]
        if base > max_days:
            base = _MAX_DAYS[data_id]
            nb_splits = total_days // max_days
        remainder = total_days % nb_splits
        time_ranges = []
        current = start
        for i in range(nb_splits):
            length = base + (1 if i < remainder else 0)
            sub_start = current
            sub_end = current + datetime.timedelta(days=length)
            time_ranges.append(
                (sub_start.strftime("%Y-%m-%d"), sub_end.strftime("%Y-%m-%d"))
            )
            current = sub_end + datetime.timedelta(days=1)
        return time_ranges
