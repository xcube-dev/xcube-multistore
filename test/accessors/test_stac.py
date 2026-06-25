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

import unittest
from unittest.mock import MagicMock

import xarray as xr
from xcube.core.store import new_data_store

from xcube_multistore.accessors.stac import StacAccessor

from ..sample_data import get_sample_data_2d


class StacAccessorTest(unittest.TestCase):

    def setUp(self):
        self.ds_2d = get_sample_data_2d()
        self.storage = new_data_store("memory", root="data")
        self.accessor = StacAccessor(MagicMock(), self.storage, "test", MagicMock())

    def test_open_data(self):
        self.accessor.store.open_data.return_value = self.ds_2d
        ds = self.accessor.open_data(
            "sentinel-2-l2a",
            bbox=[9, 54, 11, 56],
            time_range=("2025-01-01", "2025-01-31"),
            spatial_res=0.0002,
        )
        self.assertIsInstance(ds, xr.Dataset)
        self.assertCountEqual(["band_1"], ds.data_vars)
        self.assertCountEqual(("time", "lat", "lon"), ds.dims)
        self.assertEqual(
            [11, 9, 9], [ds.sizes["time"], ds.sizes["lat"], ds.sizes["lon"]]
        )

    def test_open_data_point_request(self):
        self.accessor.store.open_data.return_value = self.ds_2d
        ds = self.accessor.open_data(
            "sentinel-2-l2a",
            point=(10, 55),
            bbox_width=4000,
            time_range=("2020-01-01", "2020-12-31"),
            spatial_res=10,
            asset_names=["B02"],
        )
        self.assertIsInstance(ds, xr.Dataset)
        self.assertCountEqual(["band_1"], ds.data_vars)
        self.assertCountEqual(("time", "lat", "lon"), ds.dims)
        self.assertEqual(
            [8, 9, 9], [ds.sizes["time"], ds.sizes["lat"], ds.sizes["lon"]]
        )

    def test_split_time_range_s2_within_max_days(self):
        time_ranges = StacAccessor._split_time_range(
            "sentinel-2-l2a",
            {
                "time_range": ("2025-01-01", "2025-02-19"),  # 50 days
                "bbox": [9, 54, 11, 56],
                "spatial_res": 0.1,
            },
        )
        self.assertEqual([("2025-01-01", "2025-02-19")], time_ranges)

    def test_split_time_range_s2_exceeds_max_days(self):
        time_ranges = StacAccessor._split_time_range(
            "sentinel-2-l2a",
            {
                "time_range": ("2025-01-01", "2025-03-01"),  # 60 days
                "bbox": [9, 54, 11, 56],
                "spatial_res": 0.1,
            },
        )
        self.assertEqual(
            [
                ("2025-01-01", "2025-02-19"),
                ("2025-02-20", "2025-03-01"),
            ],
            time_ranges,
        )

    def test_split_time_range_s3_uses_two_day_limit(self):
        time_ranges = StacAccessor._split_time_range(
            "sentinel-3-sl-2-lst-ntc",
            {
                "time_range": ("2025-01-01", "2025-01-05"),
                "bbox": [9, 54, 11, 56],
                "spatial_res": 0.1,
            },
        )

        self.assertEqual(
            [
                ("2025-01-01", "2025-01-02"),
                ("2025-01-03", "2025-01-04"),
                ("2025-01-05", "2025-01-05"),
            ],
            time_ranges,
        )

    def test_split_time_range_limited_by_pixel_count(self):
        time_ranges = StacAccessor._split_time_range(
            "sentinel-2-l2a",
            {
                "time_range": ("2025-01-01", "2025-01-10"),
                "bbox": [0, 0, 100000, 100000],
                "spatial_res": 10,
            },
        )

        self.assertEqual(
            [
                ("2025-01-01", "2025-01-03"),
                ("2025-01-04", "2025-01-06"),
                ("2025-01-07", "2025-01-09"),
                ("2025-01-10", "2025-01-10"),
            ],
            time_ranges,
        )
