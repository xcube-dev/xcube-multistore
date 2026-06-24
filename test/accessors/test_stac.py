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
            spatial_res=0.0001,
        )
        self.assertIsInstance(ds, xr.Dataset)
        self.assertCountEqual(["band_1"], ds.data_vars)
        self.assertCountEqual(("time", "lat", "lon"), ds.dims)
        self.assertEqual(
            [8, 9, 9], [ds.sizes["time"], ds.sizes["lat"], ds.sizes["lon"]]
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
            [4, 9, 9], [ds.sizes["time"], ds.sizes["lat"], ds.sizes["lon"]]
        )
