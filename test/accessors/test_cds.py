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
from requests.exceptions import HTTPError
from requests.models import Response
from xcube.core.store import DataStoreError

from xcube_multistore.accessors.cds import CdsAccessor

from ..sample_data import get_sample_data_3d


class CdsAccessorTest(unittest.TestCase):

    def setUp(self):
        self.ds_3d = get_sample_data_3d()
        self.accessor = CdsAccessor(MagicMock(), MagicMock(), "era5-land", MagicMock())

    def test_open_data(self):
        self.accessor.store.open_data.return_value = self.ds_3d
        ds = self.accessor.open_data(
            "era5-land", time_range=("2025-01-01", "2025-01-10")
        )
        xr.testing.assert_equal(self.ds_3d, ds)
        self.accessor.store.open_data.assert_called_once()

    def test_open_data_spatial_res(self):
        self.accessor.store.open_data.return_value = self.ds_3d
        ds = self.accessor.open_data(
            "era5-land",
            time_range=("2025-01-01", "2025-01-10"),
            point=(5.0, 40.0),
        )
        self.assertIsInstance(ds, xr.Dataset)
        self.assertCountEqual(["band_1"], ds.data_vars)
        self.assertCountEqual(("time",), ds.dims)
        self.assertEqual([10], [ds.sizes["time"]])

    def test_open_data_splits(self):

        # Fail first call, succeed sequentially on split halves
        def side_effect(*args, **kwargs):
            time_range = kwargs.get("time_range")
            if time_range == ("2025-01-01", "2025-01-10"):
                raise make_403_error("Cost limits exceeded")
            if time_range == ("2025-01-01", "2025-01-05"):
                return self.ds_3d.isel(time=slice(0, 5))
            if time_range == ("2025-01-06", "2025-01-10"):
                return self.ds_3d.isel(time=slice(5, 10))
            raise AssertionError("Unexpected time_range")

        self.accessor.store.open_data.side_effect = side_effect
        ds = self.accessor.open_data(
            "era5-land", time_range=("2025-01-01", "2025-01-10")
        )
        xr.testing.assert_equal(self.ds_3d, ds)

    def test_open_with_split_base_case_error(self):
        # Always raise 403 error
        self.accessor.store.open_data.side_effect = make_403_error()

        with self.assertRaises(DataStoreError) as cm:
            _ = self.accessor.open_data(
                "era5-land",
                time_range=("2025-01-01", "2025-01-10"),
            )

        self.assertIn("Cannot further split time range", str(cm.exception))


def make_403_error(message="Cost limits exceeded"):
    response = Response()
    response.status_code = 403
    response._content = message.encode()  # optional
    return HTTPError(message, response=response)
