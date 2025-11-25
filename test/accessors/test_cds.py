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

from xcube.core.store import new_data_store
from xcube.util.jsonschema import JsonObjectSchema,JsonNumberSchema
import xarray as xr
from unittest.mock import patch

from xcube_multistore.accessors.cds import CdsAccessor
from ..sample_data import get_sample_data_3d

import unittest


class CdsAccessorTest(unittest.TestCase):

    def setUp(self):
        ds_3d = get_sample_data_3d()
        memory_store = new_data_store("memory", root="datasource")
        memory_store.write_data(ds_3d, "era5_dataset3.zarr", replace=True)
        self.accesor = CdsAccessor(memory_store)

    def test_open_data(self):
        ds = self.accesor.open_data("era5_dataset3.zarr")
        self.assertIsInstance(ds, xr.Dataset)
        self.assertCountEqual(["band_1"], ds.data_vars)
        self.assertEqual([10, 3, 3], [ds.sizes["time"], ds.sizes["lat"], ds.sizes["lon"]])

    @patch("xcube.core.store.fs.store.BaseFsDataStore.open_data")
    @patch("xcube.core.store.fs.store.BaseFsDataStore.get_open_data_params_schema")
    def test_open_data_spatial_res(self, mock_open_params_schema, mock_open_data):
        mock_open_params_schema.return_value = JsonObjectSchema(
            properties=dict(spatial_res=JsonNumberSchema(minimum=0.1 ))
        )
        mock_open_data.return_value = get_sample_data_3d()
        ds = self.accesor.open_data("era5_dataset3.zarr", point=(5.0, 40.0))
        self.assertIsInstance(ds, xr.Dataset)
        self.assertCountEqual(["band_1"], ds.data_vars)
        self.assertCountEqual(("time",), ds.dims)
        self.assertEqual([10], [ds.sizes["time"]])
