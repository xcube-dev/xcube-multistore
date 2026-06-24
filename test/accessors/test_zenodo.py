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

from xcube_multistore.accessors.zenodo import ZenodoAccessor

from ..sample_data import get_sample_data_3d


class ZenodoAccessorTest(unittest.TestCase):

    def setUp(self):
        self.ds_3d = get_sample_data_3d()
        memory_store = new_data_store("memory", root="datasource")
        memory_store.write_data(self.ds_3d, "dataset.zarr", replace=True)
        memory_store.cache_store = new_data_store("memory", root="cache_datadource")
        memory_store.cache_store.write_data(
            self.ds_3d, "zenodo_cache/dataset.zarr", replace=True
        )
        storage_store = new_data_store("file", root="data")
        self.accesor = ZenodoAccessor(memory_store, storage_store, "test", MagicMock())

    def test_open_data(self):
        ds = self.accesor.open_data("dataset.zarr")
        self.assertIsInstance(ds, xr.Dataset)
        self.assertCountEqual(["band_1"], ds.data_vars)
        self.assertEqual(
            [10, 3, 3], [ds.sizes["time"], ds.sizes["lat"], ds.sizes["lon"]]
        )

    def test_open_data_cache_store(self):
        ds = self.accesor.open_data("zenodo_cache/dataset.zarr")
        self.assertIsInstance(ds, xr.Dataset)
        self.assertCountEqual(["band_1"], ds.data_vars)
        self.assertEqual(
            [10, 3, 3], [ds.sizes["time"], ds.sizes["lat"], ds.sizes["lon"]]
        )
