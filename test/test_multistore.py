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
import shutil
import unittest

import numpy as np
import pytest
import xarray as xr
from xcube.core.store import new_data_store
from xcube.util.jsonschema import JsonObjectSchema

from xcube_multistore.multistore import MultiSourceDataStore

from .sample_data import (
    get_config_dict0,
    get_config_dict1,
    get_config_dict2,
    get_config_dict3,
    get_config_dict5,
    get_config_dict6,
    get_config_dict7,
    get_config_dict8,
    get_sample_data_2d,
    get_sample_data_3d,
)


class MultiSourceDataStoreTest(unittest.TestCase):

    def setUp(self):
        ds_2d = get_sample_data_2d()
        ds_3d = get_sample_data_3d()
        memory_store = new_data_store("memory", root="datasource")
        memory_store.write_data(ds_2d, "dataset1.zarr", replace=True)
        memory_store.write_data(ds_3d, "dataset3.zarr", replace=True)

    def test_get_config_schema(self):
        schema = MultiSourceDataStore.get_config_schema()
        self.assertIsInstance(schema, JsonObjectSchema)
        self.assertIn("datasets", schema.properties)
        self.assertIn("data_stores", schema.properties)
        self.assertIn("grid_mappings", schema.properties)
        self.assertIn("datasets", schema.required)
        self.assertIn("data_stores", schema.required)

    def test_init_no_visualization(self):
        # test without visualization, but logging
        config_dict = get_config_dict0()
        with self.assertLogs("xcube.multistore", level="INFO") as cm:
            msds = MultiSourceDataStore(config_dict)
        self.assertIsInstance(msds, MultiSourceDataStore)
        self.assertEqual(4, len(cm.output))
        msg = "INFO:xcube.multistore:Dataset 'dataset1' finished."
        self.assertEqual(msg, str(cm.output[-1]))
        ds = msds.stores.storage.open_data("dataset1.nc")
        self.assertIsInstance(ds, xr.Dataset)
        np.testing.assert_almost_equal(
            ds.band_1.values, np.array([[20, 22, 24], [38, 40, 42], [56, 58, 60]])
        )
        msds.stores.storage.delete_data("dataset1.nc")

    def test_init_with_visualization(self):
        # test with visualization as table by setting the field 'visualize' to True
        config_dict = get_config_dict0()
        config_dict["general"]["visualize"] = True
        msds = MultiSourceDataStore(config_dict)
        self.assertIsInstance(msds, MultiSourceDataStore)
        ds = msds.stores.storage.open_data("dataset1.nc")
        self.assertIsInstance(ds, xr.Dataset)
        np.testing.assert_almost_equal(
            ds.band_1.values, np.array([[20, 22, 24], [38, 40, 42], [56, 58, 60]])
        )
        msds.stores.storage.delete_data("dataset1.nc")

    def test_init_without_gridmapping_resampling(self):
        config_dict = get_config_dict1()
        config_dict["general"]["visualize"] = True
        msds = MultiSourceDataStore(config_dict)
        self.assertIsInstance(msds, MultiSourceDataStore)
        ds = msds.stores.storage.open_data("dataset1.nc")
        self.assertIsInstance(ds, xr.Dataset)
        np.testing.assert_almost_equal(ds.band_1.values, np.arange(81).reshape((9, 9)))
        msds.stores.storage.delete_data("dataset1.nc")

    def test_init_with_custom_processing(self):
        config_dict = get_config_dict3()
        config_dict["general"]["visualize"] = True
        msds = MultiSourceDataStore(config_dict)
        self.assertIsInstance(msds, MultiSourceDataStore)
        ds = msds.stores.storage.open_data("dataset1.nc")
        self.assertIsInstance(ds, xr.Dataset)
        np.testing.assert_almost_equal(
            ds.band_1.values, 2 * np.arange(81).reshape((9, 9))
        )
        msds.stores.storage.delete_data("dataset1.nc")

    def test_init_with_point_interpolation(self):
        config_dict = get_config_dict6()
        msds = MultiSourceDataStore(config_dict)
        self.assertIsInstance(msds, MultiSourceDataStore)
        ds = msds.stores.storage.open_data("dataset3.zarr")
        self.assertIsInstance(ds, xr.Dataset)
        np.testing.assert_almost_equal(ds.band_1.values, np.arange(4, 90, 9))
        msds.stores.storage.delete_data("dataset3.zarr")

    @pytest.mark.vcr()
    def test_init_preload(self):
        data_vars = [
            f"annual_disturbances_1985_2023_band_{i}" for i in range(1, 40)
        ] + ["forest_mask"]
        config_dict = get_config_dict5()
        # first run
        msds = MultiSourceDataStore(config_dict)
        self.assertIsInstance(msds, MultiSourceDataStore)
        ds = msds.stores.storage.open_data("senf_andorra.zarr")
        self.assertIsInstance(ds, xr.Dataset)
        self.assertCountEqual([971, 1149], [ds.sizes["y"], ds.sizes["x"]])
        self.assertCountEqual(data_vars, ds.data_vars.keys())
        ds = msds.stores.storage.open_data("biomass_xu.zarr")
        self.assertIsInstance(ds, xr.Dataset)
        self.assertCountEqual(
            [20, 971, 1149], [ds.sizes["time"], ds.sizes["y"], ds.sizes["x"]]
        )
        self.assertCountEqual(["carbon_density"], ds.data_vars.keys())

        # run again, preload and cube generation is skipped
        with self.assertLogs("xcube.multistore", level="INFO") as cm:
            msds = MultiSourceDataStore(config_dict)
        self.assertIsInstance(msds, MultiSourceDataStore)
        self.assertEqual(4, len(cm.output))
        msg = "INFO:xcube.multistore:Data ID 'andorra.zip' already preloaded."
        self.assertEqual(msg, str(cm.output[1]))
        msg = "INFO:xcube.multistore:Dataset 'senf_andorra' already generated."
        self.assertEqual(msg, str(cm.output[2]))
        msg = "INFO:xcube.multistore:Dataset 'biomass_xu' already generated."
        self.assertEqual(msg, str(cm.output[3]))
        ds = msds.stores.storage.open_data("senf_andorra.zarr")
        self.assertIsInstance(ds, xr.Dataset)
        self.assertCountEqual([971, 1149], [ds.sizes["y"], ds.sizes["x"]])
        self.assertCountEqual(data_vars, ds.data_vars.keys())
        msds.stores.storage.delete_data("senf_andorra.zarr")
        ds = msds.stores.storage.open_data("biomass_xu.zarr")
        self.assertIsInstance(ds, xr.Dataset)
        self.assertCountEqual(
            [20, 971, 1149], [ds.sizes["time"], ds.sizes["y"], ds.sizes["x"]]
        )
        self.assertCountEqual(["carbon_density"], ds.data_vars.keys())

        # preload is excluded, since already preloaded
        with self.assertLogs("xcube.multistore", level="INFO") as cm:
            msds = MultiSourceDataStore(config_dict)
        self.assertIsInstance(msds, MultiSourceDataStore)
        self.assertEqual(7, len(cm.output))
        msg = "INFO:xcube.multistore:Data ID 'andorra.zip' already preloaded."
        self.assertEqual(msg, str(cm.output[1]))
        msg = "INFO:xcube.multistore:Dataset 'biomass_xu' already generated."
        self.assertEqual(msg, str(cm.output[-1]))
        ds = msds.stores.storage.open_data("senf_andorra.zarr")
        self.assertIsInstance(ds, xr.Dataset)
        self.assertCountEqual([971, 1149], [ds.sizes["y"], ds.sizes["x"]])
        self.assertCountEqual(data_vars, ds.data_vars.keys())
        msds.stores.storage.delete_data("senf_andorra.zarr")

        # preload is excluded, since already preloaded, with visualization
        config_dict["general"] = {"visualize": True}
        msds = MultiSourceDataStore(config_dict)
        self.assertIsInstance(msds, MultiSourceDataStore)
        ds = msds.stores.storage.open_data("senf_andorra.zarr")
        self.assertIsInstance(ds, xr.Dataset)
        self.assertCountEqual([971, 1149], [ds.sizes["y"], ds.sizes["x"]])
        self.assertCountEqual(data_vars, ds.data_vars.keys())
        msds.stores.storage.delete_data("senf_andorra.zarr")

        # force to preload again, with visualization
        config_dict["general"] = {"force_preload": True, "visualize": True}
        msds = MultiSourceDataStore(config_dict)
        self.assertIsInstance(msds, MultiSourceDataStore)
        ds = msds.stores.storage.open_data("senf_andorra.zarr")
        self.assertIsInstance(ds, xr.Dataset)
        self.assertCountEqual([971, 1149], [ds.sizes["y"], ds.sizes["x"]])
        self.assertCountEqual(data_vars, ds.data_vars.keys())
        ds = msds.stores.storage.open_data("biomass_xu.zarr")
        self.assertIsInstance(ds, xr.Dataset)
        self.assertCountEqual(
            [20, 971, 1149], [ds.sizes["time"], ds.sizes["y"], ds.sizes["x"]]
        )
        self.assertCountEqual(["carbon_density"], ds.data_vars.keys())
        msds.stores.storage.delete_data("senf_andorra.zarr")
        msds.stores.storage.delete_data("biomass_xu.zarr")
        shutil.rmtree(msds.stores.zenodo_senf.cache_store.root)

    def test_init_error(self):
        # with logging, no visualization
        config_dict = get_config_dict2()
        with self.assertLogs("xcube.multistore", level="INFO") as cm:
            msds = MultiSourceDataStore(config_dict)
        self.assertIsInstance(msds, MultiSourceDataStore)
        self.assertEqual(2, len(cm.output))
        msg = (
            "ERROR:xcube.multistore:An error occurred: Failed to open dataset "
            "'datasource/dataset2.zarr': group not found at path ''"
        )
        self.assertEqual(msg, str(cm.output[-1]))

    def test_init_error_process_dataset(self):
        # test without visualization, but logging
        config_dict = get_config_dict7()
        with self.assertLogs("xcube.multistore", level="INFO") as cm:
            msds = MultiSourceDataStore(config_dict)
        self.assertIsInstance(msds, MultiSourceDataStore)
        self.assertEqual(3, len(cm.output))
        msg = "ERROR:xcube.multistore:An error occurred: 'grid1'"
        self.assertEqual(msg, str(cm.output[-1]))

    def test_init_error_write_dataset(self):
        # test without visualization, but logging
        config_dict = get_config_dict8()
        with self.assertLogs("xcube.multistore", level="INFO") as cm:
            msds = MultiSourceDataStore(config_dict)
        self.assertIsInstance(msds, MultiSourceDataStore)
        self.assertEqual(4, len(cm.output))
        msg = (
            "ERROR:xcube.multistore:An error occurred: 'ZenodoDataStore' "
            "object has no attribute 'write_data'"
        )
        self.assertEqual(msg, str(cm.output[-1]))
