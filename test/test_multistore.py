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
from io import StringIO
from unittest.mock import patch
from collections.abc import Container, Iterator
from typing import Any

import numpy as np
import pytest
import xarray as xr
from xcube.core.store import new_data_store
from xcube.util.jsonschema import JsonObjectSchema, JsonStringSchema
from xcube.core.store import DataDescriptor, DataStore, DataTypeLike

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
    get_config_dict9,
)


class DummyStore(DataStore):
    """Minimal stub for a data store"""

    @classmethod
    def get_data_store_params_schema(cls) -> JsonObjectSchema:
        return JsonObjectSchema(properties={"key": JsonStringSchema()})

    @classmethod
    def get_data_types(cls) -> tuple[str, ...]:
        return ("dataset",)

    def get_data_types_for_data(self, data_id: str) -> tuple[str, ...]:
        return ("dataset",)

    def get_data_ids(
        self,
        data_type: DataTypeLike = None,
        include_attrs: Container[str] | bool = False,
    ) -> Iterator[str] | Iterator[tuple[str, dict[str, Any]]]:
        return iter(["data1", "data2"])

    def has_data(self, data_id: str, data_type: DataTypeLike = None) -> bool:
        return data_id in self.list_data_ids()

    def describe_data(
        self, data_id: str, data_type: DataTypeLike = None
    ) -> DataDescriptor:
        return DataDescriptor(data_id=data_id, data_type="dataset")

    def get_data_opener_ids(
        self, data_id: str = None, data_type: DataTypeLike = None
    ) -> tuple[str, ...]:
        return ("dataset:dummy:file",)

    def get_open_data_params_schema(
        self, data_id: str = None, opener_id: str = None
    ) -> JsonObjectSchema:
        return JsonObjectSchema(properties={"key": JsonStringSchema()})

    def open_data(self, data_id: str, opener_id: str = None, **open_params) -> Any:
        return xr.Dataset()

    def search_data(
        self, data_type: DataTypeLike = None, **search_params
    ) -> Iterator[DataDescriptor]:
        return iter(
            [
                DataDescriptor(data_id="data1", data_type="dataset"),
                DataDescriptor(data_id="data2", data_type="dataset"),
            ]
        )

    @classmethod
    def get_search_params_schema(
        cls, data_type: DataTypeLike = None
    ) -> JsonObjectSchema:
        return JsonObjectSchema(properties={"key": JsonStringSchema()})


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

    @patch("sys.stdout", new_callable=StringIO)
    def test_display_config(self, mock_stdout):
        config = get_config_dict0()
        MultiSourceDataStore.display_config(config)
        output = mock_stdout.getvalue()
        self.assertIn("dataset1", output)
        self.assertIn("memory", output)
        self.assertIn("spatial_res", output)

    @patch("xcube_multistore.multistore.list_data_store_ids")
    def test_list_data_store_ids(self, mock_list_ids):
        mock_list_ids.return_value = ["file"]
        self.assertCountEqual(["file"], MultiSourceDataStore.list_data_store_ids())

    @patch("xcube_multistore.multistore.list_data_store_ids")
    def test_get_data_store_params_schema(self, mock_list_ids):
        mock_list_ids.return_value = ["file", "memory"]
        schema = MultiSourceDataStore.get_data_store_params_schema()
        self.assertIn("file", schema.properties)
        self.assertIn("memory", schema.properties)
        self.assertNotIn("cmems", schema.properties)

        schema = MultiSourceDataStore.get_data_store_params_schema(
            data_store_ids="file"
        )
        self.assertIn("file", schema.properties)
        self.assertNotIn("memory", schema.properties)

        schema = MultiSourceDataStore.get_data_store_params_schema(
            data_store_ids=["file"]
        )
        self.assertIn("file", schema.properties)
        self.assertNotIn("memory", schema.properties)

    @patch("xcube_multistore.multistore.new_data_store")
    def test_list_data_ids(self, mock_data_store):
        mock_data_store.return_value = DummyStore()
        data_ids = MultiSourceDataStore.list_data_ids({"file": {}})
        self.assertCountEqual(["data1", "data2"], data_ids.properties["file"].enum)
        self.assertIsInstance(data_ids, JsonObjectSchema)

    @patch("xcube_multistore.multistore.new_data_store")
    def test_get_open_data_params_schema(self, mock_data_store):
        mock_data_store.return_value = DummyStore()
        schema = MultiSourceDataStore.get_open_data_params_schema("file", {}, "data1")
        self.assertIsInstance(schema, JsonObjectSchema)
        self.assertIn("key", schema.properties)
        self.assertNotIn("key1", schema.properties)

    @patch("xcube_multistore.multistore.new_data_store")
    def test_search_data_ids(self, mock_data_store):
        mock_data_store.return_value = DummyStore()
        data_ids = MultiSourceDataStore.search_data_ids({"file": ({}, {})})
        self.assertCountEqual(["data1", "data2"], data_ids.properties["file"].enum)

    @patch("xcube_multistore.multistore.new_data_store")
    def test_get_search_params_schema(self, mock_data_store):
        mock_data_store.return_value = DummyStore()
        schema = MultiSourceDataStore.get_search_params_schema({"file": {}})
        self.assertIsInstance(schema, JsonObjectSchema)
        self.assertIn("key", schema.properties["file"].properties)
        self.assertNotIn("key1", schema.properties["file"].properties)

    @patch("xcube_multistore.multistore.new_data_store")
    def test_describe_data(self, mock_data_store):
        mock_data_store.return_value = DummyStore()
        descriptor = MultiSourceDataStore.describe_data("file", {}, "data1")
        self.assertEqual("data1", descriptor.data_id)
        self.assertEqual("dataset", str(descriptor.data_type))

    def test_init_no_visualization(self):
        # test without visualization, but logging
        config_dict = get_config_dict0()
        with self.assertLogs("xcube.multistore", level="INFO") as cm:
            msds = MultiSourceDataStore(config_dict)
        print(cm.output)
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
        print(cm.output)
        self.assertIsInstance(msds, MultiSourceDataStore)
        self.assertEqual(4, len(cm.output))
        msg = (
            "ERROR:xcube.multistore:An error occurred: 'ZenodoDataStore' "
            "object has no attribute 'write_data'"
        )
        self.assertEqual(msg, str(cm.output[-1]))

    def test_resample_params_process_dataset(self):
        config_dict = get_config_dict9()
        with self.assertLogs("xcube.multistore", level="INFO") as cm:
            msds = MultiSourceDataStore(config_dict)
        print(cm.output)
        # self.assertEqual(4, len(cm.output))
        self.assertIsInstance(msds, MultiSourceDataStore)
        config_ds = msds.config.datasets
        self.assertEqual({'agg_methods': 'max'},
                         config_ds['dataset_final']['variables'][0][
                             'resample_params']
                         )
        self.assertEqual(
            {"tile_size": [400, 400]},
            config_ds["dataset_final"]["variables"][1]["resample_params"],
        )
        # print(msds.stores.storage.list_data_ids())
        # ds = msds.stores.storage.open_data("dataset_final.zarr")
        # print(ds)
        # self.assertIsInstance(ds, xr.Dataset)
