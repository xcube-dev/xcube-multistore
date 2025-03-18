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

import unittest
from unittest.mock import patch
import sys

import fsspec
import yaml
from xcube.util.jsonschema import JsonObjectSchema, JsonArraySchema, JsonComplexSchema

from xcube_multistore.config import MultiSourceConfig
from xcube_multistore.config import is_jupyter_notebook
from .sample_data import get_config_dict4


class MultiSourceConfigTest(unittest.TestCase):

    def test_get_schema(self):
        schema = MultiSourceConfig.get_schema()
        self.assertIsInstance(schema, JsonObjectSchema)
        self.assertIn("datasets", schema.properties)
        self.assertIn("preload_datasets", schema.properties)
        self.assertIn("data_stores", schema.properties)
        self.assertIn("grid_mappings", schema.properties)
        self.assertIn("general", schema.properties)
        self.assertIn("datasets", schema.required)
        self.assertIn("data_stores", schema.required)
        self.assertIsInstance(schema.properties["datasets"], JsonArraySchema)
        self.assertIsInstance(schema.properties["datasets"].items, JsonComplexSchema)
        dataset_properties = schema.properties["datasets"].items.one_of[0].properties
        self.assertIn("identifier", dataset_properties)
        self.assertIn("store", dataset_properties)
        self.assertIn("grid_mapping", dataset_properties)
        self.assertIn("data_id", dataset_properties)
        dataset_required = schema.properties["datasets"].items.one_of[0].required
        self.assertIn("identifier", dataset_required)
        self.assertIn("store", dataset_required)
        self.assertIn("data_id", dataset_required)
        self.assertNotIn("grid_mapping", dataset_required)
        self.assertIn("store_id", schema.properties["data_stores"].items.properties)
        self.assertIn("store_params", schema.properties["data_stores"].items.properties)
        self.assertIn("bbox", schema.properties["grid_mappings"].items.properties)
        self.assertIn(
            "spatial_res", schema.properties["grid_mappings"].items.properties
        )
        self.assertIn("crs", schema.properties["grid_mappings"].items.properties)
        self.assertIn("tile_size", schema.properties["grid_mappings"].items.properties)
        self.assertIn("visualize", schema.properties["general"].properties)
        self.assertIn("force_preload", schema.properties["general"].properties)
        self.assertIn("dask_scheduler", schema.properties["general"].properties)
        self.assertIn("gdal_http_params", schema.properties["general"].properties)

    def test_init(self):
        def subtest_config(config: MultiSourceConfig, gdal_http_params: dict):
            self.assertIsInstance(config, MultiSourceConfig)
            self.assertEqual(len(config.preload_datasets), 1)
            self.assertEqual(len(config.datasets), 2)
            self.assertEqual(config.datasets["dataset1"]["data_id"], "dataset1.zarr")
            self.assertEqual(len(config.grid_mappings), 1)
            self.assertEqual(config.grid_mappings[0]["identifier"], "grid1")
            self.assertEqual(len(config.data_stores), 2)
            self.assertEqual(config.data_stores["storage"]["store_id"], "memory")
            self.assertEqual(
                config.preload_map,
                {
                    "dataset0.zip": [
                        "dataset0/data_var0.zarr",
                        "dataset0/data_var1.zarr",
                    ],
                    "dataset1.zarr.zip": ["dataset1.zarr"],
                },
            )
            self.assertEqual(config.general["visualize"], False)
            self.assertEqual(config.general["force_preload"], False)
            self.assertEqual(config.general["dask_scheduler"], "single-threaded")
            self.assertDictEqual(gdal_http_params, config.general["gdal_http_params"])

        # test config given as dict
        config_dict = get_config_dict4()
        config = MultiSourceConfig(config_dict)
        gdal_http_params = dict(gdal_http_max_retry=20, gdal_http_retry_delay=2)
        subtest_config(config, gdal_http_params)
        config_dict = get_config_dict4()
        del config_dict["general"]["gdal_http_params"]["gdal_http_max_retry"]
        config = MultiSourceConfig(config_dict)
        gdal_http_params = dict(gdal_http_max_retry=10, gdal_http_retry_delay=2)
        subtest_config(config, gdal_http_params)
        config_dict = get_config_dict4()
        del config_dict["general"]["gdal_http_params"]["gdal_http_retry_delay"]
        config = MultiSourceConfig(config_dict)
        gdal_http_params = dict(gdal_http_max_retry=20, gdal_http_retry_delay=5)
        subtest_config(config, gdal_http_params)

        # test config given as filepath
        config_path = "memory://config.yml"
        config_dict = get_config_dict4()
        with fsspec.open(config_path, "w") as file:
            yaml.dump(config_dict, file)
        config = MultiSourceConfig(config_path)
        gdal_http_params = dict(gdal_http_max_retry=20, gdal_http_retry_delay=2)
        subtest_config(config, gdal_http_params)

    def test_is_jupyter_notebook(self):
        with patch.dict(sys.modules, {"IPython": None}):
            self.assertFalse(is_jupyter_notebook())
