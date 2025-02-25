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

import fsspec
import yaml
from xcube.util.jsonschema import JsonObjectSchema, JsonArraySchema

from xcube_multistore.config import MultiSourceConfig
from .sample_data import get_config_dict0


class MultiSourceConfigTest(unittest.TestCase):

    def test_get_schema(self):
        schema = MultiSourceConfig.get_schema()
        self.assertIsInstance(schema, JsonObjectSchema)
        self.assertIn("datasets", schema.properties)
        self.assertIn("data_stores", schema.properties)
        self.assertIn("grid_mappings", schema.properties)
        self.assertIn("general", schema.properties)
        self.assertIn("datasets", schema.required)
        self.assertIn("data_stores", schema.required)
        self.assertIsInstance(schema.properties["datasets"], JsonArraySchema)
        self.assertIn("identifier", schema.properties["datasets"].items.properties)
        self.assertIn("store", schema.properties["datasets"].items.properties)
        self.assertIn("grid_mapping", schema.properties["datasets"].items.properties)
        self.assertIn("data_id", schema.properties["datasets"].items.properties)
        self.assertIn("identifier", schema.properties["datasets"].items.required)
        self.assertIn("data_id", schema.properties["datasets"].items.required)
        self.assertNotIn("grid_mapping", schema.properties["datasets"].items.required)
        self.assertIn("store_id", schema.properties["data_stores"].items.properties)
        self.assertIn("store_params", schema.properties["data_stores"].items.properties)
        self.assertIn("bbox", schema.properties["grid_mappings"].items.properties)
        self.assertIn(
            "spatial_res", schema.properties["grid_mappings"].items.properties
        )
        self.assertIn("crs", schema.properties["grid_mappings"].items.properties)

    def test_init(self):
        def subtest_config(config: MultiSourceConfig):
            self.assertIsInstance(config, MultiSourceConfig)
            self.assertEqual(len(config.datasets), 1)
            self.assertEqual(config.datasets[0]["identifier"], "dataset1")
            self.assertEqual(len(config.grid_mappings), 1)
            self.assertEqual(config.grid_mappings[0]["identifier"], "grid1")
            self.assertEqual(len(config.data_stores), 2)
            self.assertEqual(config.data_stores[0]["identifier"], "storage")

        # test config given as dict
        config_dict = get_config_dict0()
        config = MultiSourceConfig(config_dict)
        subtest_config(config)

        # test config given as filepath
        config_path = "memory://config.yml"
        config_dict = get_config_dict0()
        with fsspec.open(config_path, "w") as file:
            yaml.dump(config_dict, file)
        config = MultiSourceConfig(config_path)
        subtest_config(config)
