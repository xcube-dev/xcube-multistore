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
from unittest.mock import patch, MagicMock, mock_open

from xcube_multistore.stores import DataStores
from xcube_multistore.config import MultiSourceConfig


class DataStoresTest(unittest.TestCase):

    @patch("xcube_multistore.stores.new_data_store")
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data='{"client_id": "id", "key_id": "key"}',
    )
    def test_setup_data_stores(self, mock_open_file, mock_new_data_store):
        config_dict = {
            "datasets": [
                {
                    "identifier": "clms",
                    "store": "datasource",
                    "data_id": "bla",
                }
            ],
            "data_stores": [
                {
                    "identifier": "storage",
                    "store_id": "memory",
                    "store_params": {"root": "data"},
                },
                {
                    "identifier": "datasource",
                    "store_id": "clms",
                    "store_params": {"credentials": "clms-credentials.json"},
                },
            ],
        }
        mock_store_instance = MagicMock()
        mock_new_data_store.return_value = mock_store_instance

        config = MultiSourceConfig(config_dict)
        stores = DataStores.setup_data_stores(config)
        self.assertTrue(hasattr(stores, "datasource"))
        self.assertTrue(hasattr(stores, "storage"))
        self.assertEqual(getattr(stores, "datasource"), mock_store_instance)
        self.assertEqual(getattr(stores, "storage"), mock_store_instance)
        mock_open_file.assert_called_once_with("clms-credentials.json")
        mock_new_data_store.assert_any_call(
            "clms", credentials={"client_id": "id", "key_id": "key"}
        )
