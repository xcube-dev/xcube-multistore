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

from .version import __version__

from .multistore import MultiSourceDataStore

describe_data = MultiSourceDataStore.describe_data
get_config_schema = MultiSourceDataStore.get_config_schema
get_data_store_params_schema = MultiSourceDataStore.get_data_store_params_schema
get_open_data_params_schema = MultiSourceDataStore.get_open_data_params_schema
list_data_ids = MultiSourceDataStore.list_data_ids
list_data_store_ids = MultiSourceDataStore.list_data_store_ids
get_search_params_schema = MultiSourceDataStore.get_search_params_schema
search_data_ids = MultiSourceDataStore.search_data_ids

__all__ = [
    "MultiSourceDataStore",
    "describe_data",
    "get_config_schema",
    "get_data_store_params_schema",
    "get_open_data_params_schema",
    "get_search_params_schema",
    "list_data_ids",
    "list_data_store_ids",
    "search_data_ids",
]
