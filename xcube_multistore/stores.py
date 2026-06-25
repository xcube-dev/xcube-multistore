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

import json

from xcube.core.store import new_data_store

from .config import MultiSourceConfig


class DataStores:

    @classmethod
    def setup_data_stores(cls, config: MultiSourceConfig):
        for identifier, config_store in config.data_stores.items():
            store_params = config_store.get("store_params", {})
            if config_store["store_id"] == "clms":
                with open(store_params["credentials"]) as f:
                    store_params["credentials"] = json.load(f)
            if config_store["identifier"] == "storage" and (
                config_store["store_id"] in ["file", "s3"]
            ):
                if not "max_depth" in store_params:
                    store_params["max_depth"] = 10
            setattr(
                cls,
                identifier,
                new_data_store(config_store["store_id"], **store_params),
            )
            setattr(cls, f"{identifier}_store_id", config_store["store_id"])
        return cls
