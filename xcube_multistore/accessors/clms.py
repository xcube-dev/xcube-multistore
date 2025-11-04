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

import xarray as xr
from xcube.core.mldataset import MultiLevelDataset

from xcube_multistore.accessor import Accessor


class CdsAccessor(Accessor):
    """Provides methods for accessing dataset from xcube-cds data store"""

    def open_data(
        self,
        data_id: str,
        **open_params,
    ) -> xr.Dataset | MultiLevelDataset:
        """Open and return the dataset corresponding to data ID."""
        open_params = self._convert_point_to_bbox(open_params)
        return self.store.open_data(data_id, **open_params)

    @staticmethod
    def _convert_point_to_bbox(open_params: dict):
        if "point" in open_params:
            lon, lat = open_params.pop("point")
            open_params["bbox"] = [
                lon - 2 * open_params["spatial_res"],
                lat - 2 * open_params["spatial_res"],
                lon + 2 * open_params["spatial_res"],
                lat + 2 * open_params["spatial_res"],
            ]
        return open_params
