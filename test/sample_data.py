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

import numpy as np
import pyproj
import xarray as xr


def get_config_dict0():
    return {
        "datasets": [
            {
                "identifier": "dataset1",
                "store": "datasource",
                "grid_mapping": "grid1",
                "data_id": "dataset1.zarr",
                "format_id": "netcdf",
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
                "store_id": "memory",
                "store_params": {"root": "datasource"},
            },
        ],
        "grid_mappings": [
            {
                "identifier": "grid1",
                "bbox": [0, 30, 30, 60],
                "spatial_res": 10,
                "crs": "EPSG:4326",
            }
        ],
        "general": {"visualize": False},
    }


def get_config_dict1():
    return {
        "datasets": [
            {
                "identifier": "dataset1",
                "store": "datasource",
                "data_id": "dataset1.zarr",
                "format_id": "netcdf",
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
                "store_id": "memory",
                "store_params": {"root": "datasource"},
            },
        ],
        "general": {"visualize": False},
    }


def get_config_dict2():
    return {
        "datasets": [
            {
                "identifier": "dataset2",
                "store": "datasource",
                "data_id": "dataset2.zarr",
                "format_id": "netcdf",
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
                "store_id": "memory",
                "store_params": {"root": "datasource"},
            },
        ],
        "general": {"visualize": False},
    }


def get_config_dict3():
    return {
        "datasets": [
            {
                "identifier": "dataset1",
                "store": "datasource",
                "data_id": "dataset1.zarr",
                "format_id": "netcdf",
                "custom_processing": {
                    "module_path": "test.sample_data",
                    "function_name": "modify_dataset",
                },
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
                "store_id": "memory",
                "store_params": {"root": "datasource"},
            },
        ],
        "general": {"visualize": False},
    }


def get_sample_data():
    lon = np.arange(-5.0, 36.0, 5.0)
    lat = np.arange(65.0, 24.0, -5.0)
    spatial_ref = np.array(0)
    band_1 = np.arange(81).reshape((9, 9))
    ds = xr.Dataset(
        dict(
            band_1=xr.DataArray(
                band_1, dims=("lat", "lon"), attrs=dict(grid_mapping="spatial_ref")
            )
        ),
        coords=dict(lat=lat, lon=lon, spatial_ref=spatial_ref),
    )
    ds.spatial_ref.attrs = pyproj.CRS.from_epsg("4326").to_cf()
    ds.attrs = dict(history=["test", "list sterilization", "if saved to netcdf"])
    return ds


def modify_dataset(ds: xr.Dataset) -> xr.Dataset:
    ds["band_1"] *= 2
    return ds
