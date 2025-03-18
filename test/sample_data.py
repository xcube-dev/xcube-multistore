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
            },
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


def get_config_dict4():
    return {
        "preload_datasets": [
            {
                "store": "datasource",
                "data_ids": ["dataset0.zip", "dataset1.zarr.zip"],
            }
        ],
        "datasets": [
            {
                "identifier": "dataset1",
                "store": "datasource",
                "grid_mapping": "grid1",
                "data_id": "dataset1.zarr",
                "format_id": "netcdf",
            },
            {
                "identifier": "dataset2",
                "grid_mapping": "grid1",
                "variables": [
                    {
                        "identifier": "data_var1",
                        "store": "datasource",
                        "data_id": "dataset0/data_var0.zarr",
                    },
                    {
                        "identifier": "data_var2",
                        "store": "datasource",
                        "data_id": "dataset0/data_var1.zarr",
                    },
                ],
            },
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
                "tile_size": 512,
            }
        ],
        "general": {
            "dask_scheduler": "single-threaded",
            "gdal_http_params": dict(gdal_http_max_retry=20, gdal_http_retry_delay=2),
        },
    }


def get_config_dict5():
    return {
        "preload_datasets": [
            {
                "store": "zenodo_senf",
                "data_ids": ["andorra.zip"],
            }
        ],
        "datasets": [
            {
                "identifier": "senf_andorra",
                "variables": [
                    {
                        "identifier": "annual_disturbances_1985_2023",
                        "store": "zenodo_senf",
                        "data_id": "andorra/annual_disturbances_1985_2023_andorra.zarr",
                    },
                    {
                        "identifier": "forest_mask",
                        "store": "zenodo_senf",
                        "data_id": "andorra/forest_mask_andorra.zarr",
                    },
                ],
            },
            {
                "identifier": "biomass_xu",
                "store": "zenodo_xu",
                "grid_mapping": "senf_andorra",
                "data_id": "test10a_cd_ab_pred_corr_2000_2019_v2.tif",
                "custom_processing": {
                    "module_path": "test.sample_data",
                    "function_name": "biomass_xu_merge_years",
                },
            },
        ],
        "data_stores": [
            {
                "identifier": "storage",
                "store_id": "memory",
                "store_params": {"root": "data"},
            },
            {
                "identifier": "zenodo_senf",
                "store_id": "zenodo",
                "store_params": {"root": "13333034"},
            },
            {
                "identifier": "zenodo_xu",
                "store_id": "zenodo",
                "store_params": {"root": "4161694"},
            },
        ],
    }


def get_config_dict6():
    return {
        "datasets": [
            {
                "identifier": "dataset3",
                "store": "datasource",
                "data_id": "dataset3.zarr",
                "open_params": {"point": [40, 0]},
            },
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
    }


def get_config_dict7():
    return {
        "datasets": [
            {
                "identifier": "dataset1",
                "store": "datasource",
                "grid_mapping": "grid1",
                "data_id": "dataset1.zarr",
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


def get_config_dict8():
    return {
        "datasets": [
            {
                "identifier": "dataset1",
                "store": "datasource",
                "data_id": "dataset1.zarr",
            }
        ],
        "data_stores": [
            {
                "identifier": "storage",
                "store_id": "zenodo",
                "store_params": {"root": "13333034"},
            },
            {
                "identifier": "datasource",
                "store_id": "memory",
                "store_params": {"root": "datasource"},
            },
        ],
        "general": {"visualize": False},
    }


def get_sample_data_2d():
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


def get_sample_data_3d():
    lon = np.arange(-5.0, 6.0, 5.0)
    lat = np.arange(45.0, 34.0, -5.0)
    time = np.arange(10)
    spatial_ref = np.array(0)
    band_1 = np.arange(90).reshape((10, 3, 3))
    ds = xr.Dataset(
        dict(
            band_1=xr.DataArray(
                band_1,
                dims=("time", "lat", "lon"),
                attrs=dict(grid_mapping="spatial_ref"),
            )
        ),
        coords=dict(time=time, lat=lat, lon=lon, spatial_ref=spatial_ref),
    )
    ds.spatial_ref.attrs = pyproj.CRS.from_epsg("4326").to_cf()
    ds.attrs = dict(history=["test", "list sterilization", "if saved to netcdf"])
    return ds


def modify_dataset(ds: xr.Dataset) -> xr.Dataset:
    ds["band_1"] *= 2
    return ds


def biomass_xu_merge_years(ds: xr.Dataset) -> xr.Dataset:
    ds = ds.rename(name_dict={"x": "lon", "y": "lat"})
    ds_arr = ds.to_dataarray(dim="time")
    time = [np.datetime64(f"{year}-01-01T00:00:00") for year in range(2000, 2019 + 1)]
    ds["carbon_density"] = ds_arr.assign_coords(coords=dict(time=time))
    var_del = [f"da_{i}" for i in range(1, 21)]
    ds = ds.drop_vars(var_del)
    return ds
