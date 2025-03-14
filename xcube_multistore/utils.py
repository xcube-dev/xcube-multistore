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

import pyproj
import functools

import numpy as np
import xarray as xr
from xcube.core.gridmapping import GridMapping

from .constants import CRS_WGS84, COMPRESSED_FORMATS


def safe_execute():
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                return e
            except KeyboardInterrupt:
                return "Keyboard Interrupt caught! Exiting gracefully."

        return wrapper

    return decorator


def prepare_dataset_for_netcdf(ds: xr.Dataset) -> xr.Dataset:
    attrs = ds.attrs
    for key in attrs:
        if (
            isinstance(attrs[key], list)
            or isinstance(attrs[key], tuple)
            or isinstance(attrs[key], dict)
        ):
            attrs[key] = str(attrs[key])
    ds = ds.assign_attrs(attrs)
    return ds


def get_utm_zone(lat: float, lon: float) -> str:
    zone_number = int((lon + 180) / 6) + 1
    if lat >= 0:
        epsg_code = 32600 + zone_number
    else:
        epsg_code = 32700 + zone_number
    return f"epsg:{epsg_code}"


def get_bbox(
    lat: float,
    lon: float,
    cube_width: float,
    crs_final: pyproj.CRS | str = "utm",
) -> (list[int], pyproj.CRS):

    if crs_final == "utm":
        crs_final = get_utm_zone(lat, lon)

    transformer = pyproj.Transformer.from_crs(CRS_WGS84, crs_final, always_xy=True)
    x, y = transformer.transform(lon, lat)

    half_size_m = cube_width / 2
    bbox_final = [
        round(x - half_size_m),
        round(y - half_size_m),
        round(x + half_size_m),
        round(y + half_size_m),
    ]
    return bbox_final, crs_final


def get_data_id(config: dict) -> str:
    format_id = config.get("format_id", "zarr")
    if format_id == "netcdf":
        data_id = f"{config['identifier']}.nc"
    else:
        data_id = f"{config['identifier']}.zarr"
    return data_id


def clean_dataset(ds: xr.Dataset) -> xr.Dataset:
    check_vars = ["x_bnds", "y_bnds", "lat_bnds", "lon_bnds", "time_bnds"]
    sel_vars = []
    for var in check_vars:
        if var in ds:
            sel_vars.append(var)
    ds = ds.drop_vars(sel_vars)
    ds = normalize_grid_mapping(ds)
    return ds


def normalize_grid_mapping(ds: xr.Dataset) -> xr.Dataset:
    gm_name = get_grid_mapping_name(ds)
    if gm_name is None:
        return ds
    gm = GridMapping.from_dataset(ds)
    ds = ds.drop_vars(gm_name)
    ds = ds.assign_coords(spatial_ref=xr.DataArray(0, attrs=gm.crs.to_cf()))
    for var in ds.data_vars:
        ds[var].attrs["grid_mapping"] = "spatial_ref"
    return ds


def get_grid_mapping_name(ds: xr.Dataset) -> str | None:
    gm_names = []
    for var in ds.data_vars:
        if "grid_mapping" in ds[var].attrs:
            gm_names.append(ds[var].attrs["grid_mapping"])
    if "crs" in ds:
        gm_names.append("crs")
    if "spatial_ref" in ds.coords:
        gm_names.append("spatial_ref")
    gm_names = np.unique(gm_names)
    assert len(gm_names) <= 1, "Multiple grid mapping names found."
    if len(gm_names) == 1:
        return str(gm_names[0])
    else:
        return None


def remove_compressed_extension(data_id: str) -> str:
    for format_id in COMPRESSED_FORMATS:
        if data_id.endswith(format_id):
            data_id = data_id.replace(f".{format_id}", "")
            break
    return data_id
