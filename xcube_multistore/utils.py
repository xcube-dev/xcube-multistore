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

import functools

import numpy as np
import pyproj
import xarray as xr

from .constants import COMPRESSED_FORMATS, CRS_WGS84


def prepare_dataset_for_netcdf(ds: xr.Dataset) -> xr.Dataset:
    """Prepares an xarray Dataset for NetCDF serialization.

    Converts non-serializable attributes (lists, tuples, and dictionaries) into strings
    to ensure compatibility with NetCDF format.

    Args:
        ds: The input xarray Dataset.

    Returns:
        A dataset with updated attributes, ensuring compatibility with NetCDF.
    """
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
    """Determines the UTM (Universal Transverse Mercator) zone for given coordinates.

    Computes the UTM zone based on longitude and returns the corresponding EPSG code.
    Northern hemisphere zones use EPSG codes in the 32600 range, while southern
    hemisphere zones use EPSG codes in the 32700 range.

    Args:
        lat: Latitude in degrees.
        lon: Longitude in degrees.

    Returns:
        The EPSG code for the corresponding UTM zone (e.g., "epsg:32633").
    """
    zone_number = int((lon + 180) / 6) + 1
    if lat >= 0:
        epsg_code = 32600 + zone_number
    else:
        epsg_code = 32700 + zone_number
    return f"epsg:{epsg_code}"


def get_bbox(
    lat: float, lon: float, cube_width: float, crs_final: pyproj.CRS | str = "utm"
) -> (list[int], pyproj.CRS):
    """Generates a bounding box around a specified latitude and longitude.

    Given a point (latitude, longitude) and the desired width of a cube, this function
    computes the bounding box in the specified coordinate reference system (CRS).
    The bounding box is returned as a list of coordinates, and the CRS is returned
    as well.

    Args:
        lat: Latitude of the central point in degrees.
        lon: Longitude of the central point in degrees.
        cube_width: The width of the cube in units of crs_final, used to define the
            extent of the bounding box.
        crs_final (pyproj.CRS or str, optional): The target CRS for the bounding box.
            Defaults to "utm", which automatically determines the UTM zone based on the
            latitude and longitude.

    Returns:
        A list of four integers representing the bounding box in the format
            [west, south, east, north].
        The final CRS used for the bounding box, returned as a `pyproj.CRS` object.
    """
    if crs_final == "utm":
        crs_final = get_utm_zone(lat, lon)
    if isinstance(crs_final, str):
        crs_final = pyproj.CRS.from_user_input(crs_final)

    transformer = pyproj.Transformer.from_crs(CRS_WGS84, crs_final, always_xy=True)
    x, y = transformer.transform(lon, lat)

    half_size = cube_width / 2
    bbox_final = [x - half_size, y - half_size, x + half_size, y + half_size]
    if not crs_final.is_geographic:
        bbox_final = [round(item) for item in bbox_final]
    return bbox_final, crs_final


def clean_dataset(ds: xr.Dataset) -> xr.Dataset:
    """Cleans an xarray Dataset by removing boundary variables and normalizing the
    grid mapping.

    This function removes specific variables related to bounds (e.g., "x_bnds",
    "y_bnds", "lat_bnds", "lon_bnds", "time_bnds") and normalizes the grid mapping
    by adding a spatial reference coordinate called "spatial_ref" and assigning
    it to the relevant data variables.

    Args:
        ds: The input xarray dataset to be cleaned.

    Returns:
        A cleaned version of the dataset with boundary variables removed and grid
        mapping normalized.
    """
    check_vars = ["x_bnds", "y_bnds", "lat_bnds", "lon_bnds", "time_bnds"]
    sel_vars = []
    for var in check_vars:
        if var in ds:
            sel_vars.append(var)
    ds = ds.drop_vars(sel_vars)
    ds = _normalize_grid_mapping(ds)
    return ds


def _normalize_grid_mapping(ds: xr.Dataset) -> xr.Dataset:
    gm_name = _get_grid_mapping_name(ds)
    if gm_name is None:
        return ds
    crs = pyproj.crs.CRS.from_cf(ds[gm_name].attrs)
    ds = ds.drop_vars(gm_name)
    ds = ds.assign_coords(spatial_ref=xr.DataArray(0, attrs=crs.to_cf()))
    for var in ds.data_vars:
        ds[var].attrs["grid_mapping"] = "spatial_ref"
    return ds


def _get_grid_mapping_name(ds: xr.Dataset) -> str | None:
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


def _remove_compressed_extension(data_id: str) -> str:
    for format_id in COMPRESSED_FORMATS:
        if data_id.endswith(format_id):
            data_id = data_id.replace(f".{format_id}", "")
            break
    return data_id


def _get_data_id(config: dict) -> str:
    format_id = config.get("format_id", "zarr")
    if format_id == "netcdf":
        data_id = f"{config['identifier']}.nc"
    else:
        data_id = f"{config['identifier']}.zarr"
    return data_id


def _safe_execute():
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
