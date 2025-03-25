import pandas as pd
import xarray as xr


def modify_time_axis(ds: xr.Dataset) -> xr.Dataset:
    time_axis = pd.date_range(start="1985-01-01", end="2023-01-01", freq="YS")
    var_names = [f"band_{i}" for i in range(1, len(time_axis) + 1)]
    ds = ds[var_names]
    da = ds.to_dataarray(dim="time")
    da = da.assign_coords(dict(time=time_axis))
    ds_out = xr.Dataset()
    ds_out["data"] = da
    return ds_out


def modify_gami(ds: xr.Dataset) -> xr.Dataset:
    return ds.transpose("members", "time", "latitude", "longitude")
