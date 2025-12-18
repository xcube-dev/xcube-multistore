import pandas as pd
import xarray as xr


def modify_time_axis(ds: xr.Dataset) -> xr.Dataset:
    ds = ds.rename({"band": "time"})
    time_axis = pd.date_range(start="1985-01-01", end="2023-01-01", freq="YS")
    return ds.assign_coords(dict(time=time_axis))


def modify_gami(ds: xr.Dataset) -> xr.Dataset:
    return ds.transpose("members", "time", "latitude", "longitude")
