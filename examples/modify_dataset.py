import xarray as xr
import numpy as np


def biomass_xu_merge_years(ds: xr.Dataset) -> xr.Dataset:
    ds = ds.rename(name_dict={"x": "lon", "y": "lat"})
    ds_arr = ds.to_dataarray(dim="time")
    time = [np.datetime64(f"{year}-01-01T00:00:00") for year in range(2000, 2019 + 1)]
    ds["carbon_density"] = ds_arr.assign_coords(coords=dict(time=time))
    var_del = [f"da_{i}" for i in range(1, 21)]
    ds = ds.drop_vars(var_del)
    return ds
