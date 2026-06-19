import numpy as np
import xarray as xr


def modify_gami(ds: xr.Dataset) -> xr.Dataset:
    ds = ds.isel(time=-1)
    ds = ds.mean(dim="members")
    return ds


def modify_efda(ds: xr.Dataset) -> xr.Dataset:
    ds = ds.drop_vars("disturbance_occurrence")
    ds["disturbance_agent"] = ds["disturbance_agent"].astype(np.uint8)
    return ds
