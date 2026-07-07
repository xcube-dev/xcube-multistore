import xarray as xr


def modify_cci_biomass(ds: xr.Dataset) -> xr.Dataset:
    return ds.transpose("time", "latitude", "longitude")


def modify_gami(ds: xr.Dataset) -> xr.Dataset:
    ds = ds.mean(dim="members")
    return ds.transpose("time", "latitude", "longitude")
