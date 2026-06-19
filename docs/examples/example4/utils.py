import xarray as xr


def biomass_xu_rename(ds: xr.Dataset) -> xr.Dataset:
    ds = ds.rename(name_dict={"x": "lon", "y": "lat", "da": "carbon_density"})
    return ds
