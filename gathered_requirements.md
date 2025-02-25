

## Requirements from the EO-LINCS project
* different time axis possible, e.g. hourly ERA5 and irregular Sen2 observations
* grid mapping may be defined by the user or by a dataset 
* allow for time series at a single spatial point 
* maybe support frame work, which allows to define a point and the size of the bounding 
  box edge (used in DeepExtremes, DeepFeatures, EO-LINCS SCS 1&3)
* some python function as a config generator
* should deal with preloading, should it wait till preload is done, or should it deal
  with other datasets in the mean time 
* interpolate along time axis (optional)
* allow to write to netcdf and zarr

## Requirement from xcube / BC