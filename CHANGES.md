## Changes in 0.1.0 (In Development)

* The initial version of the Multi-Source Data Store has been developed in 
  the `xcube_multistore` package. It supports the following features:
  * subset of dataset (defined by grid mapping)
  * resample and reproject dataset (defined by grid mapping)
  * grid mapping may be defined by the user or by a dataset 
  * allow for time series at a single spatial point; interpolate the neighbouring points
  * allow data fusion, where data variables in one `xr.Dataset` refers to different data sources
  * support spatial cutout of an area around a defined spatial point.
  * support preload API for [xcube-clms](https://github.com/xcube-dev/xcube-clms) and 
    [xcube-zendoo](https://github.com/xcube-dev/xcube-zenodo)
  * allow to write to netcdf and zarr