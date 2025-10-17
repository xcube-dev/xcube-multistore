## Changes in 0.3.0 (in development)

* Switched to the new unified `xcube-resampling` library from 
  `xcube.core.resampling`.
* Added `spatial_resample_params` support and updated the config visualization to
  display the `Spatial Resample Params`.


## Changes in 0.2.0

* Added the method `display_config`, which provides an overview of the cube generation 
  plan retrieved from the configuration file.  
* Added auxiliary functions to support setting up configurations for cube generation. 
  Examples of all functionalities are provided in the notebook [setup_config.ipynb](examples/setup_config.ipynb).

## Changes in 0.1.0

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