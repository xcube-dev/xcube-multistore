## Changes in 0.3.2

- Improved logging output when `visualize` is disabled in the configuration.
- Updated dependency versions for the xcube data store plugins.


## Changes in 0.3.1

- Use `xcube_resampling.gridmapping.GridMapping.regular_from_bbox` to create a 
  regular grid mapping instance from configuration.
- Added support for ERA5 time series extraction via the xcube-cds data store plugin.
- Improved documentation by adding a new Data Availability page.
- Fixed an issue by unifying chunking of concatenated datasets prior to writing.


## Changes in 0.3.0

* **Unified spatial resampling**: Switched to the new
  `xcube_resampling.resample_in_space` library, replacing
  `xcube.core.resampling.resample_in_space`.
* **Spatial resample parameters**: Added support for `spatial_resample_params`
  and updated the configuration visualization to display these parameters.
* **Temporal resampling**: Introduced `xcube_resampling.resample_in_time` with
  `temporal_resample_params` added to the configuration to cover temporal resampling.
* **Controlled cube generation**: `xcube_multistore.MultiSourceDataStore` is now only
  initialized in the constructor without automatically starting cube generation.
  A new `generate()` method is provided to explicitly start cube creation.
* **Geolocation visualization**: Added `display_geolocations()` to visualize
  all requested geolocations of the datacubes to be generated.
* Fixed visualization of preload handle
* Experimental configuration setup GUI added using panel.
* Implemented a new access handler that supports large dataset requests by
  automatically splitting and retrieving data from the `"cds"`, `"stac-cdse-ardc"`,
  and `"stac-pc-ardc"` data stores.


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