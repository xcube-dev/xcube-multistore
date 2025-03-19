# xcube Multi-Source Data Store

`xcube-multistore` is a Python package designed to create a Multi-Source Data Store 
that enables the seamless integration of data from multiple sources into a unified 
data model. This approach simplifies the data fusion process while ensuring 
**transparency** and **reproducibility** through well-defined configurations.

The package utilizes [xcube’s data access](https://xcube.readthedocs.io/en/latest/dataaccess.html#data-access), 
implemented via data store plugins, along with additional functionalities from 
[xcube](https://xcube.readthedocs.io/), to manipulate and harmonize datasets according
to user-defined specifications.

The workflow includes the following steps:

1. Data access through xcube data stores
2. Data harmonization
3. Data fusion (if required)

This process results in either a single, unified data cube with all datasets aligned
to a consistent grid or a catalog of separate datasets.

## Overview

The Multi-Source Data Store is configured via a YAML file. You can find an example 
configuration in [examples/config.yml](https://github.com/xcube-dev/xcube-multistore/blob/main/examples/config.yml).

For more detailed guidance on creating a configuration file, please refer to the 
[Configuration Guide](https://xcube-dev.github.io/xcube-multistore/config/).

Once the configuration file is ready, the Multi-Source Data Store can be started 
with a single line of code, as shown below:


```python
from xcube_multistore.multistore import MultiSourceDataStore

msds = MultiSourceDataStore("config.yml")
```

For further examples please view the [examples folder](https://github.com/xcube-dev/xcube-multistore/blob/main/examples).

## Features 

> **IMPORTANT:**  
> The `xcube-multistore` package is currently in the early stages of development.  
> The following features are available so far:

* subset of dataset (defined by grid mapping)
* resample and reproject dataset (defined by grid mapping)
* grid mapping may be defined by the user or by a dataset 
* allow for time series at a single spatial point; interpolate the neighbouring points
* allow data fusion, where data variables in one `xr.Dataset` refers to different data sources
* support spatial cutout of an area around a defined spatial point.
* support preload API for [xcube-clms](https://github.com/xcube-dev/xcube-clms) and 
  [xcube-zendoo](https://github.com/xcube-dev/xcube-zenodo)
* allow to write to netcdf and zarr

> The following features will be implemented in the future:

* some auxiliary functionalities which shall help to setup a config yml file. 
* interpolate along the time axis 

### License

The package is open source and released under the 
[MIT license](https://opensource.org/license/mit). :heart:

