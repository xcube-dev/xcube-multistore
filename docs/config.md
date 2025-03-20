# Configuration Guide

The Multi-Source Data Store is configured using a YAML file. This configuration 
defines key elements such as datasets, data stores, grid mappings, and general 
parameters, which are essential for setting up a workflow to generate data cubes. 
This guide will walk you through the schema for each section of the configuration 
file and show you how to create a valid configuration.

To view the schema of the configuration, you can run the following code:

```python
from xcube_multistore.multistore import MultiSourceDataStore

MultiSourceDataStore.get_config_schema()
```

Additionally, you can explore example configurations in the 
[examples folder](https://github.com/xcube-dev/xcube-multistore/blob/main/examples).


## Entire configuration schema

The entire configuration schema consists of the following sections: 

### Properties:
- **datasets** (Required): list of [dataset objects](#dataset-object) objects.
- **preload_datasets** (Optional): list of [preload dataset objects](#preload-dataset-object).
- **data_stores** (Required): list of [store objects](#store-object).  
  A single data store with identifier `storage` is required for 
  writing the final data cubes.
- **grid_mappings** (Optional): list of [grid mapping objects](#grid-mapping-object).
- **general** (Optional): following the [general object](#general-object) mapping.


## Object parameters

An object in this configuration schema can refer to a dataset, data store, or grid 
mapping. 

### dataset object

Two different dataset types are available:

#### single dataset object
This configures a dataset object from a single data source.  

**Properties**:
- **[identifier](#identifier)**: This identifier defines the name of the final data cube.
- **[store](#store)**
- **[grid_mapping](#grid_mapping)**
- **[data_id](#data_id)**
- **[open_params](#open_params)**
- **[format_id](#format_id)**
- **[custom_processing](#custom_processing)**

#### multi dataset object
This configures a datasets from multiple data sources. Each [variable object](#variable-object) is 
opened individually, and once all datasets are loaded, they are merged using 
`xarray.merge()`.

**Properties**:
- **[identifier](#identifier)**: This identifier defines the name of the final data cube.
- **[grid_mapping](#grid_mapping)**
- **[format_id](#format_id)**
- **[xr_merge_params](#xr_merge_params)**
- **variables**: List of [variable object](#variable-object) objects.

#### variable object
This configures a variable object, which is stored as a data variable within a single 
`xarray.Dataset` object.

**Properties**:
- **[identifier](#identifier)**: This identifier defines the variable name within the `xarray.Dataset`.
- **[store](#store)**
- **[data_id](#data_id)**
- **[open_params](#open_params)**
- **[custom_processing](#custom_processing)**


### store object
This configures a store object representing one xcube data store instance.

**Properties:**
- **[identifier](#identifier)**
- **[store_id](#store_id)**
- **[store_params](#store_params)**


### grid mapping object
This configures a grid mapping object.

**Properties:**
- **[identifier](#identifier)**
- **[bbox](#bbox)**
- **[crs](#crs)**
- **[spatial_res](#spatial_res)**
- **[tile_size](#tile_size)**
- 
### preload dataset object
This object configures the preloading of datasets that require it. Note that some 
datasets are not accessible instantly via lazy loading, due to storage in compressed 
formats or due to lengthy queuing processes during data requests. These dataset can 
be preloaded once, which are then available to the user throughout the lifespan of
a project.

Currently, the preload API is available for the following data stores:
- [clms](https://github.com/xcube-dev/xcube-clms)
- [zenodo](https://github.com/xcube-dev/xcube-zenodo)

**Properties**:
- **[store](#store)**: The data store where the dataset is located.
- **data_ids**: A list of [data_id](#data_id) values for the datasets to preload.
- **preload_params**: Parameters specific to the respective data store for preloading.


### general object
This object configures general parameters that control various aspects of the cube 
generation process.

**Properties:**
- **[visualize](#visualize)**
- **[force_preload](#force_preload)**
- **[dask_scheduler](#dask_scheduler)**
- **[gdal_http_params](#gdal_http_params)**


## Single field parameters

### identifier
Identifier for any object like dataset, grid mapping, store.

### store
Store identifier used to open the dataset.

### grid_mapping
Identifier that assigns a grid mapping to the final dataset for reprojection.

### data_id
Unique identifier for the dataset's data source within the assigned data store.

### open_params
Open data parameters related to the data store and data_id

### xr_merge_params
Parameters of `xarry.merge` needed if harmonization of multiple datasets into one
datacube is required. For further information view the [xarray.merge documentation](https://docs.xarray.dev/en/stable/generated/xarray.merge.html).
**Defaults**: `(compat="no_conflicts", join="exact", fill_value="<NA>", combine_attrs="drop_conflicts")`.

### format_id
Desired format of the saved datacube.  

**Default:** `zarr`  
**Allowed values:** `netcdf`, `zarr`

### custom_processing
This section enables users to define a Python function that is executed after opening 
the data. It allows for custom dataset manipulation, making it particularly useful 
for handling unstructured datasets. The function must accept an `xarray.Dataset` as 
input and return a modified `xarray.Dataset` as output.

**Properties**:  
- **module_path**: Path to the Python module relative to the config file.  
- **function_name**: Name of the function within the specified module. The function must
  take an `xarray.Dataset` as input and return a transformed `xarray.Dataset`.  

### store_id
The **store identifier** is used within the xcube ecosystem to specify different data 
stores. For more information about the available data stores, refer to the 
[xcube data store documentation](https://xcube.readthedocs.io/en/latest/dataaccess.html#data-store-framework),
or check the README of each specific data store plugin on GitHub, where links are given 
below.

**Allowed Values**:  
- [cds](https://github.com/xcube-dev/xcube-cds)  
- [clms](https://github.com/xcube-dev/xcube-clms)  
- [cmems](https://github.com/xcube-dev/xcube-cmems)  
- [esa-cci, esa-cci-kc, esa-cci-zarr](https://github.com/esa-cci/xcube-cci)
- [file, https, memory, s3](https://xcube.readthedocs.io/en/latest/dataaccess.html#filesystem-based-data-stores)   
- [sentinelhub, sentinelhub-cdse](https://github.com/xcube-dev/xcube-sh)  
- [smos](https://github.com/xcube-dev/xcube-smos)  
- [stac, stac-cdse, stac-xcube](https://github.com/xcube-dev/xcube-stac)
- [zenodo](https://github.com/xcube-dev/xcube-zenodo)  

---

### store_params
The **store-specific parameters** define the configuration for a particular data store. 
These parameters can be retrieved using `get_data_store_params_schema(store_id)`. 
For more details, refer to the [xcube data store documentation](https://xcube.readthedocs.io/en/latest/dataaccess.html#data-store-framework), 
or consult the README of each specific data store plugin on GitHub, which is linked in 
the [store_id section](#store_id).

### spatial_res
Spatial Resolution of the final grid mapping.

### crs
The Coordinate Reference System (CRS) defines the coordinate reference system 
of the final grid mapping. Note that the CRS should be provided as a string, 
e.g., "EPSG:4326".

### bbox
The bounding box of the final grid mapping, expressed in the units of the CRS, is 
provided as [west, south, east, north].

### tile_size
The spatial chunk size in the grid mapping. If a single integer is provided, a square 
tile size is assumed. Otherwise, specify a list in the format [x, y] or [lon, lat].

**Default**: [1024, 1024]

### visualize
Switch between visualization in table, if True, and logging, if False.  

**Default:** `True`

### force_preload
If True, all data IDS given in section `preload_datasets` will be preloaded. If False, 
only non-preloaded datasets will be preloaded.  

**Default:** `True`

### dask_scheduler
Scheduler mode put into `dask.config.set(scheduler=<scheduler_mode>)`  
  
**Default:** `threads`  
**Allowed values:** `threads`, `processes`, `single-threaded`, `sync`, `distributed`

### gdal_http_params
GDAL http environment variables which are used when opening a tif file with 
`rioxarray.open_rasterio`, which uses GDAL driver under the hood, from a remote source.  

**Properties**:
- **gdal_http_max_retry**: Maximal number of retries of an HTTP request in GDAL. 
  **Default:** `10` 
- **gdal_http_retry_delay** :Delay in seconds between retries of an HTTP request in GDAL. 
  **Default:** `5` 




