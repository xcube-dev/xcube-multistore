# How to ...

This section provides practical examples for common `xcube-multistore` workflows.
It covers configuring storage, discovering available datasets, adding new data
sources, and adjusting spatial and temporal properties of generated data cubes.

## How to Change the Location where Final Data Cubes are Stored

The final storage location for generated data cubes is defined by the user in the 
`data_stores` section, as described in the [configuration schema](config.md#entire-configuration-schema).

The storage location can be changed to a local directory using the `file` data store:

```yaml
- identifier: storage
  store_id: file
  store_params:
    root: <relative-path>
```

where `<relative-path>` is the path relative to the directory from which the
Multi-Source Data Store is executed.

Alternatively, the final data cube can be stored in an S3-compatible object
storage using:

```yaml
- identifier: storage
  store_id: s3
  store_params:
    root: <s3-bucket>
    storage_options: 
      anon: False
      key: <S3-key>
      secret: <S3-secret>
```

For additional S3 configuration options (e.g. custom endpoints or other
object-storage settings), refer to [MultiSourceDataStore.get_data_store_params_schema("s3")](api/#xcube_multistore.multistore.MultiSourceDataStore.get_data_store_params_schema).

## How to List All Available Datasets within a Data Store

Use the auxiliary function [`list_data_ids`](api/#xcube_multistore.multistore.MultiSourceDataStore.list_data_ids) 
to retrieve all available dataset identifiers (`data_id`) from a specific data store.

In the example below, we list the available data IDs for two different data stores:

```python
store_params = {
    "cds": {
        "endpoint_url": "https://cds.climate.copernicus.eu/api",
        "cds_api_key": "<cds-api-key>"
    },
    "stac": {
        "url": "https://s3.gfz-potsdam.de/dog.atlaseo-glm.eo-gridded-data/collections/public/catalog.json"
    }
}

MultiSourceDataStore.list_data_ids(store_params)
```

Note that some data stores provide hundreds of datasets, which can make it difficult 
to identify the desired one. Therefore, `xcube-multistore` also provides functionality 
to search for specific datasets using [`MultiSourceDataStore.search_data_ids`](api/#xcube_multistore.multistore.MultiSourceDataStore.search_data_ids).

The available search parameters for a specific data store can be inspected with 
[`MultiSourceDataStore.get_search_params_schema`](api/#xcube_multistore.multistore.MultiSourceDataStore.get_search_params_schema).

An example demonstrating the use of these functions is available in 
[setup_config.ipynb](examples/setup_config.ipynb).

## How to Add New Datasets

New datasets can be added in the `datasets` section of the configuration file. Each 
dataset definition must follow the structure of a [dataset object](config.md#dataset-object).

This section demonstrates two common workflows:

1. Opening a dataset using the data store's native opening parameters and capabilities.
2. Accessing a static Zarr data cube and resampling it to a user-defined grid using the Multi-Source Data Store's built-in resampling functionality.

### A Sentinel-2 L2A Data Cube Using Opening Parameters

The example below shows how to configure access to a Sentinel-2 L2A dataset hosted on 
the Planetary Computer.

First, add the corresponding data store:

```yaml
datastores:
  - identifier: stac-ardc
    store_id: stac-pc-ardc
```

The `identifier` is a user-defined name that is referenced throughout the configuration. 
The `store_id` refers to the underlying xcube data store implementation.

All available (installed) data store IDs can be listed using:

```python
MultiSourceDataStore.list_data_store_ids()
```

The parameters accepted by a given data store can be inspected with:

```python
MultiSourceDataStore.get_data_store_params_schema("<store-id>")
```

For example, to access Sentinel-2 data via the Copernicus Data Space Ecosystem (CDSE) 
STAC service instead of the Planetary Computer, the configuration could look as follows:

```yaml
datastores:
  - identifier: stac-ardc
    store_id: stac-cdse-ardc
    store_params:
      key: <CDSE-S3-key>
      secret: <CDSE-S3-secret>
```

Next, add the dataset definition to the `datasets` section:

```yaml
datasets:
  - identifier: sen2l2a
    store: stac-ardc
    data_id: sentinel-2-l2a
    open_params:
      bbox:
        - 9.7
        - 53.3
        - 10.3
        - 53.8
      time_range:
        - 2020-07-15
        - 2020-08-01
      spatial_res: 10 / 111320  # 10 m in degrees
      crs: EPSG:4326
```

The `identifier` must be unique among all datasets. It is also used as the output 
filename (data ID) when storing the final data cube.

The `store` parameter references the configured data store identifier, while `data_id`
specifies the dataset to open within that store.

Available dataset identifiers can be listed using:

```python
MultiSourceDataStore.list_data_ids(...)
```

Supported opening parameters for a specific dataset can be inspected with:

```python
MultiSourceDataStore.get_open_data_params_schema(...)
```

An example demonstrating the use of auxiliary functions is available in 
[setup_config.ipynb](examples/setup_config.ipynb).

### A Static Dataset and Resampling It

The following example demonstrates how to access a dataset from the 
[EO Forest STAC](https://simonbesnard1.github.io/eoforeststac/) and resample it to the grid of the Sentinel-2 data cube defined 
in the previous section.

First, configure the STAC data store:

```yaml
datastores:
  - identifier: store_foresteo
    store_id: stac
    store_params:
      url: https://s3.gfz-potsdam.de/dog.atlaseo-glm.eo-gridded-data/collections/public/catalog.json
```

Next, add the dataset definition:

```yaml
datasets:
  - identifier: AU-How/SAATCHI_BIOMASS
    store: store_foresteo
    data_id: biomass-carbon/SAATCHI_BIOMASS/SAATCHI_BIOMASS_v2.0/SAATCHI_BIOMASS_v2.0.json
    grid_mapping: sen2l2a
```

The available `data_id` values can be explored using:

```python
MultiSourceDataStore.list_data_ids(...)
```

The `grid_mapping` parameter references the identifier of another dataset or grid 
mapping definition. In this example, the biomass dataset is reprojected and resampled 
to the grid of the `sen2l2a` dataset.

Alternatively, a dedicated grid mapping can be defined in the 
[grid mappings section](config.md#grid-mapping-object) and referenced by its identifier.

Examples of custom grid mappings can be found in:

* [Example 2](examples/example2/config.yml)
* [Example 4](examples/example4/config.yml)

## How to Add Local Data Sources to the Configuration

Local datasets can easily be added from the local file system or from private object 
storage by using xcube's [filesystem-based data stores](https://xcube.readthedocs.io/en/latest/dataaccess.html#filesystem-based-data-stores), 
such as `file` or `s3`.

For example, a local Zarr dataset can be added to the configuration as follows:

```yaml
datasets:
  - identifier: local_dataset
    store: local_store
    grid_mapping: globe
    data_id: "dataset.zarr"

data_stores:
  # Storage data store
  - identifier: storage
    store_id: file
    store_params:
      root: data

  # Local source data store
  - identifier: local_store
    store_id: file
    store_params:
      root: local

grid_mappings:
  - identifier: globe
    bbox: [-180, -90, 180, 90]
    spatial_res: 0.1
    crs: EPSG:4326
    tile_size: [1800, 1800]
```

The same configuration can be used with an S3-compatible object storage by replacing 
the `file` data store with an `s3` data store:

```yaml
- identifier: local_store
  store_id: s3
  store_params:
    root: <s3-bucket>
    storage_options:
      anon: False
      key: <S3-key>
      secret: <S3-secret>
```

An example workflow is provided in [Example 4](examples/example4/config.yml). In this 
example, the LAI dataset from Zenodo is first prepared locally using [prepare_laiv3.ipynb](examples/example4/prepare_laiv3.ipynb). 
The resulting local dataset is then incorporated into the cube generation workflow 
defined in [Example 4](examples/example4/config.yml).


## How to Adjust the Time Axis

Some data stores allow specifying a time range directly in the opening parameters.  
For example, Sentinel-2 and Sentinel-3 retrievals using `stac-pc-ardc` and 
`stac-pc-cdse` support time filtering, as shown in [Example 1](examples/example1/config.yml).

To check whether a specific dataset supports time-related opening parameters, use:

```python
MultiSourceDataStore.get_open_data_params_schema(...)
```

Additionally, datasets can be resampled along the time axis using the 
[`temporal_resample_params`](config/#temporal_resample_params) option. This allows the temporal resolution of a 
dataset to be adjusted to a user-defined frequency and aggregation method.

An example from [Example 4](examples/example4/config.yml) is shown below:

```yaml
- identifier: laiv3
  store: local_laiv3
  grid_mapping: globe
  data_id: "GlobMapLAIV3.zarr"
  temporal_resample_params:
    frequency: "1MS"
    agg_methods: "mean"
  format_id: netcdf
```


## How to Modify the Spatial Extent and Resolution

There are several ways to specify the spatial extent and resolution of a dataset.

First, users can define grid mappings in the [`grid_mappings`](config/#entire-configuration-schema) section of the 
configuration file. A grid mapping allows specifying the spatial extent, spatial 
resolution, coordinate reference system, and tile size. For example:

```yaml
grid_mappings:
  - identifier: globe
    bbox: [-180, -90, 180, 90]
    spatial_res: 0.1
    crs: EPSG:4326
    tile_size: [1800, 1800]

  - identifier: france
    bbox: [3400000, 2300000, 4150000, 3050000]
    spatial_res: 30
    crs: EPSG:3035
    tile_size: 2048
```

Multiple grid mappings can be defined within a single configuration. The grid mapping 
`identifier` can then be referenced in the [`datasets`](config/#entire-configuration-schema) 
section, allowing different datasets within the same configuration to use different 
spatial grids.

Second, some data stores support specifying the spatial extent, resolution, and 
coordinate reference system directly through opening parameters. Examples include 
Sentinel-2 and Sentinel-3 retrievals via `stac-pc-ardc` and `stac-cdse-ardc` 
(see [Example 1](examples/example1/config.yml)), as well as ERA5 retrievals through the `cds` data store 
(see [Example 3](examples/example3/config.yml)).

To check whether a specific data store supports adjusting the spatial extent and 
resolution through opening parameters, use:

```python
MultiSourceDataStore.get_open_data_params_schema(...)
```

