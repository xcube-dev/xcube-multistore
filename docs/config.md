# Configuration Guide

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

## Configuration Schema

Each item in the schema is represented by a user defined `identifier`, which is used 
to identify the assigned object in the multi-source data store ecosystem. Furthermore, 
the configuration file consists of the following sections: 

- `datasets` (Required): list of dataset objects.
- `preload_dataset` (Optional): list of preload objects.
- `data_store` (Required): list of data store objects.
- `grid_mappings` (Optional): list of grid mapping objects.
- `general` (Optional): mapping (dictionary) of general configuration parameters.

### `datasets` (Required)

The `datasets` section consists if a list of dataset configuration which defines the 
source and the processing step to be applied to the dataset. Two different dataset 
types are available: 
1. **Single Dataset Schema**: represents a dataset from a single data source
2. **Multi Dataset Schema**: allows to merge multiple datasets from multiple 
data sources into one dataset

An example for the **Single Dataset Schema** is shown below:
```yaml
identifier: <string>
store: <string>
data_id: <string>
grid_mapping: <string>
open_params: <object>
format_id: <string>
custom_processing:
  module_path: <string>
  function_name: <string>
```

An example for the **Multi Dataset Schema** is shown below:
```yaml
identifier: <string>
grid_mapping: <string>
format_id: <string>
xr_merge_params: <dict>
variables:
    - identifier: <string>
      store: <string>
      data_id: <string>
      open_params: <dict>
      custom_processing:
        module_path: <string>
        function_name: <string>
```



### `preload_dataset` (Optional)

### `data_store` (Required)

### `grid_mapping` (Optional)

### `general` (Optional)
