## Data Extraction

This directory contains the workflow used to generate the data cubes for the SCS. The resulting data cubes are stored in the top-level `data/` directory and serve as the input for the subsequent scientific analysis.

The output location is configured via the `storage` data store defined in [config.yml](config.yml). This choice is fully user-configurable and can be replaced with any supported file-system-based storage backend (e.g. `"file"` or `"s3"`). The only requirement is that a writable data store with the identifier `storage` is available. For further details, see the [xcube-multistore configuration documentation](https://xcube-dev.github.io/xcube-multistore/config/#entire-configuration-schema).


The module [utils.py](utils.py) provides helper functions that are applied during the cube 
generation process, after opening the data sources.

### How to Generate the Data Cubes

#### Set up the Environment

The recommended approach is to create a Conda environment using the provided environment specification:

```bash
conda/mamba env create -f environment.yml
```
The corresponding file can be found here: [environment.yml](environment.yml)

After creation, activate the environment:
```bash
conda activate eo-lincs-scs2
```

Next, access to CLMS data requires authentication credentials for the CLMS API, as described in the [xcube-clms documentation](https://github.com/xcube-dev/xcube-clms#create-credentials-to-access-the-clms-api).

These credentials should be stored in a JSON file (in this example `clms-credentials.json`) and are loaded during the cube generation process as specified in the configuration file [config.yml](config.yml). Note that both the filename and the relative path to the credentials file are user-defined and can be adjusted if needed, provided they are correctly referenced in the configuration.

#### Execute the Cube Generation Pipeline

The main workflow is implemented in [data_extraction.ipynb](data_extraction.ipynb), which provides a step-by-step guide through the full cube generation process.
