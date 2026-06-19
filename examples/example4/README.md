## Data Extraction

This directory contains the workflow used to generate the data cubes for the SCS. The resulting data cubes are stored in the local `data/` directory and serve as the input for the subsequent scientific analysis.

The output location is configured via the `storage` data store defined in [config.yml](config.yml). This choice is fully user-configurable and can be replaced with any supported file-system-based storage backend (e.g. `"file"` or `"s3"`). The only requirement is that a writable data store with the identifier `storage` is available. For further details, see the [xcube-multistore configuration documentation](https://xcube-dev.github.io/xcube-multistore/config/#entire-configuration-schema).


The module [utils.py](utils.py) provides helper functions that are applied during the cube 
generation process, after opening the data sources.

### How to Generate the Data Cubes

#### Set up the Environment

The recommended approach is to create a Conda environment using the provided environment specification:

`conda env create -f environment.yml`

The corresponding file can be found here: [environment.yml](environment.yml)

After creation, activate the environment:

`conda activate eo-lincs-scs4`

Next, to access the FLUXCOM-X-BASE data from the [ICOS Data Portal](https://www.icos-cp.eu/data-services/about-data-portal), users 
must [create an account](https://cpauth.icos-cp.eu/login/?targetUrl=https%3A%2F%2Fwww.icos-cp.eu%2Fdata-services%2Fabout-data-portal) and provide their registered email address and 
password to the configuration YAML [config.yml#L71](config.yml#L71).


### Execute the Cube Generation Pipeline


The cube generation workflow is split into two notebooks:

1. The **GLOBMAP Global Leaf Area Index (LAI) Dataset Since 1981** must be prepared in advance. To do so, run the notebook [prepare_lai.ipynb](prepare_laiv3.ipynb). 
2. Run the notebook [data_extraction.ipynb](data_extraction.ipynb), which consists of a step-by-step guide to generate the data cubes  
