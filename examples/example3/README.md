## Data Extraction

This directory contains the workflow used to generate the data cubes for the SCS. The resulting data cubes are stored in the local `data/` directory and serve as the input for the subsequent scientific analysis.

The output location is configured via the `storage` data store defined in [config.yml](config.yml). This choice is fully user-configurable and can be replaced with any supported file-system-based storage backend (e.g. `"file"` or `"s3"`). The only requirement is that a writable data store with the identifier `storage` is available. For further details, see the [xcube-multistore configuration documentation](https://xcube-dev.github.io/xcube-multistore/config/#entire-configuration-schema).

### How to Generate the Data Cubes

#### Set up the Environment

The recommended approach is to create a Conda environment using the provided environment specification:
```bash
conda/mamba env create -f environment.yml
```
The corresponding file can be found here: [environment.yml](environment.yml)

After creation, activate the environment:
```bash
conda activate eo-lincs-scs3
```


Next, accessing ERA5 reanalysis data via the Copernicus Data Store (CDS) requires a valid CDS API key. This can be obtained by following the instructions in the [xcube-cds documentation](https://github.com/xcube-dev/xcube-cds#obtain-a-cds-personal-access-token).

Once obtained, the credentials must be added to the configuration file, which will be described in the notebook.

#### Execute the Cube Generation Pipeline

The main workflow is implemented in `data_extraction.ipynb`, which provides a step-by-step guide through the full cube generation process.

> **NOTE**: `sites.csv` is required for this science case which provides 
> the individual flux tower sites that we need to extract the data from.