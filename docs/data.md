# Data Availability

xcube provides unified access to a wide range of Earth System datasets through its
**Data Store framework**, exposed via
[xcube data store plugins](https://xcube.readthedocs.io/en/latest/dataaccess.html#data-store-framework).
These plugins can be seamlessly integrated into the Multi-Source Data Store.


### Public Data Sources

The following data store plugins provide direct access to major Earth observation
and research data providers.

# EO Data Sources and Variables via xcube Plugins
| **Plugin** | **Store ID** | **Data Source** | **Key Variables / Products**                                                                       | **Domain** |
|---|---|---|----------------------------------------------------------------------------------------------------|---|
| **[xcube-sh](https://github.com/xcube-dev/xcube-sh)** | `sentinelhub` | Sentinel Hub | Sentinel-1 GRD                                                                                     | Land, Forestry |
| | `sentinelhub` | Sentinel Hub | Sentinel-2 L1C/L2A                                                                                 | Land, Agriculture |
| | `sentinelhub` | Sentinel Hub | Sentinel-3 OLCI/SLSTR                                                                              | Ocean, Land |
| | `sentinelhub` | Sentinel Hub | Sentinel-5P L2                                                                                     | Atmosphere |
| **[xcube-smos](https://github.com/xcube-dev/xcube-smos)** | `smos` | ESA SMOS L2 | Soil moisture, ocean salinity                                                                      | Land, Ocean |
| **[xcube-cci](https://github.com/esa-cci/xcube-cci)** | `cciodp`, `ccizarr` | ESA Climate Change Initiative | Land cover, biomass, soil moisture, fires, GHGs, clouds, sea level, glaciers, and more             | Multi-domain |
| **[xcube-cds](https://github.com/xcube-dev/xcube-cds)** | `cds` | ERA5 / ERA5-Land | Temperature, precipitation, wind, radiation, soil moisture, snow                                   | Climate, Hydrology |
| | `cds` | Copernicus Soil Moisture / Sea Ice | Soil moisture, sea ice thickness                                                                   | Land, Cryosphere |
| | `cds` | Drought indices | SPI, SPEI                                                                                          | Land |
| | `cds` | Land cover classification | LCCS by UN FAO                                                                                     | Land |
| | `cds` | Land surface temperature | Land surface temperature day and night                                                             | Land |
| | `cds` | Surface albedo | Broadband and spectral surface albedo                                                              | Land |
| **[xcube-cmems](https://github.com/xcube-dev/xcube-cmems)** | `cmems` | Copernicus Marine Service | Sea surface temperature, salinity, ocean currents, biogeochemistry, sea level, chlorophyll         | Ocean |
| **[xcube-stac](https://github.com/xcube-dev/xcube-stac)** | `stac` | SpatioTemporal Asset Catalog (STAC) | Dependent on catalog                                                                               | Multi-domain |
| | `stac` | CDSE | Sentinel-2 L1C/L2A, Sentinel-3 SYNERGY, Sentinel-3 SLSTR LST                                       | Land, Ocean |
| | `stac` | Planetary Computer | Sentinel-2 L1C/L2A, Sentinel-3 SYNERGY, , Sentinel-3 SLSTR LST, Harmonized Sentinel-2 and Landsat  | Land, Ocean |
| **[xcube-clms](https://github.com/xcube-dev/xcube-clms)** | `clms` | Copernicus Land Monitoring Service (CLMS) | Land cover, imperviousness, vegetation phenology, forest characteristics                           | Land, Urban |
| **[xcube-zenodo](https://github.com/xcube-dev/xcube-zenodo)** | `zenodo` | Zenodo Data Portal | Scientific datasets, model outputs, benchmark products                                             | Open Research |
| **[xcube-icosdp](https://github.com/xcube-dev/xcube-icosdp)** | `icosdp` | ICOS Carbon Portal | GHG fluxes, atmospheric concentrations, meteorological variables                                   | Atmosphere, Land |
| **[xcube-gedidb](https://github.com/xcube-dev/xcube-gedidb)** | `gedi` | GEDI L2A, L2B, L4A, L4C products | Aboveground biomass density, canopy cover, DEM                                                     | Land, Forest |
| **[xcube-eopf](https://github.com/xcube-dev/xcube-eopf)** | `eopf-zarr` | EOPF Sentinel Zarr Samples | Sentinel-1 GRD, Sentinel-1 SLC, Sentinel-1 OCN                                                     | Land, Forestry, Ocean |
| | `eopf-zarr` | EOPF Sentinel Zarr Samples | Sentinel-2 L1C/L2A                                                                                 | Land, Agriculture |
| | `eopf-zarr` | EOPF Sentinel Zarr Samples | Sentinel-3 OLCI, SLSTR-RBT, SLSTR-LST                                                              | Ocean, Land |

### Bring Your Own Data

In addition to public data services, xcube allows you to integrate your own datasets
into the same framework. The built-in [filesystem-based data stores](https://xcube.readthedocs.io/en/latest/dataaccess.html#filesystem-based-data-stores)
enable access to:

- Local datasets via `"file"`
- Private S3 buckets via `"s3"`

This allows you to seamlessly combine external services and private datasets within
the xcube Multi-Source Data Store.

### Learn More

To configure and explore available datasets:

- See the [Setup Configuration YAML example](examples/setup_config.ipynb)
- Explore the [Python API documentation](api.md)