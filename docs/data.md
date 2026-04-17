# Data Availability

xcube provides unified access to a wide range of Earth System datasets through its
**Data Store framework**, exposed via
[xcube data store plugins](https://xcube.readthedocs.io/en/latest/dataaccess.html#data-store-framework).
These plugins can be seamlessly integrated into the Multi-Source Data Store.


### Public Data Sources

The following data store plugins provide direct access to major Earth observation
and research data providers.

| Data Source                                                                 | Store ID                | Plugin Repository                                          |
|-----------------------------------------------------------------------------|------------------------|------------------------------------------------------------|
| **[Copernicus Climate Data Store (CDS)](https://cds.climate.copernicus.eu/)** | `"cds"`                | [GitHub](https://github.com/xcube-dev/xcube-cds)            |
| **[Copernicus Marine Service (CMEMS)](https://marine.copernicus.eu/)**       | `"cmems"`              | [GitHub](https://github.com/xcube-dev/xcube-cmems)          |
| **[Copernicus Land Monitoring Service (CLMS)](https://land.copernicus.eu/en/dataset-catalog)** | `"clms"` | [GitHub](https://github.com/xcube-dev/xcube-clms)           |
| **[EOPF Sentinel Zarr Samples](https://zarr.eopf.copernicus.eu/)**           | `"eopf-zarr"`          | [GitHub](https://github.com/EOPF-Sample-Service/xcube-eopf) |
| **[ESA CCI](https://climate.esa.int/en/data/#/dashboard)**                   | `"cciodp"`<br>`"ccizarr"` | [GitHub](https://github.com/xcube-dev/xcube-cci)        |
| **[ESA SMOS](https://earth.esa.int/eogateway/missions/smos)**                | `"smos"`               | [GitHub](https://github.com/xcube-dev/xcube-smos)           |
| **[ICOS Data Portal](https://www.icos-cp.eu/data-services)**                 | `"icosdp"`             | [GitHub](https://github.com/xcube-dev/xcube-icosdp)         |
| **[Global Ecosystem Dynamics Investigation (GEDI)](https://gedi.umd.edu/)**  | `"gedi"`               | [GitHub](https://github.com/xcube-dev/xcube-gedi)           |
| **[Sentinel Hub](https://www.sentinel-hub.com/)**                            | `"sentinelhub"`        | [GitHub](https://github.com/xcube-dev/xcube-sh)             |
| **[SpatioTemporal Asset Catalog (STAC)](https://stacspec.org/en/about/datasets/)** | `"stac"`        | [GitHub](https://github.com/xcube-dev/xcube-stac)           |
| **[Zenodo](https://zenodo.org/)**                                            | `"zenodo"`             | [GitHub](https://github.com/xcube-dev/xcube-zenodo)         |

Each plugin repository contains usage examples and documentation specific to the
respective data provider.

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