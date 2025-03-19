# MIT License
#
# Copyright (c) 2025 Brockmann Consult GmbH
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NON INFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import os
from collections import defaultdict
from typing import Any

import dask
import fsspec
import yaml
from xcube.util.jsonschema import (
    JsonArraySchema,
    JsonBooleanSchema,
    JsonComplexSchema,
    JsonNumberSchema,
    JsonObjectSchema,
    JsonStringSchema,
)

from .constants import NAME_WRITE_STORE
from .utils import remove_compressed_extension

SCHEMA_IDENTIFIER = JsonStringSchema(title="Identifier for the object", min_length=1)

# define schema for dataset
SCHEMA_STORE_IDENTIFIER = JsonStringSchema(
    title="Store used to open the dataset", min_length=1
)
SCHEMA_GRIDMAPPING_ID = JsonStringSchema(
    title="Final grid mapping for the dataset", min_length=1
)
SCHEMA_DATA_ID = JsonStringSchema(
    title="Data ID of the dataset in the assigned data store", min_length=1
)
SCHEMA_OPEN_PARAMS = JsonObjectSchema(
    title="Open data parameters", additional_properties=True
)
SCHEMA_XR_MERGE_PARAMS = JsonObjectSchema(
    title="`xr.merge parameters`",
    description=(
        "See documentation https://docs.xarray.dev/en/stable/generated/xarray.merge.html"
    ),
    default=dict(
        compat="no_conflicts",
        join="exact",
        fill_value="<NA>",
        combine_attrs="drop_conflicts",
    ),
    additional_properties=True,
)
SCHEMA_FORTMAT_ID = JsonStringSchema(
    title="Desired format of the saved datacube.",
    default="zarr",
    enum=["netcdf", "zarr"],
)
SCHEMA_CUSTOM_PROCESSING = JsonObjectSchema(
    properties=dict(
        module_path=JsonStringSchema(min_length=1),
        function_name=JsonStringSchema(min_length=1),
    ),
    required=["module_path", "function_name"],
    additional_properties=False,
)

SCHEMA_DATA_VARIABLE = JsonObjectSchema(
    properties=dict(
        identifier=SCHEMA_IDENTIFIER,
        store=SCHEMA_STORE_IDENTIFIER,
        data_id=SCHEMA_DATA_ID,
        open_params=SCHEMA_OPEN_PARAMS,
        custom_processing=SCHEMA_CUSTOM_PROCESSING,
    ),
    required=["identifier", "store", "data_id"],
    additional_properties=False,
)
SCHEMA_MULTI_DATASET = JsonObjectSchema(
    properties=dict(
        identifier=SCHEMA_IDENTIFIER,
        variables=JsonArraySchema(
            title="List of data variables",
            items=SCHEMA_DATA_VARIABLE,
        ),
        grid_mapping=SCHEMA_GRIDMAPPING_ID,
        format_id=SCHEMA_FORTMAT_ID,
        xr_merge_params=SCHEMA_XR_MERGE_PARAMS,
    ),
    required=["identifier", "variables"],
    additional_properties=False,
)
SCHEMA_SINGLE_DATASET = JsonObjectSchema(
    properties=dict(
        identifier=SCHEMA_IDENTIFIER,
        store=SCHEMA_STORE_IDENTIFIER,
        grid_mapping=SCHEMA_GRIDMAPPING_ID,
        data_id=SCHEMA_DATA_ID,
        open_params=SCHEMA_OPEN_PARAMS,
        format_id=SCHEMA_FORTMAT_ID,
        xr_merge_params=SCHEMA_XR_MERGE_PARAMS,
        custom_processing=SCHEMA_CUSTOM_PROCESSING,
    ),
    required=["identifier", "store", "data_id"],
    additional_properties=False,
)
SCHEMA_DATASET = JsonComplexSchema(one_of=[SCHEMA_SINGLE_DATASET, SCHEMA_MULTI_DATASET])

# define schema for data store
SCHEMA_STORE_ID = JsonStringSchema(title="Store identifier", default="EPSG:4326")
SCHEMA_STORE_PARAMS = JsonObjectSchema(
    title="STORE parameters", additional_properties=True
)
SCHEMA_STORE = JsonObjectSchema(
    properties=dict(
        identifier=SCHEMA_IDENTIFIER,
        store_id=SCHEMA_STORE_ID,
        store_params=SCHEMA_STORE_PARAMS,
    ),
    required=["identifier", "store_id"],
    additional_properties=False,
)

# define schema for grid mapping
SCHEMA_SPATIAL_RES = JsonNumberSchema(title="Spatial Resolution", exclusive_minimum=0.0)
SCHEMA_CRS = JsonStringSchema(title="Coordinate reference system", default="EPSG:4326")
SCHEMA_BBOX = JsonArraySchema(
    items=(
        JsonNumberSchema(),
        JsonNumberSchema(),
        JsonNumberSchema(),
        JsonNumberSchema(),
    ),
    title="Bounding box [west, south, east, north]",
)
SCHEMA_TILE_SIZE = JsonComplexSchema(
    title="Spatial size of chunk in grid mapping",
    one_of=[
        JsonArraySchema(
            items=(
                JsonNumberSchema(minimum=256, default=1024),
                JsonNumberSchema(minimum=256, default=1024),
            ),
            default=[1024, 1024],
        ),
        JsonNumberSchema(minimum=256, default=1024),
    ],
)

SCHEMA_GRID_MAPPING = JsonObjectSchema(
    properties=dict(
        identifier=SCHEMA_IDENTIFIER,
        bbox=SCHEMA_BBOX,
        crs=SCHEMA_CRS,
        spatial_res=SCHEMA_SPATIAL_RES,
        tile_size=SCHEMA_TILE_SIZE,
    ),
    required=["bbox", "crs", "spatial_res"],
    additional_properties=False,
)

# define schema for preload dataset
SCHEMA_PRELOAD_PARAMS = JsonObjectSchema(
    title="Preload data parameters", additional_properties=True
)
SCHEMA_PRELOAD_DATASET = JsonObjectSchema(
    properties=dict(
        store=SCHEMA_STORE_IDENTIFIER,
        data_ids=JsonArraySchema(items=SCHEMA_DATA_ID),
        preload_params=SCHEMA_PRELOAD_PARAMS,
    ),
    required=["store", "data_ids"],
    additional_properties=False,
)


# define schema for general
SCHEMA_VISUALIZE = JsonBooleanSchema(
    title="Switch between visualization in table, if True, and logging, if False.",
    default=True,
)
SCHEMA_FORCE_PRELOAD = JsonBooleanSchema(
    description="If True, all data IDS given in section `preload_datasets` will be "
    "preloaded. If False, only non-preloaded datasets will be preloaded.",
    default=True,
)
SCHEMA_DASK_SCHEDULER = JsonStringSchema(
    description="Scheduler mode put into `dask.config.set(scheduler=<scheduler_mode>)`",
    enum=["threads", "processes", "single-threaded", "sync", "distributed"],
    default="threads",
)
SCHEMA_GDAL_HTTP_PARAMS = JsonObjectSchema(
    properties=dict(
        gdal_http_max_retry=JsonNumberSchema(
            title="Maximal retry of a http request in GDAL.",
            description=(
                "This is used when opening a tif file with `rioxarray.open_rasterio` "
                "from a remote source."
            ),
            default=10,
        ),
        gdal_http_retry_delay=JsonNumberSchema(
            title="Delay in seconds between retries of a http request in GDAL.",
            description=(
                "This is used when opening a tif file with `rioxarray.open_rasterio` "
                "from a remote source."
            ),
            default=5,
        ),
    ),
    required=[],
    additional_properties=False,
)
SCHEMA_GENERAL = JsonObjectSchema(
    properties=dict(
        visualize=SCHEMA_VISUALIZE,
        force_preload=SCHEMA_FORCE_PRELOAD,
        dask_scheduler=SCHEMA_DASK_SCHEDULER,
        gdal_http_params=SCHEMA_GDAL_HTTP_PARAMS,
    ),
    required=[],
    additional_properties=False,
)

# define general config schema
CONFIG_SCHEMA = JsonObjectSchema(
    properties=dict(
        datasets=JsonArraySchema(items=SCHEMA_DATASET),
        preload_datasets=JsonArraySchema(items=SCHEMA_PRELOAD_DATASET),
        data_stores=JsonArraySchema(
            items=SCHEMA_STORE,
            title="Used xcube's data stores.",
            description=(
                f"A single data store with identifier {NAME_WRITE_STORE!r} is "
                f"required for writing the final data cubes."
            ),
        ),
        grid_mappings=JsonArraySchema(items=SCHEMA_GRID_MAPPING),
        general=SCHEMA_GENERAL,
    ),
    required=["datasets", "data_stores"],
    additional_properties=False,
)


class MultiSourceConfig:

    def __init__(self, config: str | dict[str, Any]):
        if not isinstance(config, dict):
            config = read_yaml(config)
        schema = self.get_schema()
        schema.validate_instance(config)
        self.preload_datasets = config.get("preload_datasets", None)
        self.datasets = {
            config_ds["identifier"]: config_ds for config_ds in config["datasets"]
        }
        self.preload_map = self._get_preload_map()
        self.data_stores = {
            config_store["identifier"]: config_store
            for config_store in config["data_stores"]
        }
        self.grid_mappings = config.get("grid_mappings", None)

        self.general = config.get("general", {})
        self._general_setup()

        assert (
            NAME_WRITE_STORE in self.data_stores.keys()
        ), f"store with identifier {NAME_WRITE_STORE!r} needs to be given."

    def _general_setup(self):
        if "visualize" not in self.general:
            self.general["visualize"] = True if is_jupyter_notebook() else False
        if "force_preload" not in self.general:
            self.general["force_preload"] = False
        if "dask_scheduler" not in self.general:
            self.general["dask_scheduler"] = "threads"
        _GDAL_HTTP_MAX_RETRY_DEFAULT = 10
        _GDAL_HTTP_RETRY_DELAY_DEFAULT = 5
        if "gdal_http_params" not in self.general:
            self.general["gdal_http_params"] = dict(
                gdal_http_max_retry=_GDAL_HTTP_MAX_RETRY_DEFAULT,
                gdal_http_retry_delay=_GDAL_HTTP_RETRY_DELAY_DEFAULT,
            )
        if "gdal_http_max_retry" not in self.general["gdal_http_params"]:
            self.general["gdal_http_params"][
                "gdal_http_max_retry"
            ] = _GDAL_HTTP_MAX_RETRY_DEFAULT
        if "gdal_http_retry_delay" not in self.general["gdal_http_params"]:
            self.general["gdal_http_params"][
                "gdal_http_retry_delay"
            ] = _GDAL_HTTP_RETRY_DELAY_DEFAULT

        # apply general setup
        dask.config.set(scheduler=self.general["dask_scheduler"])
        os.environ["GDAL_HTTP_MAX_RETRY"] = str(
            self.general["gdal_http_params"]["gdal_http_max_retry"]
        )
        os.environ["GDAL_HTTP_RETRY_DELAY"] = str(
            self.general["gdal_http_params"]["gdal_http_retry_delay"]
        )

    def _get_preload_map(self):
        if self.preload_datasets is None:
            return None
        preload_map = defaultdict(list)
        for config_preload in self.preload_datasets:
            for data_id in config_preload["data_ids"]:
                data_id_mod = remove_compressed_extension(data_id)
                for identifier_ds, config_ds in self.datasets.items():
                    if "variables" in config_ds:
                        for config_da in config_ds["variables"]:
                            if data_id_mod in config_da["data_id"]:
                                preload_map[data_id].append(config_da["data_id"])
                    else:
                        if data_id_mod in config_ds["data_id"]:
                            preload_map[data_id].append(config_ds["data_id"])
        return preload_map

    @classmethod
    def get_schema(cls) -> JsonObjectSchema:
        return CONFIG_SCHEMA


def read_yaml(config_path: str) -> dict[str, Any]:
    with fsspec.open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config


def is_jupyter_notebook():
    try:
        from IPython import get_ipython

        return get_ipython() is not None
    except ModuleNotFoundError:
        return False
