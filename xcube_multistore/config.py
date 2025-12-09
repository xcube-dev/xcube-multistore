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

import fsspec
import yaml
from xcube.core.store import list_data_store_ids
from xcube.util.jsonschema import (
    JsonArraySchema,
    JsonBooleanSchema,
    JsonComplexSchema,
    JsonNumberSchema,
    JsonObjectSchema,
    JsonStringSchema,
)

from .constants import NAME_WRITE_STORE
from .utils import _remove_compressed_extension

SCHEMA_IDENTIFIER = JsonStringSchema(title="Identifier for the object", min_length=1)

# define schema for dataset
SCHEMA_STORE_IDENTIFIER = JsonStringSchema(
    title="Store identifier used to open the dataset", min_length=1
)
SCHEMA_GRIDMAPPING_ID = JsonStringSchema(
    title="Grid mapping identifier for the dataset", min_length=1
)
SCHEMA_DATA_ID = JsonStringSchema(
    title="Data ID of the dataset in the assigned data store", min_length=1
)
SCHEMA_OPEN_PARAMS = JsonObjectSchema(
    title="Open data parameters", additional_properties=True
)
SCHEMA_XR_MERGE_PARAMS = JsonObjectSchema(
    title="`xarry.merge parameters`",
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
SCHEMA_FORMAT_ID = JsonStringSchema(
    title="Desired format of the saved datacube.",
    default="zarr",
    enum=["netcdf", "zarr", "levels"],
)
SCHEMA_CHUNKSIZE = JsonObjectSchema(
    title="Mapping from dimension to chunk size.",
    required=[],
    additional_properties=True,
)
SCHEMA_CUSTOM_PROCESSING = JsonObjectSchema(
    properties=dict(
        module_path=JsonStringSchema(
            title="Path to python module relative to config file.", min_length=1
        ),
        function_name=JsonStringSchema(
            title="Name of function in the python module.",
            description=(
                "Any function is allowed with takes a `xarray.Dataset` as an "
                "input and returns a modified xarray.Dataset."
            ),
            min_length=1,
        ),
    ),
    required=["module_path", "function_name"],
    additional_properties=False,
)
SCHEMA_SPATIAL_RESAMPLE_PARAMS = JsonObjectSchema(
    title="Resample parameters used for xcube_resampling.resample_in_space",
    description="For further information on the parameters refer to "
    "https://xcube-dev.github.io/xcube-resampling/api/#xcube_resampling.resample_in_space.",
    properties=dict(
        variables=JsonComplexSchema(
            title="A single variable name or iterable of variable names to be "
            "resampled. If None, all data variables will be processed.",
            any_of=[JsonStringSchema(), JsonArraySchema()],
        ),
        interp_methods=JsonComplexSchema(
            title="Optional interpolation method to be used for upsampling spatial "
            "data variables.",
            description="Interpolation method to be used. Can be a single "
            "interpolation method for all variables or a dictionary "
            "mapping variable names or dtypes to interpolation "
            "method.",
            any_of=[
                JsonStringSchema(enum=["nearest", "triangular", "bilinear"]),
                JsonNumberSchema(
                    description="0 => nearest neighbor " "1 => linear/bilinear",
                    enum=[0, 1],
                ),
                JsonObjectSchema(
                    description="A dictionary mapping variable names or dtypes to "
                    "interpolation as shown in the enum in the sibling "
                    "schema.",
                ),
            ],
            default="0 for integer arrays, else 1",
        ),
        agg_methods=JsonComplexSchema(
            title="Optional aggregation methods for downsampling spatial " "variables.",
            any_of=[
                JsonStringSchema(
                    enum=[
                        "center",
                        "count",
                        "first",
                        "last",
                        "max",
                        "mean",
                        "median",
                        "mode",
                        "min",
                        "prod",
                        "std",
                        "sum",
                        "var",
                    ]
                ),
                JsonObjectSchema(
                    description="A dictionary mapping variable names or dtypes to"
                    " methods as shown in the enum in the sibling "
                    "schema."
                ),
            ],
            default="Defaults to 'center' for integer arrays, else 'mean'.",
        ),
        recover_nans=JsonComplexSchema(
            title="Optional boolean or mapping to enable NaN recovery during "
            "upsampling (only applies when interpolation method is not "
            "`nearest`).",
            any_of=[
                JsonBooleanSchema(),
                JsonObjectSchema(
                    description="A dictionary mapping variable names or dtypes to "
                    "booleans"
                ),
            ],
            default="False",
        ),
        fill_values=JsonComplexSchema(
            title="Optional fill value(s) for areas outside input coverage.",
            any_of=[
                JsonNumberSchema(),
                JsonObjectSchema(description="A dictionary by variable or type"),
            ],
            default="Defaults based on data type are used:"
            " `float`: `NaN`, "
            "  `uint8`: `255`, "
            "  `uint16`: `65535`, "
            "  `other ints`: `-1`",
        ),
        tile_size=JsonComplexSchema(
            title="Optional tile size used when generating a regular grid from "
            "an irregular source grid mapping. Only used if `target_gm` is not "
            "provided.",
            any_of=[
                JsonNumberSchema(),
                JsonArraySchema(items=JsonNumberSchema(), min_items=2, max_items=2),
            ],
        ),
    ),
)
SCHEMA_TEMPORAL_RESAMPLE_PARAMS = JsonObjectSchema(
    title="Resample parameters used for xcube_resampling.resample_in_time",
    description="For further information on the parameters refer to "
    "https://xcube-dev.github.io/xcube-resampling/api/#xcube_resampling.resample_in_time.",
    properties=dict(
        frequency=JsonStringSchema(
            title="Target temporal frequency",
            description="Target temporal frequency, following Pandas period aliases. "
            "Format `<count><period>`, where `<period>` may be one of 's', 'min', 'h', "
            "'D', 'W', 'M', 'Q', 'Y'.",
        ),
        variables=JsonComplexSchema(
            title="A single variable name or iterable of variable names to be "
            "resampled. If None, all data variables will be processed.",
            any_of=[JsonStringSchema(), JsonArraySchema()],
        ),
        interp_methods=JsonComplexSchema(
            title="Optional interpolation method(s) for upsampling.",
            any_of=[JsonStringSchema(), JsonArraySchema(), JsonObjectSchema()],
        ),
        agg_methods=JsonComplexSchema(
            title="Optional aggregation method(s) for downsampling.",
            any_of=[JsonStringSchema(), JsonArraySchema(), JsonObjectSchema()],
        ),
        offset=JsonStringSchema(
            title="offset to adjust resampled time labels. Uses the same syntax "
            "as frequency."
        ),
        tolerance=JsonStringSchema(
            title="maximum allowed distance for selective downsampling methods (e.g., "
            "`'backfill'`, `'ffill'`, `'nearest'`). Defaults to the resampling "
            "frequency."
        ),
    ),
)
SCHEMA_DATA_VARIABLE = JsonObjectSchema(
    properties=dict(
        identifier=SCHEMA_IDENTIFIER,
        store=SCHEMA_STORE_IDENTIFIER,
        data_id=SCHEMA_DATA_ID,
        open_params=SCHEMA_OPEN_PARAMS,
        custom_processing=SCHEMA_CUSTOM_PROCESSING,
        spatial_resample_params=SCHEMA_SPATIAL_RESAMPLE_PARAMS,
        temporal_resample_params=SCHEMA_TEMPORAL_RESAMPLE_PARAMS,
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
        format_id=SCHEMA_FORMAT_ID,
        chunksize=SCHEMA_CHUNKSIZE,
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
        format_id=SCHEMA_FORMAT_ID,
        chunksize=SCHEMA_CHUNKSIZE,
        custom_processing=SCHEMA_CUSTOM_PROCESSING,
        spatial_resample_params=SCHEMA_SPATIAL_RESAMPLE_PARAMS,
        temporal_resample_params=SCHEMA_TEMPORAL_RESAMPLE_PARAMS,
    ),
    required=["identifier", "store", "data_id"],
    additional_properties=False,
)
SCHEMA_DATASET = JsonComplexSchema(one_of=[SCHEMA_SINGLE_DATASET, SCHEMA_MULTI_DATASET])

# define schema for data store
SCHEMA_STORE_ID = JsonStringSchema(
    title="Store identifier",
    enum=list_data_store_ids(),
)
SCHEMA_STORE_PARAMS = JsonObjectSchema(
    title="Store parameters",
    description=(
        "Store parameters can be obtained by `get_data_store_params_schema(store_id)`. "
        "Further documentation can be found at "
        "https://xcube.readthedocs.io/en/latest/dataaccess.html#data-store-framework."
    ),
    additional_properties=True,
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
    title="Spatial chunk size in grid mapping.",
    one_of=[
        JsonArraySchema(
            items=(
                JsonNumberSchema(title="Chunk size in lat/x direction."),
                JsonNumberSchema(title="Chunk size in lon/y direction."),
            ),
        ),
        JsonNumberSchema(title="Squared chunk size"),
    ],
    default=1024,
)

SCHEMA_GRID_MAPPING = JsonObjectSchema(
    properties=dict(
        identifier=SCHEMA_IDENTIFIER,
        bbox=SCHEMA_BBOX,
        crs=SCHEMA_CRS,
        spatial_res=SCHEMA_SPATIAL_RES,
        tile_size=SCHEMA_TILE_SIZE,
    ),
    required=["identifier", "bbox", "crs", "spatial_res"],
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
SCHEMA_GDAL_HTTP_PARAMS = JsonObjectSchema(
    properties=dict(
        gdal_http_max_retry=JsonNumberSchema(
            title="Maximal number of retries of a http request in GDAL.",
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
            config = _read_yaml(config)
        schema = self.get_schema()
        schema.validate_instance(config)
        self.original: dict = config
        self.preload_datasets = config.get("preload_datasets", None)
        self.datasets = {
            config_ds["identifier"]: config_ds for config_ds in config["datasets"]
        }
        self.preload_map = self._get_preload_map()
        self.data_stores = {
            config_store["identifier"]: config_store
            for config_store in config["data_stores"]
        }
        grid_mappings = config.get("grid_mappings")
        if grid_mappings:
            self.grid_mappings = {
                config_gm["identifier"]: config_gm for config_gm in grid_mappings
            }
        else:
            self.grid_mappings = None
        self.general = config.get("general", {})
        self._general_setup()

        assert (
            NAME_WRITE_STORE in self.data_stores.keys()
        ), f"store with identifier {NAME_WRITE_STORE!r} needs to be given."

    def _general_setup(self):
        if "visualize" not in self.general:
            self.general["visualize"] = True
        if "force_preload" not in self.general:
            self.general["force_preload"] = False
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
                data_id_mod = _remove_compressed_extension(data_id)
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


def _read_yaml(config_path: str) -> dict[str, Any]:
    with fsspec.open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config


def _is_jupyter_notebook():
    try:
        from IPython import get_ipython

        return get_ipython() is not None
    except ModuleNotFoundError:
        return False
