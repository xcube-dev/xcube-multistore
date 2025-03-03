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

from typing import Any

import fsspec
import yaml
from xcube.util.jsonschema import (
    JsonArraySchema,
    JsonNumberSchema,
    JsonObjectSchema,
    JsonStringSchema,
    JsonBooleanSchema,
)

from .constants import NAME_WRITE_STORE


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
SCHEMA_FORTMAT_ID = JsonStringSchema(
    title="Desired format of the saved datacube.",
    description="netcdf and zarr are available.",
    default="zarr",
)

SCHEMA_CUSTOM_PROCESSING = JsonObjectSchema(
    properties=dict(
        module_path=JsonStringSchema(min_length=1),
        function_name=JsonStringSchema(min_length=1),
    ),
    required=["module_path", "function_name"],
    additional_properties=False,
)
SCHEMA_DATASET = JsonObjectSchema(
    properties=dict(
        identifier=SCHEMA_IDENTIFIER,
        store=SCHEMA_STORE_IDENTIFIER,
        grid_mapping=SCHEMA_GRIDMAPPING_ID,
        data_id=SCHEMA_DATA_ID,
        open_params=SCHEMA_OPEN_PARAMS,
        format_id=SCHEMA_FORTMAT_ID,
        custom_processing=SCHEMA_CUSTOM_PROCESSING,
    ),
    required=["identifier", "store", "data_id"],
    additional_properties=False,
)

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
SCHEMA_GRID_MAPPING = JsonObjectSchema(
    properties=dict(
        identifier=SCHEMA_IDENTIFIER,
        bbox=SCHEMA_BBOX,
        crs=SCHEMA_CRS,
        spatial_res=SCHEMA_SPATIAL_RES,
    ),
    required=["bbox", "crs", "spatial_res"],
    additional_properties=False,
)

# define schema for general
SCHEMA_VISUALIZE = JsonBooleanSchema(
    title="Switch between visualization in table, if True, and logging, if False.",
    default=True,
)
SCHEMA_GENERAL = JsonObjectSchema(
    properties=dict(visualize=SCHEMA_VISUALIZE),
    required=[],
    additional_properties=False,
)

# define general config schema
CONFIG_SCHEMA = JsonObjectSchema(
    properties=dict(
        datasets=JsonArraySchema(items=SCHEMA_DATASET),
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
        self.datasets = config["datasets"]
        self.data_stores = config["data_stores"]
        self.grid_mappings = config.get("grid_mappings", None)
        self.general = config.get(
            "general",
            dict(visualize=True) if is_jupyter_notebook() else dict(visualize=False),
        )
        assert NAME_WRITE_STORE in [
            store["identifier"] for store in self.data_stores
        ], f"store with identifier {NAME_WRITE_STORE!r} needs to ge given."

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
    except NameError:
        return False
