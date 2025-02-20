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

import yaml

from xcube.util.jsonschema import JsonObjectSchema
from xcube.util.jsonschema import (
    JsonArraySchema,
    JsonBooleanSchema,
    JsonDateSchema,
    JsonIntegerSchema,
    JsonNumberSchema,
    JsonObjectSchema,
    JsonStringSchema,
)


SCEMA_IDENTIFIER = JsonStringSchema(title="Identifier for the object", min_length=1)

# define schema for dataset
SCEMA_STORE_IDENTIFIER = JsonStringSchema(
    title="Store used to open the dataset", min_length=1
)
SCEMA_GRIDMAPPING_ID = JsonStringSchema(
    title="Final grid mapping for the dataset", min_length=1
)
SCEMA_DATA_ID = JsonStringSchema(
    title="Data ID of the dataset in the assigned data store", min_length=1
)
SCEMA_OPEN_PARAMS = JsonObjectSchema(
    title="Open data parameters", additional_properties=True
)
SCEMA_FORTMAT_ID = JsonStringSchema(
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
        identifier=SCEMA_IDENTIFIER,
        store=SCEMA_STORE_IDENTIFIER,
        grid_mapping=SCEMA_GRIDMAPPING_ID,
        data_id=SCEMA_DATA_ID,
        open_params=SCEMA_OPEN_PARAMS,
        format_id=SCEMA_FORTMAT_ID,
        custom_processing=SCHEMA_CUSTOM_PROCESSING,
    ),
    required=["identifier", "store", "data_id"],
    additional_properties=False,
)

# define schema for data store
SCEMA_STORE_ID = JsonStringSchema(title="Store identifier", default="EPSG:4326")
SCEMA_STORE_PARAMS = JsonObjectSchema(
    title="STORE parameters", additional_properties=True
)
SCHEMA_STORE = JsonObjectSchema(
    properties=dict(
        identifier=SCEMA_IDENTIFIER,
        store_id=SCEMA_STORE_ID,
        store_params=SCEMA_STORE_PARAMS,
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
        identifier=SCEMA_IDENTIFIER,
        bbox=SCHEMA_BBOX,
        crs=SCHEMA_CRS,
        spatial_res=SCHEMA_SPATIAL_RES,
    ),
    required=["bbox", "crs", "spatial_res"],
    additional_properties=False,
)

# define general copnfig schema
CONFIG_SCHEMA = JsonObjectSchema(
    properties=dict(
        datasets=JsonArraySchema(items=SCHEMA_DATASET),
        data_stores=JsonArraySchema(items=SCHEMA_STORE),
        grid_mappings=JsonArraySchema(items=SCHEMA_GRID_MAPPING),
    ),
    required=["datasets", "data_stores"],
    additional_properties=False,
)


class MultiSourceConfig:

    def __init__(self, config: dict):
        self.datasets = config["datasets"]
        self.grid_mappings = config["grid_mappings"]
        self.data_stores = config["data_stores"]

    @classmethod
    def get_schema(cls) -> JsonObjectSchema:
        return CONFIG_SCHEMA

    @classmethod
    def from_file(cls, config_path: str):
        with open(config_path) as fp:
            config = yaml.safe_load(fp)
        schema = cls.get_schema()
        schema.validate_instance(config)
        return MultiSourceConfig(config)
