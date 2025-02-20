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

import functools
import importlib

import pyproj
import xarray as xr
from xcube.core.geom import clip_dataset_by_geometry
from xcube.core.gridmapping import GridMapping
from xcube.core.resampling.spatial import resample_in_space
from xcube.util.jsonschema import JsonObjectSchema

from .config import MultiSourceConfig
from .constants import LOG, MAP_FORMAT_ID_FILE_EXT
from .grid_mappings import GridMappings
from .stores import DataStores
from .visualisation import GeneratorDisplay, GeneratorState, GeneratorStatus


class MultiSourceDataStore:

    def __init__(self, config: MultiSourceConfig, visualize: bool = True):
        self.config = config
        self.visualize = visualize
        self.stores = DataStores.setup_data_stores(config)
        if hasattr(config, "grid_mappings"):
            self.grid_mapping = GridMappings.setup_grid_mappings(config)
        else:
            self.grid_mapping = None
        self._states = {
            config_ds["identifier"]: GeneratorState(
                identifier=config_ds["identifier"], status=GeneratorStatus.waiting
            )
            for config_ds in config.datasets
        }
        if self.visualize:
            self._display = GeneratorDisplay.create(list(self._states.values()))
            self._display.show()
        self.generate_cubes()

    @classmethod
    def get_config_schema(cls) -> JsonObjectSchema:
        return MultiSourceConfig.get_schema()

    @classmethod
    def from_file(
        cls, config_path: str, visualize: bool = True
    ) -> "MultiSourceDataStore":
        config = MultiSourceConfig.from_file(config_path)
        return MultiSourceDataStore(config, visualize=visualize)

    def notify(self, event: GeneratorState):
        state = self._states[event.identifier]
        if (
            event.status is not None
            and event.status != state.status
            and state.status in (GeneratorStatus.stopped, GeneratorStatus.failed)
        ):
            # Status cannot be changed
            return
        state.update(event)
        if self.visualize:
            self._display.update()
        else:
            LOG.info(event.message)

    def generate_cubes(self):
        for config_ds in self.config.datasets:
            self.notify(
                GeneratorState(
                    config_ds["identifier"],
                    status=GeneratorStatus.started,
                    message=f"Open dataset {config_ds['identifier']}.",
                )
            )
            ds = open_dataset(self.stores, config_ds)
            if isinstance(ds, xr.Dataset):
                self.notify(
                    GeneratorState(
                        config_ds["identifier"],
                        message=f"Processing dataset {config_ds['identifier']}.",
                    )
                )
            else:
                self.notify(
                    GeneratorState(
                        config_ds["identifier"],
                        status=GeneratorStatus.failed,
                        exception=ds,
                    )
                )
            ds = process_dataset(ds, self.grid_mapping, config_ds)
            if isinstance(ds, xr.Dataset):
                self.notify(
                    GeneratorState(
                        config_ds["identifier"],
                        message=f"Write dataset {config_ds['identifier']}.",
                    )
                )
            else:
                self.notify(
                    GeneratorState(
                        config_ds["identifier"],
                        status=GeneratorStatus.failed,
                        exception=ds,
                    )
                )
            ds = write_dataset(ds, self.stores, config_ds)
            if isinstance(ds, xr.Dataset):
                self.notify(
                    GeneratorState(
                        config_ds["identifier"],
                        status=GeneratorStatus.stopped,
                        message=f"Finished dataset {config_ds['identifier']}.",
                    )
                )
            else:
                self.notify(
                    GeneratorState(
                        config_ds["identifier"],
                        status=GeneratorStatus.failed,
                        exception=ds,
                    )
                )


def safe_execute():
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                return e

        return wrapper

    return decorator


@safe_execute()
def open_dataset(stores: DataStores, config: dict) -> xr.Dataset | Exception:
    store = getattr(stores, config["store"])
    open_params = config.get("open_params", {})
    return store.open_data(config["data_id"], **open_params)


@safe_execute()
def process_dataset(
    ds: xr.Dataset, grid_mappings: GridMappings | None, config: dict
) -> xr.Dataset | Exception:
    # custom processing
    if "custom_processing" in config:
        module = importlib.import_module(config["custom_processing"]["module_path"])
        function = getattr(module, config["custom_processing"]["function_name"])
        ds = function(ds)

    # if grid mapping is given, resample the dataset
    if "grid_mapping" in config:
        target_gm = getattr(grid_mappings, config["grid_mapping"])
        source_gm = GridMapping.from_dataset(ds)
        transformer = pyproj.Transformer.from_crs(
            target_gm.crs, source_gm.crs, always_xy=True
        )
        bbox = transformer.transform_bounds(*target_gm.xy_bbox, densify_pts=21)
        ds = clip_dataset_by_geometry(ds, geometry=bbox)
        ds = resample_in_space(ds, target_gm=target_gm, encode_cf=True)
    return ds


@safe_execute()
def write_dataset(
    ds: xr.Dataset, stores: DataStores, config: dict
) -> xr.Dataset | Exception:
    store = getattr(stores, "storage")
    format_id = config.get("format_id", "zarr")
    if format_id == "netcdf":
        ds = prepare_dataset_for_netcdf(ds)
    data_id = f"{config['identifier']}.{MAP_FORMAT_ID_FILE_EXT[format_id]}"
    store.write_data(ds, data_id, replace=True)
    return ds


def prepare_dataset_for_netcdf(ds: xr.Dataset) -> xr.Dataset:
    attrs = ds.attrs
    for key in attrs:
        if (
            isinstance(attrs[key], list)
            or isinstance(attrs[key], tuple)
            or isinstance(attrs[key], dict)
        ):
            attrs[key] = str(attrs[key])
    ds = ds.assign_attrs(attrs)
    return ds
