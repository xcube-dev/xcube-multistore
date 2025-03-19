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

import copy
import importlib
from typing import Any

import numpy as np
import pyproj
import xarray as xr
from xcube.core.chunk import chunk_dataset
from xcube.core.geom import clip_dataset_by_geometry
from xcube.core.gridmapping import GridMapping
from xcube.core.resampling.spatial import resample_in_space
from xcube.util.jsonschema import JsonObjectSchema

from .config import MultiSourceConfig
from .constants import LOG, MAP_FORMAT_ID_FILE_EXT, NAME_WRITE_STORE
from .gridmappings import GridMappings
from .stores import DataStores
from .utils import (
    _get_data_id,
    _safe_execute,
    clean_dataset,
    prepare_dataset_for_netcdf,
)
from .visualization import GeneratorDisplay, GeneratorState, GeneratorStatus


class MultiSourceDataStore:
    """Manages access to multiple data sources and their configurations for generating
    data cubes.

    This class utilizes xcube data store plugins for data access, supports data
    harmonization, and enables visualization of data cube generation.

    Args:
        config: Configuration settings, provided as a dictionary or a string
            reference to a YAML configuration file.

    Notes:
        Detailed instructions on setting up the configuration can be found in the
        [Configuration Guide](https://xcube-dev.github.io/xcube-multistore/config/).
    """

    def __init__(self, config: str | dict[str, Any]):
        config = MultiSourceConfig(config)
        self.config = config
        self.stores = DataStores.setup_data_stores(config)
        if config.grid_mappings:
            self._grid_mappings = GridMappings.setup_grid_mappings(config)
        else:
            self._grid_mappings = None
        self._states = {
            identifier: GeneratorState(
                identifier=identifier, status=GeneratorStatus.waiting
            )
            for identifier, config_ds in config.datasets.items()
        }

        # preload data, which is not preloaded as default
        if config.preload_datasets is not None:
            self._preload_datasets()

        # generate data cubes
        if self.config.general["visualize"]:
            self._display = GeneratorDisplay.create(list(self._states.values()))
            self._display.display_title("Cube Generation")
            self._display.show()
        self._generate_cubes()

    @classmethod
    def get_config_schema(cls) -> JsonObjectSchema:
        """Retrieves the configuration schema for the multi-source data store.

        Returns:
            A schema object defining the expected structure of the configuration.
        """
        return MultiSourceConfig.get_schema()

    def _notify(self, event: GeneratorState):
        state = self._states[event.identifier]
        state.update(event)
        if self.config.general["visualize"]:
            self._display.update()
        else:
            if event.status == GeneratorStatus.failed:
                LOG.error("An error occurred: %s", event.exception)
            else:
                LOG.info(event.message)

    def _notify_error(self, identifier: str, exception: Any):
        self._notify(
            GeneratorState(
                identifier,
                status=GeneratorStatus.failed,
                exception=exception,
            )
        )

    def _preload_datasets(self):
        for config_preload in self.config.preload_datasets:
            store = getattr(self.stores, config_preload["store"])

            if self.config.general["force_preload"]:
                # preload all datasets again
                data_ids_preloaded = []
                data_ids = config_preload["data_ids"]
            else:
                # filter preloaded data IDs
                data_ids = []
                data_ids_preloaded = []
                for data_id_preload in config_preload["data_ids"]:
                    if all(
                        store.cache_store.has_data(data_id)
                        for data_id in self.config.preload_map[data_id_preload]
                    ):
                        data_ids_preloaded.append(data_id_preload)
                    else:
                        data_ids.append(data_id_preload)

            # setup visualization
            if self.config.general["visualize"]:
                display_preloaded = GeneratorDisplay.create(
                    [
                        GeneratorState(
                            identifier=data_id,
                            status=GeneratorStatus.stopped,
                            message="Already preloaded.",
                        )
                        for data_id in data_ids_preloaded
                    ]
                )
                display_preloaded.display_title(
                    f"Preload Datasets from store {config_preload['store']!r}"
                )
                if data_ids_preloaded:
                    display_preloaded.show()
            else:
                LOG.info(f"Preload Datasets from store {config_preload['store']!r}")
                for data_id in data_ids_preloaded:
                    LOG.info(f"Data ID {data_id!r} already preloaded.")

            if data_ids:
                preload_params = config_preload.get("preload_params", {})
                if "silent" not in preload_params:
                    preload_params["silent"] = self.config.general["visualize"]
                _ = store.preload_data(*data_ids, **preload_params)

    def _generate_cubes(self):
        for identifier, config_ds in self.config.datasets.items():
            data_id = _get_data_id(config_ds)
            if getattr(self.stores, "storage").has_data(data_id):
                self._notify(
                    GeneratorState(
                        identifier,
                        status=GeneratorStatus.stopped,
                        message=f"Dataset {identifier!r} already generated.",
                    )
                )
                continue
            self._notify(
                GeneratorState(
                    identifier,
                    status=GeneratorStatus.started,
                    message=f"Open dataset {identifier!r}.",
                )
            )
            ds = self._open_dataset(config_ds)
            if isinstance(ds, xr.Dataset):
                self._notify(
                    GeneratorState(
                        identifier,
                        message=f"Processing dataset {identifier!r}.",
                    )
                )
            else:
                self._notify_error(identifier, ds)
                continue
            ds = self._process_dataset(ds, config_ds)
            if isinstance(ds, xr.Dataset):
                self._notify(
                    GeneratorState(
                        identifier,
                        message=f"Write dataset {identifier!r}.",
                    )
                )
            else:
                self._notify_error(identifier, ds)
                continue
            ds = self._write_dataset(ds, config_ds)
            if isinstance(ds, xr.Dataset):
                self._notify(
                    GeneratorState(
                        identifier,
                        status=GeneratorStatus.stopped,
                        message=f"Dataset {identifier!r} finished.",
                    )
                )
            else:
                store = getattr(self.stores, NAME_WRITE_STORE)
                format_id = config_ds.get("format_id", "zarr")
                data_id = (
                    f"{config_ds['identifier']}.{MAP_FORMAT_ID_FILE_EXT[format_id]}"
                )
                store.has_data(data_id) and store.delete_data(data_id)
                self._notify_error(identifier, ds)

    @_safe_execute()
    def _open_dataset(self, config: dict) -> xr.Dataset | Exception:
        if "data_id" in config:
            return self._open_single_dataset(config)
        else:
            dss = []
            for config_var in config["variables"]:
                ds = self._open_single_dataset(config_var)
                if len(ds.data_vars) > 1:
                    name_dict = {
                        var: f"{config_var["identifier"]}_{var}"
                        for var in ds.data_vars.keys()
                    }
                else:
                    name_dict = {
                        var: f"{config_var["identifier"]}"
                        for var in ds.data_vars.keys()
                    }
                dss.append(ds.rename_vars(name_dict=name_dict))
            merge_params = config.get("xr_merge_params", {})
            if "join" not in merge_params:
                merge_params["join"] = "exact"
            if "combine_attrs" not in merge_params:
                merge_params["combine_attrs"] = "drop_conflicts"
            ds = xr.merge(dss, **merge_params)
        return clean_dataset(ds)

    def _open_single_dataset(self, config: dict) -> xr.Dataset | Exception:
        store = getattr(self.stores, config["store"])
        open_params = copy.deepcopy(config.get("open_params", {}))
        lat, lon = open_params.pop("point", [np.nan, np.nan])
        schema = store.get_open_data_params_schema(data_id=config["data_id"])
        if (
            ~np.isnan(lat)
            and ~np.isnan(lon)
            and "bbox" in schema.properties
            and ["spatial_res"] in open_params
            and "spatial_res" in schema.properties
        ):
            lat, lon = open_params.pop("point")
            open_params["bbox"] = [
                lon - 2 * open_params["spatial_res"],
                lat - 2 * open_params["spatial_res"],
                lon + 2 * open_params["spatial_res"],
                lat + 2 * open_params["spatial_res"],
            ]

        if hasattr(store, "cache_store"):
            try:
                ds = store.cache_store.open_data(config["data_id"], **open_params)
            except Exception:
                ds = store.open_data(config["data_id"], **open_params)
        else:
            ds = store.open_data(config["data_id"], **open_params)

        # custom processing
        if "custom_processing" in config:
            module = importlib.import_module(config["custom_processing"]["module_path"])
            function = getattr(module, config["custom_processing"]["function_name"])
            ds = function(ds)

        return clean_dataset(ds)

    @_safe_execute()
    def _process_dataset(self, ds: xr.Dataset, config: dict) -> xr.Dataset | Exception:
        # if grid mapping is given, resample the dataset
        if "grid_mapping" in config:
            if hasattr(self._grid_mappings, config["grid_mapping"]):
                target_gm = getattr(self._grid_mappings, config["grid_mapping"])
            else:
                config_ref = self.config.datasets[config["grid_mapping"]]
                data_id = _get_data_id(config_ref)
                ds_ref = getattr(self.stores, "storage").open_data(data_id)
                target_gm = GridMapping.from_dataset(ds_ref)
                for var_name, data_array in ds.items():
                    if np.issubdtype(data_array.dtype, np.number):
                        ds[var_name] = data_array.astype(target_gm.x_coords.dtype)
            source_gm = GridMapping.from_dataset(ds)
            transformer = pyproj.Transformer.from_crs(
                target_gm.crs, source_gm.crs, always_xy=True
            )
            bbox = transformer.transform_bounds(*target_gm.xy_bbox, densify_pts=21)
            bbox = [
                bbox[0] - 2 * source_gm.x_res,
                bbox[1] - 2 * source_gm.y_res,
                bbox[2] + 2 * source_gm.x_res,
                bbox[3] + 2 * source_gm.y_res,
            ]

            ds = clip_dataset_by_geometry(ds, geometry=bbox)
            ds = resample_in_space(ds, target_gm=target_gm, encode_cf=True)
            # this is needed since resample in space returns one chunk along the time
            # axis; this part can be removed once https://github.com/xcube-dev/xcube/issues/1124
            # is resolved.
            if "time" in ds.coords:
                ds = chunk_dataset(
                    ds, dict(time=1), format_name=config.get("format_id", "zarr")
                )

        # if "point" in open_params, timeseries is requested
        open_params = config.get("open_params", {})
        if "point" in open_params:
            ds = ds.interp(
                lat=open_params["point"][0],
                lon=open_params["point"][1],
                method="linear",
            )

        return ds

    @_safe_execute()
    def _write_dataset(self, ds: xr.Dataset, config: dict) -> xr.Dataset | Exception:
        store = getattr(self.stores, NAME_WRITE_STORE)
        format_id = config.get("format_id", "zarr")
        if format_id == "netcdf":
            ds = prepare_dataset_for_netcdf(ds)
        data_id = f"{config['identifier']}.{MAP_FORMAT_ID_FILE_EXT[format_id]}"
        ds = clean_dataset(ds)
        store.write_data(ds, data_id, replace=True)
        return ds
