# MIT License
#
# Copyright (c) 2025-2026 Brockmann Consult GmbH
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
import datetime
import importlib
import sys
from typing import Any, Mapping

import geopandas as gpd
import xarray as xr
from shapely.geometry import Point, box
from xcube.core.mldataset import MultiLevelDataset
from xcube.core.store import (
    DataDescriptor,
    DataStoreError,
    get_data_store_params_schema,
    list_data_store_ids,
    new_data_store,
)
from xcube.util.jsonschema import JsonArraySchema, JsonObjectSchema
from xcube_resampling import resample_in_space, resample_in_time
from xcube_resampling.gridmapping import GridMapping
from xcube_resampling.utils import resolution_meters_to_degrees

from .accessors import guess_accessor
from .config import MultiSourceConfig
from .constants import LOG, NAME_WRITE_STORE
from .gridmappings import GridMappings
from .stores import DataStores
from .utils import (
    _get_data_id,
    _safe_execute,
    clean_dataset,
    prepare_dataset_for_netcdf,
)
from .visualization import (
    ConfigDisplay,
    GeneratorDisplay,
    GeneratorState,
    GeneratorStatus,
)


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
        self.stores = DataStores.setup_data_stores(self.config)
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
        if self.config.general["visualize"]:
            self._display = GeneratorDisplay.create(list(self._states.values()))

    def generate(self):
        # preload data, which is not preloaded as default
        if self.config.preload_datasets is not None:
            self._preload_datasets()

        # generate data cubes
        if self.config.general["visualize"]:
            self._display.display_title("Cube Generation")
            self._display.show()
        self._generate_cubes()

        # clean up if needed
        self._close()

    @classmethod
    def get_config_schema(cls) -> JsonObjectSchema:
        """Retrieves the configuration schema for the multi-source data store.

        Returns:
            A schema object defining the expected structure of the configuration.
        """
        return MultiSourceConfig.get_schema()

    def display_config(self):
        display = ConfigDisplay.create(self.config)
        display.display_title("Configuration")
        display.show()

    def display_geolocations(self):
        records = []

        for config_ds in self.config.datasets.values():
            name = config_ds.get("identifier", "Unnamed")

            # Case 1: grid mapping
            if "grid_mapping" in config_ds:
                gm_name = config_ds["grid_mapping"]
                if self.config.grid_mappings and gm_name in self.config.grid_mappings:
                    gm = self.config.grid_mappings[gm_name]
                    geom = box(*gm["bbox"])
                    gdf_temp = gpd.GeoDataFrame(
                        geometry=[geom], crs=gm.get("crs", "EPSG:4326")
                    )
                    gdf_temp = gdf_temp.to_crs("EPSG:4326")
                    records.append(
                        {"name": name, "geometry": gdf_temp.geometry.iloc[0]}
                    )
                else:
                    LOG.debug(
                        f"Grid mapping of datacube {name!r} is the same as "
                        f"{gm_name!r}. No geometry assigned."
                    )

            # Case 2: dataset with variables
            elif "variables" in config_ds:
                gdf = self._get_gdf_from_dataset(config_ds["variables"][0])
                gdf["name"] = name
                gdf = gdf.to_crs("EPSG:4326")
                for _, row in gdf.iterrows():
                    records.append({"name": row["name"], "geometry": row["geometry"]})

            # Case 3: direct dataset
            else:
                gdf = self._get_gdf_from_dataset(config_ds)
                gdf["name"] = name
                gdf = gdf.to_crs("EPSG:4326")
                for _, row in gdf.iterrows():
                    records.append({"name": row["name"], "geometry": row["geometry"]})

        if not records:
            LOG.warning("No geometries found.")
            return None

        # Create GeoDataFrame directly in EPSG:4326
        gdf_all = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")

        # Display using interactive GeoPandas explorer
        m = gdf_all.explore(
            tooltip="name",
            color="blue",
            style_kwds=dict(weight=4, fillOpacity=0.1),
            tiles="OpenStreetMap",
        )
        return m

    def _get_gdf_from_dataset(self, config_ds: dict) -> gpd.GeoDataFrame:
        op = config_ds.get("open_params", {})

        # Case: bounding box
        if "bbox" in op:
            return gpd.GeoDataFrame(
                geometry=[box(*op["bbox"])],
                crs=op.get("crs", "EPSG:4326"),
            )

        # Case: point with radius
        elif "point" in op or "location" in op:
            point = Point(op.get("point", op.get("location")))

            # If radius is given, create a circle
            if "radius" in op:
                gdf_temp = gpd.GeoDataFrame(geometry=[point], crs="EPSG:4326")
                gdf_temp["geometry"] = gdf_temp.buffer(op["radius"])
                return gdf_temp

            # Case: bbox_width (xcube-stac)
            elif "bbox_width" in op:
                lon, lat = op["point"]
                lon_width, lat_width = resolution_meters_to_degrees(
                    op["bbox_width"], lat
                )
                bbox = [
                    lon - lon_width / 2,
                    lat - lat_width / 2,
                    lon + lon_width / 2,
                    lat + lat_width / 2,
                ]
                return gpd.GeoDataFrame(geometry=[box(*bbox)], crs="EPSG:4326")

            else:
                return gpd.GeoDataFrame(geometry=[point], crs="EPSG:4326")

        # Fallback: use GridMapping
        else:
            ds = self._open_dataset(config_ds)
            gm = GridMapping.from_dataset(ds)
            return gpd.GeoDataFrame(geometry=[box(*gm.xy_bbox)], crs=gm.crs)

    @classmethod
    def list_data_store_ids(cls) -> list[str]:
        """
        List the identifiers of all available data stores.

        Returns:
            A list of data store identifiers.

        Note:
            If a data store identifier is missing, ensure that the respective
            xcube plugin is installed in the environment.
        """
        return list_data_store_ids()

    @classmethod
    def get_data_store_params_schema(
        cls,
        data_store_ids: str | list[str] | None = None,
    ) -> JsonObjectSchema:
        """
        Get the parameter schema for one or more data stores.

        Args:
            data_store_ids: A single data store identifier, a list of identifiers,
                or None. If None, all available data store identifiers are used.

        Returns:
            A JSON object schema containing the parameter schemas for the
            requested data stores. Each key corresponds to a data store ID,
            and each value is its parameter schema.
        """
        if data_store_ids is None:
            data_store_ids = list_data_store_ids()
        if isinstance(data_store_ids, str):
            data_store_ids = [data_store_ids]
        return JsonObjectSchema(
            title="Data store parameters",
            properties={
                data_store_id: get_data_store_params_schema(data_store_id)
                for data_store_id in data_store_ids
            },
        )

    @classmethod
    def list_data_ids(
        cls, data_store_ids_params: Mapping[str, dict]
    ) -> JsonObjectSchema:
        """
        List available data IDs for one or more data stores.

        Args:
            data_store_ids_params: A mapping of data store identifiers to their
                data store parameters for initialization.

        Returns:
            A JSON object schema containing available data IDs for each data store.

        Logs:
            A warning is logged if data IDs cannot be listed for a given store.
        """
        properties = {}
        for data_store_id, data_store_params in data_store_ids_params.items():
            store = new_data_store(data_store_id, **data_store_params)
            try:
                properties[data_store_id] = JsonArraySchema(
                    title=f"Data IDs in the {data_store_id!r} data store",
                    enum=store.list_data_ids(),
                )
            except DataStoreError as err:
                LOG.warning(
                    "Could not list data IDs for store %r: %s", data_store_id, err
                )

        return JsonObjectSchema(
            title="Available data IDs for each data store",
            properties=properties,
        )

    @classmethod
    def get_open_data_params_schema(
        cls, data_store_id: str, data_store_params: dict, data_id: str
    ) -> JsonObjectSchema:
        """
        Get the parameter schema to open a specific dataset.

        Args:
            data_store_id: The identifier of the data store.
            data_store_params: Parameters used to initialize the data store.
            data_id: The identifier of the dataset within the data store.

        Returns:
            A JSON object schema describing the parameters available for opening
            the specified dataset.
        """
        store = new_data_store(data_store_id, **data_store_params)
        return store.get_open_data_params_schema(data_id=data_id)

    @classmethod
    def search_data_ids(
        cls,
        data_store_ids_params: Mapping[
            str, tuple[Mapping[str, Any], Mapping[str, Any]]
        ],
    ) -> JsonObjectSchema:
        """
        Search for available data IDs across multiple data stores.

        Args:
            data_store_ids_params: A mapping from data store identifiers (`str`) to a tuple:
                - `data_store_params` (Mapping[str, Any]): Parameters to initialize the data store.
                - `search_params` (Mapping[str, Any]): Parameters to use for searching data within the store.

        Returns:
            JsonObjectSchema: Contains one property per data store, each holding a
            JsonArraySchema of found data IDs.

        Logs:
            Warnings are logged for any data store where the search fails. This can
            happen if the store does not support searching or if there is an error
            retrieving the data IDs.

        Note:
            Ensure that the search parameters match the expected format for each store.
            To see the expected search parameters, use `get_search_params_schema`.
        """
        properties = {}
        for data_store_id, (
            data_store_params,
            search_params,
        ) in data_store_ids_params.items():
            store = new_data_store(data_store_id, **data_store_params)
            try:
                descriptors = store.search_data(**search_params)
                data_ids = [descriptor.data_id for descriptor in descriptors]

                properties[data_store_id] = JsonArraySchema(
                    title=(
                        f"Data IDs found in the {data_store_id!r} data store for "
                        f"search params {search_params!r}"
                    ),
                    enum=data_ids,
                )
            except Exception as err:
                LOG.warning(
                    "Could not search for data in store %r: %s",
                    data_store_id,
                    err,
                )

        return JsonObjectSchema(
            title="Found data IDs for each data store for given search params.",
            properties=properties,
        )

    @classmethod
    def get_search_params_schema(
        cls, data_store_ids_params: Mapping[str, dict]
    ) -> JsonObjectSchema:
        """
        Retrieve the search parameter schemas for one or more data stores.

        Args:
            data_store_ids_params: A mapping of data store identifiers to their
                initialization parameters.

        Returns:
            A JSON object schema containing the search parameter
            schemas for each data store. Each key is a data store identifier,
            and its value is the corresponding search parameter schema.

        Logs:
            A warning is logged if the search parameter schema cannot be retrieved
            for a given store.
        """
        properties = {}
        for data_store_id, data_store_params in data_store_ids_params.items():
            store = new_data_store(data_store_id, **data_store_params)
            try:
                properties[data_store_id] = store.get_search_params_schema()
            except DataStoreError as err:
                LOG.warning(
                    "Could not get search parameters for store %r: %s",
                    data_store_id,
                    err,
                )

        return JsonObjectSchema(
            title="Search parameters for each data store",
            properties=properties,
        )

    @classmethod
    def describe_data(
        cls, data_store_id: str, data_store_params: dict, data_id: str
    ) -> DataDescriptor:
        """
        Describe a dataset from a data store.

        Args:
            data_store_id: The identifier of the data store.
            data_store_params: Parameters used to initialize the data store.
            data_id: The identifier of the dataset within the data store.

        Returns:
            An object describing the dataset, including
            metadata such as its spatial, temporal, and variable information.
        """
        store = new_data_store(data_store_id, **data_store_params)
        return store.describe_data(data_id)

    def _notify(self, event: GeneratorState):
        state = self._states[event.identifier]
        state.update(event)
        if self.config.general["visualize"]:
            self._display.update()
        else:
            if event.status == GeneratorStatus.failed:
                LOG.error(
                    "Data ID: %s; Status: %s; Exception: %s",
                    event.identifier,
                    event.status,
                    event.exception,
                )
            else:
                LOG.info(
                    "Data ID: %s; Status: %s; Message: %s",
                    event.identifier,
                    event.status,
                    event.message,
                )

    def _notify_error(self, identifier: str, exception: Any):
        self._notify(
            GeneratorState(
                identifier,
                status=GeneratorStatus.failed,
                exception=exception,
            )
        )
        if "Keyboard Interrupt caught! Exiting gracefully." == exception:
            sys.exit(1)

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
                            status=GeneratorStatus.completed,
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
                    preload_params["silent"] = not self.config.general["visualize"]
                _ = store.preload_data(*data_ids, **preload_params)

    def _generate_cubes(self):
        for identifier, config_ds in self.config.datasets.items():
            start = datetime.datetime.now()
            data_id = _get_data_id(config_ds)
            if getattr(self.stores, NAME_WRITE_STORE).has_data(data_id):
                self._notify(
                    GeneratorState(
                        identifier,
                        status=GeneratorStatus.completed,
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
            if isinstance(ds, xr.Dataset) or (
                isinstance(ds, list) and isinstance(ds[0], xr.Dataset)
            ):
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
                delta = datetime.datetime.now() - start
                time_delta = str(datetime.timedelta(seconds=int(delta.total_seconds())))
                self._notify(
                    GeneratorState(
                        identifier,
                        status=GeneratorStatus.completed,
                        message=f"Dataset {identifier!r} finished: {time_delta}",
                    )
                )
            else:
                store = getattr(self.stores, NAME_WRITE_STORE)
                data_id = _get_data_id(config_ds)
                store.has_data(data_id) and store.delete_data(data_id)
                self._notify_error(identifier, ds)

    @_safe_execute()
    def _open_dataset(self, config: dict) -> xr.Dataset | list[xr.Dataset] | Exception:
        if "data_id" in config:
            return self._open_single_dataset(config)
        else:
            dss = []
            for config_var in config["variables"]:
                config_var["identifier_var"] = config_var["identifier"]
                config_var["identifier"] = config["identifier"]
                ds = self._open_single_dataset(config_var)
                if len(ds.data_vars) > 1:
                    name_dict = {
                        var: f"{config_var['identifier_var']}_{var}"
                        for var in ds.data_vars.keys()
                    }
                else:
                    name_dict = {
                        var: f"{config_var['identifier_var']}"
                        for var in ds.data_vars.keys()
                    }
                dss.append(ds.rename_vars(name_dict=name_dict))
            return dss

    def _open_single_dataset(self, config: dict) -> xr.Dataset | Exception:
        store = getattr(self.stores, config["store"])
        store_id = getattr(self.stores, f"{config['store']}_store_id")
        open_params = copy.deepcopy(config.get("open_params", {}))
        storage_store = getattr(self.stores, NAME_WRITE_STORE)
        accessor = guess_accessor(
            store_id, store, storage_store, config["identifier"], self._notify
        )
        ds = accessor.open_data(config["data_id"], **open_params)
        if isinstance(ds, MultiLevelDataset):
            ds = ds.base_dataset

        # custom processing
        if "custom_processing" in config:
            module = importlib.import_module(config["custom_processing"]["module_path"])
            function = getattr(module, config["custom_processing"]["function_name"])
            ds = function(ds)

        return ds

    @_safe_execute()
    def _process_dataset(
        self, ds: xr.Dataset | list[xr.Dataset], config: dict
    ) -> xr.Dataset | Exception:
        gm_name = config.get("grid_mapping")
        if isinstance(ds, list):
            dss_processed = [
                self._process_single_dataset(
                    ds[0], config["variables"][0], gm_name=gm_name
                )
            ]
            for i in range(1, len(ds)):
                dss_processed.append(
                    self._process_single_dataset(
                        ds[i], config["variables"][i], gm_name=gm_name, ds_ref=ds[0]
                    )
                )
            merge_params = config.get("xr_merge_params", {})
            if "join" not in merge_params:
                merge_params["join"] = "exact"
            if "combine_attrs" not in merge_params:
                merge_params["combine_attrs"] = "drop_conflicts"
            if "compat" not in merge_params:
                merge_params["compat"] = "override"
            return xr.merge(dss_processed, **merge_params)
        else:
            return self._process_single_dataset(ds, config, gm_name=gm_name)

    @_safe_execute()
    def _process_single_dataset(
        self,
        ds: xr.Dataset,
        config: dict,
        gm_name: str | None = None,
        ds_ref: xr.Dataset = None,
    ) -> xr.Dataset | Exception:
        if "temporal_resample_params" in config:
            temp_params = config["temporal_resample_params"]
            frequency = temp_params.pop("frequency")
            ds = resample_in_time(ds, frequency, **temp_params)

        # if grid mapping is given, resample the dataset
        if gm_name:
            if hasattr(self._grid_mappings, gm_name):
                target_gm = getattr(self._grid_mappings, gm_name)
            else:
                config_ref = self.config.datasets[gm_name]
                data_id = _get_data_id(config_ref)
                ds_ref = getattr(self.stores, NAME_WRITE_STORE).open_data(data_id)
                target_gm = GridMapping.from_dataset(ds_ref)
            spatial_resample_params = config.get("spatial_resample_params", {})
            if "prevent_nan_propagations" not in spatial_resample_params:
                spatial_resample_params["prevent_nan_propagations"] = True
            ds = resample_in_space(ds, target_gm=target_gm, **spatial_resample_params)
        elif ds_ref is not None:
            target_gm = GridMapping.from_dataset(ds_ref)
            spatial_resample_params = config.get("spatial_resample_params", {})
            if "prevent_nan_propagations" not in spatial_resample_params:
                spatial_resample_params["prevent_nan_propagations"] = True
            ds = resample_in_space(ds, target_gm=target_gm, **spatial_resample_params)

        return ds

    @_safe_execute()
    def _write_dataset(self, ds: xr.Dataset, config: dict) -> xr.Dataset | Exception:
        store = getattr(self.stores, NAME_WRITE_STORE)
        format_id = config.get("format_id", "zarr")
        if format_id == "netcdf":
            ds = prepare_dataset_for_netcdf(ds)
        ds = clean_dataset(ds)

        chunksize = config.get("chunksize")
        if not chunksize:
            if hasattr(ds, "chunks") and ds.chunks is not None:
                chunksize = {dim: chunks[0] for dim, chunks in ds.chunks.items()}
        if chunksize:
            ds = ds.chunk(chunks=chunksize)
        # Remove "chunks" from encoding to avoid serialization issues
        for var in ds.data_vars:
            ds[var].encoding.pop("chunks", None)
            ds[var].encoding.pop("chunksizes", None)

        data_id = _get_data_id(config)
        store.write_data(ds, data_id, replace=True)
        return ds

    def _close(self):
        store = getattr(self.stores, NAME_WRITE_STORE)

        # remove temp files
        data_ids = store.list_data_ids()
        data_ids = [data_id for data_id in data_ids if data_id.startswith("temp/")]
        for data_id in data_ids:
            store.delete_data(data_id)
