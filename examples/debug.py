from xcube_multistore import MultiSourceDataStore

config = dict(datasets=[], data_stores=[])
config_ds = dict(
    identifier="era5land2",
    store="cds",
    data_id="reanalysis-era5-land",
    open_params=dict(
        variable_names=["2m_temperature", "total_precipitation"],
        time_range=["2020-01-01", "2020-03-30"],
        point=[4.7462, 50.5516],
        spatial_res=0.1,
    ),
)
config["datasets"].append(config_ds)

config_store = dict(
    identifier="storage",
    store_id="file",
    store_params=dict(root="data"),
)
config["data_stores"].append(config_store)
config_store = dict(
    identifier="cds",
    store_id="cds",
)
config["data_stores"].append(config_store)


msds = MultiSourceDataStore(config)
msds.generate()


ds = msds.stores.storage.open_data("era5land2.zarr")
print(ds)
