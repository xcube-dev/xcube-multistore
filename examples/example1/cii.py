from xcube.core.store import new_data_store
from xcube_resampling.gridmapping import GridMapping
from xcube_resampling import resample_in_space
import matplotlib.pyplot as plt

store = new_data_store("cciodp")

ds = store.open_data(
    "esacci.BIOMASS.yr.L4.AGB.multi-sensor.multi-platform.MERGED.5-0.100m",
    time_range=["2020-01-01", "2020-03-30"],
)

storage = new_data_store("file", root="data")
ds_ref = storage.open_data("AU-Dry_sen2.zarr")
target_gm = GridMapping.from_dataset(ds_ref)
ds_rs = resample_in_space(ds, target_gm=target_gm)
ds_rs.agb.isel(time=0).plot()
plt.show()
