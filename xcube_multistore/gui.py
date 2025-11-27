import panel as pn
import yaml

pn.extension()

# --- Widgets for dataset configuration ---
dataset_widgets = []


def add_dataset(_=None):
    wid = {
        "identifier": pn.widgets.TextInput(name="Identifier", value=""),
        "store": pn.widgets.TextInput(name="Store", value=""),
        "data_id": pn.widgets.TextInput(name="Data ID", value=""),
        "grid_mapping": pn.widgets.TextInput(name="Grid Mapping", value="gm_id"),
    }
    dataset_widgets.append(wid)
    datasets_box.append(
        pn.Card(*wid.values(), title=f"Dataset #{len(dataset_widgets)}")
    )
    update_preview()


datasets_box = pn.Column()
add_dataset_btn = pn.widgets.Button(name="➕ Add Dataset", button_type="primary")
add_dataset_btn.on_click(add_dataset)


# --- STORE SECTION ---
store_widgets = [
    {
        "identifier": pn.widgets.TextInput(name="Store Identifier", value="storage"),
        "store_id": pn.widgets.TextInput(name="Store ID", value="file"),
        "root": pn.widgets.TextInput(name="Root", value="data"),
    }
]


def add_store(_=None):
    wid = {
        "identifier": pn.widgets.TextInput(name="Store Identifier", value=""),
        "store_id": pn.widgets.TextInput(name="Store ID", value=""),
        "root": pn.widgets.TextInput(name="Root", value=""),
    }
    store_widgets.append(wid)
    card = pn.Card(*wid.values(), title=f"Store #{len(store_widgets)}")
    stores_box.append(card)
    update_preview()


stores_box = pn.Column()
add_store_btn = pn.widgets.Button(name="➕ Add Store", button_type="success")
add_store_btn.on_click(add_store)

# --- GRIDMAPPING SECTION ---
gridmapping_widgets = []


def add_gridmapping(_=None):
    wid = {
        "identifier": pn.widgets.TextInput(name="GM Identifier", value="gm_id"),
        "bbox": pn.widgets.TextInput(name="BBox", value="-180,-90,180,90"),
        "spatial_res": pn.widgets.FloatInput(name="Resolution", value=0.1),
        "crs": pn.widgets.TextInput(name="CRS", value="EPSG:4326"),
    }
    gridmapping_widgets.append(wid)
    card = pn.Card(*wid.values(), title=f"Grid Mapping {wid["identifier"]}")
    gridmapping_box.append(card)
    update_preview()


gridmapping_box = pn.Column()
add_gridmapping_btn = pn.widgets.Button(
    name="➕ Add Grid Mapping", button_type="success"
)
add_gridmapping_btn.on_click(add_gridmapping)


# --- Live preview of YAML output ---
preview = pn.widgets.TextAreaInput(
    name="YAML Preview", min_height=600, sizing_mode="stretch_both"
)


def update_preview(_=None):
    cfg = {
        "datasets": [
            {
                "identifier": wid["identifier"].value,
                "store": wid["store"].value,
                "data_id": wid["data_id"].value,
                "grid_mapping": wid["grid_mapping"].value,
            }
            for wid in dataset_widgets
        ],
        "data_stores": [
            {
                "identifier": w["identifier"].value,
                "store_id": w["store_id"].value,
                "store_params": {"root": w["root"].value},
            }
            for w in store_widgets
        ],
        "grid_mappings": [
            {
                "identifier": wid["identifier"].value,
                "bbox": [float(x) for x in wid["bbox"].value.split(",")],
                "spatial_res": wid["spatial_res"].value,
                "crs": wid["crs"].value,
            }
            for wid in gridmapping_widgets
        ],
    }
    preview.value = yaml.dump(cfg, sort_keys=False)


# --- DOWNLOAD BUTTON ---
def download_yaml():
    return yaml.dump(yaml.safe_load(preview.value))


download_btn = pn.widgets.FileDownload(
    label="💾 Download YAML Config",
    filename="xcube_config.yaml",
    callback=download_yaml,
)


# --- LAYOUT ---
layout = pn.Row(
    pn.Column(
        "## Datasets",
        add_dataset_btn,
        datasets_box,
        "## Stores",
        add_store_btn,
        stores_box,
        "## Grid Mapping",
        add_gridmapping_btn,
        gridmapping_box,
        download_btn,
        width=450,
        sizing_mode="stretch_height",
    ),
    preview,
)

update_preview()
layout.servable()
