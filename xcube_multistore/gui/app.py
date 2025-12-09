## To run this, do: panel serve xcube_multistore/gui/app.py --dev from the
# root of this package

import panel as pn
import yaml
from xcube.util.undefined import UNDEFINED

from xcube_multistore.multistore import MultiSourceDataStore

pn.extension("modal", "codeeditor", "notifications")

# ----------------------------------------------------------------------------------------
# STORAGE
# ----------------------------------------------------------------------------------------

store_entries = []
grid_mappings = []
dataset_entries = []

SUPPORTED_DATA_STORES = ["stac", "zenodo", "clms", "file"]
DATA_ID_CACHE = {}

# ----------------------------------------------------------------------------------------
# YAML PREVIEW
# ----------------------------------------------------------------------------------------

preview = pn.widgets.CodeEditor(
    name="YAML Preview",
    language="yaml",
    height=1000,
    width=1000,
    theme="monokai",
)


def update_preview():
    cfg = {
        "data_stores": store_entries,
        "grid_mappings": grid_mappings,
        "datasets": dataset_entries,
    }
    preview.value = yaml.dump(cfg, sort_keys=False)


update_preview()

# ----------------------------------------------------------------------------------------
# HELPER
# ----------------------------------------------------------------------------------------


def render_schema(name, schema, prefix=""):
    """Convert xcube JsonObjectSchema recursively to Panel widgets."""
    full_prefix = f"{prefix}.{name}" if prefix else name
    widgets = {}

    if schema.type == "string":
        w = pn.widgets.TextInput(
            name=name, value=schema.default if schema.default != UNDEFINED else ""
        )
        widgets[full_prefix] = w
        return w, widgets

    if schema.type == "integer":
        w = pn.widgets.IntInput(
            name=name,
            value=schema.default if schema.default != UNDEFINED else 0,
            start=getattr(schema, "minimum", None),
            end=getattr(schema, "maximum", None),
        )
        widgets[full_prefix] = w
        return w, widgets

    if schema.type == "number":
        w = pn.widgets.FloatInput(
            name=name,
            value=schema.default if schema.default != UNDEFINED else 0.0,
        )
        widgets[full_prefix] = w
        return w, widgets

    if schema.type == "boolean":
        w = pn.widgets.Checkbox(
            name=name, value=schema.default if schema.default != UNDEFINED else False
        )
        widgets[full_prefix] = w
        return w, widgets

    if schema.type == "object":
        section = pn.Column(pn.pane.Markdown(f"### {name}"), margin=(0, 0, 10, 0))
        for prop_name, prop_schema in schema.properties.items():
            ui, inner_widgets = render_schema(
                prop_name, prop_schema, prefix=full_prefix
            )
            section.append(ui)
            widgets.update(inner_widgets)
        return section, widgets

    # fallback
    w = pn.widgets.TextAreaInput(
        name=name, value=schema.default if schema.default != UNDEFINED else ""
    )
    widgets[full_prefix] = w
    return w, widgets


# ----------------------------------------------------------------------------------------
# DATA STORE ADD MODAL
# ----------------------------------------------------------------------------------------

all_data_store_ids = MultiSourceDataStore.list_data_store_ids()
filtered_data_store_ids = [d for d in all_data_store_ids if d in SUPPORTED_DATA_STORES]

store_id_select = pn.widgets.Select(
    name="Store ID", options=filtered_data_store_ids, value="file"
)

identifier_input = pn.widgets.TextInput(name="Identifier", value="file")

store_params = pn.Column(sizing_mode="stretch_width")


def build_store_params(store_id):
    """Build the store_params UI dynamically."""
    schema = MultiSourceDataStore.get_data_store_params_schema()
    store_params.objects = []

    if store_id not in schema.properties:
        raise Exception(f"Store ID {store_id} not supported.")

    store_schema = schema.properties[store_id].properties
    widgets = {}

    for key, prop_schema in store_schema.items():
        ui, wdict = render_schema(key, prop_schema)
        store_params.append(ui)
        widgets.update(wdict)

    return widgets


current_params_widgets = build_store_params(store_id_select.value)


def on_store_id_change(event):
    global current_params_widgets
    identifier_input.value = event.new
    current_params_widgets = build_store_params(event.new)


store_id_select.param.watch(on_store_id_change, "value")

add_button = pn.widgets.Button(name="Add Store", button_type="primary")
cancel_button = pn.widgets.Button(name="Cancel", button_type="warning")


def make_store_modal_body():
    return pn.Column(
        pn.pane.Markdown("# Add a Data Store"),
        pn.layout.Divider(),
        identifier_input,
        store_id_select,
        pn.layout.Divider(),
        pn.pane.Markdown("#### Store Params"),
        store_params,
        pn.layout.Divider(),
        pn.Row(add_button, cancel_button),
        sizing_mode="stretch_width",
    )


modal = pn.layout.modal.Modal(
    make_store_modal_body(),
    name="Add Store Modal",
    width=520,
    height=620,
    show_close_button=True,
    background_close=False,
    sizing_mode="fixed",
)
toggle_button = modal.create_button(
    "show", name="Add a Data Store", button_type="primary"
)


def collect_nested(widget_map):
    result = {}
    for key, widget in widget_map.items():
        parts = key.split(".")
        d = result
        for p in parts[:-1]:
            d = d.setdefault(p, {})
        d[parts[-1]] = widget.value
    return result


def on_add_store(event):
    identifier = identifier_input.value.strip()

    if any(s["identifier"] == identifier for s in store_entries):
        pn.state.notifications.error(
            f"A store with identifier '{identifier}' already exists.", duration=10000
        )
        modal.hide()
        return

    entry = {
        "identifier": identifier_input.value,
        "store_id": store_id_select.value,
        "store_params": collect_nested(current_params_widgets),
    }

    store_entries.append(entry)
    ds_store.options = [s["identifier"] for s in store_entries]

    update_preview()
    modal.hide()


def on_cancel(event):
    modal.hide()


add_button.on_click(on_add_store)
cancel_button.on_click(on_cancel)

# ----------------------------------------------------------------------------------------
# GRID MAPPING
# ----------------------------------------------------------------------------------------

gm_identifier = pn.widgets.TextInput(name="Identifier", value="gm")
gm_bbox = pn.widgets.TextInput(name="BBox (west, south, east, north)", value="2,2,2,2")
gm_spatial_res = pn.widgets.FloatInput(name="Spatial Resolution", value=0.1)
gm_crs = pn.widgets.TextInput(name="CRS", value="EPSG:4326")
gm_tile_size = pn.widgets.TextInput(
    name="Tile Size",
    value="[1024, 1024]",
)

add_gm_btn = pn.widgets.Button(name="Add GridMapping", button_type="primary")
cancel_gm_btn = pn.widgets.Button(name="Cancel", button_type="danger")

gm_modal = pn.Modal(
    pn.Column(
        gm_identifier,
        gm_bbox,
        gm_spatial_res,
        gm_crs,
        gm_tile_size,
        pn.Row(add_gm_btn, cancel_gm_btn),
    ),
    name="Add Grid Mapping",
)
open_gm_modal_btn = gm_modal.create_button(
    "show", name="Add Grid Mapping", button_type="primary"
)


def on_add_gm(event):
    try:
        bbox_vals = [float(x) for x in gm_bbox.value.split(",")]
        if len(bbox_vals) != 4:
            raise ValueError()
    except Exception:
        pn.state.notifications.error(
            "BBox must be 4 comma-separated floats", duration=10000
        )
        return

    gm = {
        "identifier": gm_identifier.value,
        "bbox": bbox_vals,
        "spatial_res": gm_spatial_res.value,
        "crs": gm_crs.value,
    }
    if gm not in grid_mappings:
        grid_mappings.append(gm)
    ds_grid_mapping.options = [g["identifier"] for g in grid_mappings]
    update_preview()
    gm_modal.hide()


def on_cancel_gm(event):
    gm_modal.hide()


add_gm_btn.on_click(on_add_gm)
cancel_gm_btn.on_click(on_cancel_gm)

# ----------------------------------------------------------------------------------------
# OPEN PARAMS LOGIC
# ----------------------------------------------------------------------------------------


def build_open_params_ui(container, store_id, data_id):
    """Build the open_params UI dynamically."""
    container.objects = []

    if not store_id or not data_id:
        return {}

    match = [s for s in store_entries if s["identifier"] == store_id]
    if not match:
        return {}

    store = match[0]

    try:
        schema = MultiSourceDataStore.get_open_data_params_schema(
            store["store_id"], store["store_params"], data_id
        )
    except Exception as e:
        pn.state.notifications.error(f"Open params error: {e}", duration=10000)
        return {}

    widgets = {}
    for key, prop_schema in schema.properties.items():
        ui, wdict = render_schema(key, prop_schema)
        container.append(ui)
        widgets.update(wdict)

    return widgets


# ----------------------------------------------------------------------------------------
# DATASET MODAL
# ----------------------------------------------------------------------------------------

dataset_type = pn.widgets.RadioBoxGroup(
    name="Dataset Type",
    options=["single", "multi"],
    value="single",
    inline=True,
)

ds_identifier = pn.widgets.TextInput(name="Identifier", value="")
ds_store = pn.widgets.Select(name="Store", options=[])
ds_data_id = pn.widgets.AutocompleteInput(
    name="Data ID",
    options=[],
    min_characters=0,
    case_sensitive=False,
    search_strategy="includes",
    placeholder="Write something here",
)
ds_grid_mapping = pn.widgets.Select(name="Grid Mapping", options=[])
ds_format_id = pn.widgets.TextInput(name="Format ID (optional)", value="")

ds_open_params_container = pn.Column(sizing_mode="stretch_width")
ds_open_params_widgets = {}


def update_ds_store_options():
    ds_store.options = [s["identifier"] for s in store_entries]


update_ds_store_options()


def update_grid_mapping_options():
    gm_ids = [g["identifier"] for g in grid_mappings]
    ds_grid_mapping.options = gm_ids
    md_grid_mapping.options = gm_ids


md_grid_mapping = pn.widgets.Select(name="Grid Mapping", options=[])

update_grid_mapping_options()


def on_ds_store_change(event):
    store_id = event.new
    if not store_id:
        return

    if store_id not in DATA_ID_CACHE:
        match = [s for s in store_entries if s["identifier"] == store_id]
        if not match:
            return
        store = match[0]

        try:
            modal.loading = True
            modal.show_close_button = False
            add_button.disabled = True
            cancel_button.disabled = True
            raw = MultiSourceDataStore.list_data_ids(
                {store["store_id"]: store["store_params"]}
            )
            options = []
            for k in getattr(raw, "properties", {}):
                enum = getattr(raw.properties[k], "enum", None)
                print(enum)
                if enum:
                    options = list(enum)
                    break
            if not options:
                options = list(raw)
        except Exception as e:
            pn.state.notifications.error(
                f"Error fetching data IDs: {e}", duration=10000
            )
            return
        finally:
            modal.show_close_button = True
            modal.loading = False
            add_button.disabled = False
            cancel_button.disabled = False

        DATA_ID_CACHE[store_id] = options

    ds_data_id.options = DATA_ID_CACHE[store_id]


ds_store.param.watch(on_ds_store_change, "value")


def on_ds_data_id_change(event):
    global ds_open_params_widgets
    ds_open_params_widgets = build_open_params_ui(
        ds_open_params_container, ds_store.value, ds_data_id.value
    )


ds_data_id.param.watch(on_ds_data_id_change, "value")

single_section = pn.Column(
    pn.pane.Markdown("### Single Dataset"),
    ds_identifier,
    ds_store,
    ds_grid_mapping,
    ds_data_id,
    ds_format_id,
    pn.layout.Divider(),
    pn.pane.Markdown("#### Open Params"),
    ds_open_params_container,
    sizing_mode="stretch_width",
)

add_ds_btn = pn.widgets.Button(name="Add Dataset", button_type="primary")
cancel_ds_btn = pn.widgets.Button(name="Cancel", button_type="danger")


def on_add_dataset(event):
    entry = {
        "type": "single",
        "identifier": ds_identifier.value,
        "store": ds_store.value,
        "grid_mapping": ds_grid_mapping.value,
        "data_id": ds_data_id.value,
        "open_params": {
            k.split(".")[-1]: w.value for k, w in ds_open_params_widgets.items()
        },
    }
    if ds_format_id.value:
        entry["format_id"] = ds_format_id.value

    dataset_entries.append(entry)

    update_preview()
    dataset_modal.hide()


def on_cancel_ds(event):
    dataset_modal.hide()


add_ds_btn.on_click(on_add_dataset)
cancel_ds_btn.on_click(on_cancel_ds)

dataset_modal = pn.Modal(
    pn.Column(
        pn.pane.Markdown("## Add Dataset"),
        dataset_type,
        single_section,
        pn.Row(add_ds_btn, cancel_ds_btn),
    ),
    open=False,
    name="Add Dataset",
    width=520,
    height=620,
    show_close_button=True,
    background_close=True,
    sizing_mode="fixed",
)

open_dataset_modal_btn = dataset_modal.create_button(
    "show", name="Add Dataset", button_type="primary"
)

# ----------------------------------------------------------------------------------------
# DOWNLOAD BUTTON
# ----------------------------------------------------------------------------------------


def download_callback():
    data = preview.value.encode("utf-8")
    import io

    return io.BytesIO(data)


download = pn.widgets.FileDownload(
    filename="config.yaml",
    button_type="success",
    label="Download YAML",
    callback=download_callback,
)

# ----------------------------------------------------------------------------------------
# FINAL LAYOUT
# ----------------------------------------------------------------------------------------

layout = pn.Row(
    pn.Column(
        "# xcube Multistore Config Generator",
        pn.layout.Divider(),
        pn.Row(
            toggle_button,
            open_gm_modal_btn,
            open_dataset_modal_btn,
        ),
        pn.layout.Divider(),
        download,
        gm_modal,
        dataset_modal,
        modal,
        width=500,
        sizing_mode="fixed",
    ),
    preview,
    sizing_mode="stretch_width",
)

layout.servable()
