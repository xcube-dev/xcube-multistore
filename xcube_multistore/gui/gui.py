import panel as pn
import yaml
from xcube.util.jsonschema import JsonComplexSchema, JsonStringSchema
from xcube.util.undefined import UNDEFINED

from xcube_multistore.multistore import MultiSourceDataStore

pn.extension('modal', 'codeeditor')

# -------------------------------------------------------------------
# Internal state
# -------------------------------------------------------------------

store_entries = []  # appended to YAML
grid_mappings = []
dataset_entries = []

wanted_data_stores = ["stac", "zenodo", "clms", "file"]

# -------------------------------------------------------------------
# YAML Preview
# -------------------------------------------------------------------

preview = pn.widgets.CodeEditor(name="YAML Preview", language="yaml", height=500)

def update_preview():
    cfg = {
        "data_stores": store_entries,
        "grid_mappings": grid_mappings,
        "datasets": dataset_entries,
    }
    preview.value = yaml.dump(cfg, sort_keys=False)

update_preview()

# -------------------------------------------------------------------
# Build dynamic widgets from JSON schema
# -------------------------------------------------------------------

def create_param_widget(name, spec):
    t = spec.get("type", "string")
    default = spec.get("default")

    if t == "string":
        return pn.widgets.TextInput(name=name, value=default or "")
    if t == "number":
        return pn.widgets.FloatInput(name=name, value=default or 0.0)
    if t == "integer":
        return pn.widgets.IntInput(name=name, value=default or 0)
    if t == "boolean":
        return pn.widgets.Checkbox(name=name, value=bool(default))
    if "enum" in spec:
        return pn.widgets.Select(name=name, options=spec["enum"], value=default)

    return pn.widgets.TextInput(name=name, value=str(default))

# -------------------------------------------------------------------
# Modal Widgets
# -------------------------------------------------------------------
all_data_store_ids = MultiSourceDataStore.list_data_store_ids()
filtered_data_store_ids = [data_id for data_id in all_data_store_ids if
                          data_id in wanted_data_stores]

store_id_select = pn.widgets.Select(
    name="Store ID",
    options=filtered_data_store_ids,
    value="file"
)

identifier_input = pn.widgets.TextInput(name="Identifier", value="file")

store_params = pn.Column()  # dynamically rebuilt

def render_schema(name, schema, prefix=""):
    """
    Recursively convert schema → (Panel widget tree, widget_dict)

    widget_dict maps "storage_options.credentials.client_id" → widget
    """

    full_prefix = f"{prefix}.{name}" if prefix else name
    widgets = {}

    # ---------- SIMPLE TYPES ----------
    if schema.type == "string":
        w = pn.widgets.TextInput(name=name, value=schema.default if schema.default != UNDEFINED  else "")
        widgets[full_prefix] = w
        return w, widgets

    if schema.type == "integer":
        w = pn.widgets.IntInput(
            name=name,
            value=schema.default if schema.default != UNDEFINED  else 0,
            start=getattr(schema, "minimum", None),
            end=getattr(schema, "maximum", None)
        )
        widgets[full_prefix] = w
        return w, widgets

    if schema.type == "number":
        w = pn.widgets.FloatInput(
            name=name,
            value=schema.default if schema.default != UNDEFINED  else 0.0,
        )
        widgets[full_prefix] = w
        return w, widgets

    if schema.type == "boolean":
        w = pn.widgets.Checkbox(name=name,
                                value=schema.default if schema.default !=
                                                        UNDEFINED else False)
        widgets[full_prefix] = w
        return w, widgets

    # ---------- ENUM ----------
    if getattr(schema, "enum", None):
        w = pn.widgets.Select(
            name=name,
            options=schema.enum,
            value=schema.default or schema.enum[0]
        )
        widgets[full_prefix] = w
        return w, widgets

    # ---------- ARRAY ----------
    if schema.type == "array":
        # Recursive arrays of objects OR simple arrays
        if getattr(schema, "items", None) and schema.items.type == "object":
            # Array of objects → JSON editor for now
            w = pn.widgets.JSONEditor(name=name, value=schema.default or [], mode="tree")
            widgets[full_prefix] = w
            return w, widgets

        # fallback simple array
        w = pn.widgets.JSONEditor(name=name, value=schema.default or [], mode="tree")
        widgets[full_prefix] = w
        return w, widgets

    # ---------- OBJECT ----------
    if schema.type == "object":
        section = pn.Column(
            pn.pane.Markdown(f"### {name}"),  # section header
            margin=(0, 0, 10, 0)
        )

        for prop_name, prop_schema in schema.properties.items():
            widget_or_container, inner_widgets = render_schema(
                prop_name,
                prop_schema,
                prefix=full_prefix
            )

            section.append(widget_or_container)
            widgets.update(inner_widgets)

        return section, widgets

    # ---------- FALLBACK ----------
    w = pn.widgets.TextAreaInput(name=name, value=str(schema.default or ""))
    widgets[full_prefix] = w
    return w, widgets

def build_store_params(store_id):
    schema = MultiSourceDataStore.get_data_store_params_schema()

    schema.properties = {
        key: schema.properties[key] for key in wanted_data_stores if key in schema.properties
    }
    print("schema", schema)
    # print(schema.properties["file"].properties)
    # # print(schema.properties["file"].properties['storage_options'].type)
    # s: JsonStringSchema = schema.properties["file"].properties['max_depth']
    # for item in s.to_dict().items():
    #     print(item)
    # print(s.default)
    # print(s.properties['listings_expiry_time'].type)
    # properties = schema.get("properties", {})

    # widgets = {}
    # store_params_container.objects = []  # clear
    #
    # for key, spec in properties.items():
    #     w = create_param_widget(key, spec)
    #     widgets[key] = w
    #     store_params_container.append(w)

    # return widgets
    store_params.objects = []

    # schema = MultiSourceDataStore.get_data_store_params_schema()
    store_schema = schema.properties[store_id].properties

    store_param_widgets = {}  # clear

    for key, prop_schema in store_schema.items():
        ui_block, widgets_dict = render_schema(key, prop_schema)
        store_params.append(ui_block)
        store_param_widgets.update(widgets_dict)

    def nested_dict_from_widgets(widget_map):
        root = {}

        for key, widget in widget_map.items():
            parts = key.split(".")
            d = root

            for p in parts[:-1]:
                d = d.setdefault(p, {})

            d[parts[-1]] = widget.value

        return root

    params = nested_dict_from_widgets(store_param_widgets)
    print("store_param_widgets", store_param_widgets)
    print("params", params)
    return params

# Keep reference to current params
current_params_widgets = build_store_params(store_id_select.value)

def on_store_id_change(event):
    global current_params_widgets
    # Update identifier default
    identifier_input.value = event.new

    # Rebuild parameters
    current_params_widgets = build_store_params(event.new)

store_id_select.param.watch(on_store_id_change, "value")

# -------------------------------------------------------------------
# Modal Setup
# -------------------------------------------------------------------

add_button = pn.widgets.Button(name="Add Store", button_type="primary")
cancel_button = pn.widgets.Button(name="Cancel", button_type="warning")

# The following messes up with my UI, and the data store modal behaves
# wieerdly a d store_params_conatiner does not update, but when i clikc save,
# i can see the updated params in the preview, also no scrolling acheived,
# store_params = pn.Column(
#     store_params_container,
#     max_height=350,
#     scroll=True
# )

modal = pn.Modal(
pn.Column(
    pn.pane.Markdown("### Add a Data Store"),
    identifier_input,
    store_id_select,
    pn.pane.Markdown("#### store_params"),
    store_params,
    pn.Row(add_button, cancel_button),
),
    name="Add Store Modal",
    width=400,
)

toggle_button = modal.create_button("show", name="Add a Data Store")

gm_identifier = pn.widgets.TextInput(name="Identifier", value="gm1")
gm_bbox = pn.widgets.TextInput(name="BBox (xmin,ymin,xmax,ymax)", value="-180,-90,180,90")
gm_spatial_res = pn.widgets.FloatInput(name="Spatial Resolution", value=0.1)
gm_crs = pn.widgets.TextInput(name="CRS", value="EPSG:4326")

add_gm_btn = pn.widgets.Button(name="Add GridMapping", button_type="primary")
cancel_gm_btn = pn.widgets.Button(name="Cancel", button_type="danger")

gm_modal = pn.Modal(
    pn.Column(
        gm_identifier,
        gm_bbox,
        gm_spatial_res,
        gm_crs,
        pn.Row(add_gm_btn, cancel_gm_btn),
        # scroll=True,
    ),
    open=False,
    name="Add Grid Mapping"
)

open_gm_modal_btn = gm_modal.create_button("show", name="Add Grid Mapping")

# -------------------------------------------------------------------
# Button Callbacks
# -------------------------------------------------------------------

def on_add_store(event):
    entry = {
        "identifier": identifier_input.value,
        "store_id": store_id_select.value,
        "store_params": {k: w for k, w in current_params_widgets.items()}
    }

    store_entries.append(entry)
    ds_store.options = [s["identifier"] for s in store_entries]
    var_store.options = [s["identifier"] for s in store_entries]
    update_preview()
    modal.hide()

def on_cancel(event):
    modal.hide()

def on_add_gm(event):
    # parse bbox
    try:
        bbox_vals = [float(x) for x in gm_bbox.value.split(",")]
        if len(bbox_vals) != 4:
            raise ValueError()
    except Exception:
        pn.state.notifications.error("BBox must be 4 comma-separated floats")
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
    md_grid_mapping.options = [g["identifier"] for g in grid_mappings]
    update_preview()
    gm_modal.hide()

def on_cancel_gm(event):
    gm_modal.hide()

add_gm_btn.on_click(on_add_gm)
cancel_gm_btn.on_click(on_cancel_gm)

add_button.on_click(on_add_store)
cancel_button.on_click(on_cancel)


###############################################################################
# VARIABLE SUB-MODAL (for multi dataset)
###############################################################################

var_identifier = pn.widgets.TextInput(name="Variable Identifier", value="")
var_store = pn.widgets.Select(name="Store", options=[])
var_data_id = pn.widgets.TextInput(name="Data ID", value="")

var_open_params = pn.widgets.TextAreaInput(name="open_params (YAML)", value="")
var_custom_proc = pn.widgets.TextAreaInput(name="custom_processing (YAML)", value="")
var_spatial = pn.widgets.TextAreaInput(name="spatial_resample_params (YAML)", value="")
var_temporal = pn.widgets.TextAreaInput(name="temporal_resample_params (YAML)", value="")

add_var_btn = pn.widgets.Button(name="Add Variable", button_type="primary")
cancel_var_btn = pn.widgets.Button(name="Cancel", button_type="danger")

variable_modal = pn.Modal(
    pn.Column(
        pn.pane.Markdown("### Add Variable Object"),
        var_identifier,
        var_store,
        var_data_id,
        var_open_params,
        var_custom_proc,
        var_spatial,
        var_temporal,
        pn.Row(add_var_btn, cancel_var_btn),
    ),
    open=False,
    name="Add Variable"
)

open_variable_modal_btn = variable_modal.create_button("show", name="➕ Add Variable")

# The multi-dataset holds variables temporarily until the main dataset is added
multi_variable_list = []


def on_add_variable(event):
    entry = {
        "identifier": var_identifier.value,
        "store": var_store.value,
        "data_id": var_data_id.value,
    }

    # optional YAML blocks
    for key, widget in [
        ("open_params", var_open_params),
        ("custom_processing", var_custom_proc),
        ("spatial_resample_params", var_spatial),
        ("temporal_resample_params", var_temporal),
    ]:
        val = widget.value
        if val is not None:
            entry[key] = val

    multi_variable_list.append(entry)
    variable_modal.hide()

cancel_var_btn.on_click(lambda e: variable_modal.hide())
add_var_btn.on_click(on_add_variable)


###############################################################################
# DATASET MAIN MODAL
###############################################################################

dataset_type = pn.widgets.RadioBoxGroup(
    name="Dataset Type",
    options=["single", "multi"],
    value="single",
    inline=True,
)

# SINGLE DATASET FIELDS
ds_identifier = pn.widgets.TextInput(name="Identifier", value="")
ds_store = pn.widgets.Select(name="Store", options=[])
ds_grid_mapping = pn.widgets.Select(name="Grid Mapping", options=[])
ds_data_id = pn.widgets.TextInput(name="Data ID", value="")
ds_format_id = pn.widgets.TextInput(name="Format ID (optional)", value="")

ds_open_params = pn.widgets.TextAreaInput(name="open_params (YAML)", value="{}")
ds_custom_proc = pn.widgets.TextAreaInput(name="custom_processing (YAML)", value="")
ds_spatial = pn.widgets.TextAreaInput(name="spatial_resample_params (YAML)", value="")
ds_temporal = pn.widgets.TextAreaInput(name="temporal_resample_params (YAML)", value="")

# MULTI DATASET FIELDS
md_identifier = pn.widgets.TextInput(name="Identifier", value="")
md_grid_mapping = pn.widgets.Select(name="Grid Mapping", options=[])
md_format_id = pn.widgets.TextInput(name="Format ID (optional)", value="")
md_xr_merge_params = pn.widgets.TextAreaInput(name="xr_merge_params (YAML)", value="{}")

# Just a display of variables added
md_var_display = pn.pane.Markdown("No variables added yet.")


def refresh_md_variable_display():
    if not multi_variable_list:
        md_var_display.object = "No variables added yet."
        return
    text = "### Variables Added\n"
    for i, v in enumerate(multi_variable_list, 1):
        text += f"- **{v['identifier']}** (store={v['store']}, data_id={v['data_id']})\n"
    md_var_display.object = text


###############################################################################
# RENDERING CONDITIONS
###############################################################################

def dataset_type_changed(event):
    if event.new == "single":
        multi_section.visible = False
        single_section.visible = True
    else:
        single_section.visible = False
        multi_section.visible = True

dataset_type.param.watch(dataset_type_changed, "value")


###############################################################################
# DATASET MODAL BODY
###############################################################################

add_ds_btn = pn.widgets.Button(name="Add Dataset", button_type="primary")
cancel_ds_btn = pn.widgets.Button(name="Cancel", button_type="danger")

single_section = pn.Column(
    pn.pane.Markdown("### Single Dataset"),
    ds_identifier,
    ds_store,
    ds_grid_mapping,
    ds_data_id,
    ds_format_id,
    pn.pane.Markdown("#### Optional parameter blocks"),
    ds_open_params,
    ds_custom_proc,
    ds_spatial,
    ds_temporal,
)

multi_section = pn.Column(
    pn.pane.Markdown("### Multi Dataset"),
    md_identifier,
    md_grid_mapping,
    md_format_id,
    md_xr_merge_params,
    pn.pane.Markdown("### Variables"),
    md_var_display,
    open_variable_modal_btn,
    visible=False,
)

dataset_modal = pn.Modal(
    pn.Column(
        pn.pane.Markdown("## Add Dataset"),
        dataset_type,
        single_section,
        multi_section,
        pn.Row(add_ds_btn, cancel_ds_btn),
    ),
    open=False,
    name="Add Dataset"
)

open_dataset_modal_btn = dataset_modal.create_button("show", name="➕ Add Dataset")

###############################################################################
# BUTTON CALLBACKS — MAIN DATASET MODAL
###############################################################################

def on_add_dataset(event):
    if dataset_type.value == "single":
        entry = {
            "type": "single",
            "identifier": ds_identifier.value,
            "store": ds_store.value,
            "grid_mapping": ds_grid_mapping.value,
            "data_id": ds_data_id.value,
        }
        if ds_format_id.value:
            entry["format_id"] = ds_format_id.value

        for key, widget in [
            ("open_params", ds_open_params),
            ("custom_processing", ds_custom_proc),
            ("spatial_resample_params", ds_spatial),
            ("temporal_resample_params", ds_temporal),
        ]:
            val = widget.value
            if val is not None:
                entry[key] = val

        dataset_entries.append(entry)

    else:  # MULTI DATASET
        entry = {
            "type": "multi",
            "identifier": md_identifier.value,
            "grid_mapping": md_grid_mapping.value,
        }
        if md_format_id.value:
            entry["format_id"] = md_format_id.value

        xr_val = md_xr_merge_params.value
        if xr_val is not None:
            entry["xr_merge_params"] = xr_val

        entry["variables"] = list(multi_variable_list)

        dataset_entries.append(entry)

        # reset variable list for next time
        multi_variable_list.clear()
        refresh_md_variable_display()

    update_preview()
    dataset_modal.hide()


def on_cancel_ds(event):
    dataset_modal.hide()

add_ds_btn.on_click(on_add_dataset)
cancel_ds_btn.on_click(on_cancel_ds)

# -------------------------------------------------------------------
# Layout
# -------------------------------------------------------------------

layout = pn.Row(
    pn.Column(
        "## Data Stores",
        toggle_button,
        modal,
        "## Grid Mappings",
        open_gm_modal_btn,
        gm_modal,
        "## Datasets",
        open_dataset_modal_btn,
        dataset_modal,
        variable_modal,
        width=400
    ),
    preview
)

layout.servable()
