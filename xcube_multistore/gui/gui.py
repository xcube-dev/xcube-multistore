import panel as pn
import yaml
from types import SimpleNamespace

from xcube.util.undefined import UNDEFINED

from xcube_multistore.multistore import MultiSourceDataStore

pn.extension('modal', 'codeeditor', 'notifications')

store_entries = []
grid_mappings = []
dataset_entries = []

SUPPORTED_DATA_STORES = ["stac", "zenodo", "clms", "file"]

preview = pn.widgets.CodeEditor(
    name="YAML Preview",
    language="yaml",
    height=1000,
    width=1000
)

def update_preview():
    cfg = {
        "data_stores": store_entries,
        "grid_mappings": grid_mappings,
        "datasets": dataset_entries,
    }
    preview.value = yaml.dump(cfg, sort_keys=False)

update_preview()

## Datastores

all_data_store_ids = MultiSourceDataStore.list_data_store_ids()
filtered_data_store_ids = [data_id for data_id in all_data_store_ids if
                           data_id in SUPPORTED_DATA_STORES]

store_id_select = pn.widgets.Select(
    name="Store ID",
    options=filtered_data_store_ids,
    value="file"
)

identifier_input = pn.widgets.TextInput(name="Identifier", value="file")

store_params = pn.Column(sizing_mode="stretch_width")  # dynamically rebuilt

def render_schema(name, schema, prefix=""):
    """Recursively convert schema to  widget dict"""

    full_prefix = f"{prefix}.{name}" if prefix else name
    widgets = {}

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

    if schema.type == "object":
        section = pn.Column(
            pn.pane.Markdown(f"### {name}"),
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

    w = pn.widgets.TextAreaInput(name=name, value=str(schema.default or ""))
    widgets[full_prefix] = w
    return w, widgets


def build_store_params(store_id):
    schema = MultiSourceDataStore.get_data_store_params_schema()

    store_params.objects = []

    if store_id in schema.properties.keys():
        store_schema = schema.properties[store_id].properties
    else:
        raise Exception(f"Store ID {store_id} not supported.")

    store_param_widgets = {}

    for key, prop_schema in store_schema.items():
        ui_block, widgets_dict = render_schema(key, prop_schema)
        store_params.append(ui_block)
        store_param_widgets.update(widgets_dict)

    return store_param_widgets

current_params_widgets = build_store_params(store_id_select.value)


def make_store_selector(label="Store", value=None):
    """
    Creates a store selector + dynamic store params block.
    Returns:
      selector       : a pn.Select of store identifiers
      params_ui      : a Column containing rendered params
      param_widgets  : a function that returns the current widgets dict
    """

    options = [s["identifier"] for s in store_entries]

    selector = pn.widgets.Select(
        name=label,
        options=options,
        value=value or (options[0] if options else None),
    )

    params_ui = pn.Column()
    current_param_widgets = {}

    def rebuild_params(event=None):
        nonlocal current_param_widgets
        params_ui.objects = []
        current_param_widgets = {}

        store_id = selector.value
        match = [s for s in store_entries if s["identifier"] == store_id]
        if not match:
            return

        store_entry = match[0]
        full_schema = MultiSourceDataStore.get_data_store_params_schema()
        schema = full_schema.properties[store_entry["store_id"]].properties

        for key, prop_schema in schema.items():
            ui, widgets_dict = render_schema(key, prop_schema)
            params_ui.append(ui)
            current_param_widgets.update(widgets_dict)

    selector.param.watch(rebuild_params, "value")

    rebuild_params()

    return SimpleNamespace(
        selector=selector,
        params_ui=params_ui,
        param_widgets=lambda: current_param_widgets,
    )


def on_store_id_change(event):
    global current_params_widgets
    identifier_input.value = event.new
    current_params_widgets = build_store_params(event.new)

store_id_select.param.watch(on_store_id_change, "value")

add_button = pn.widgets.Button(name="Add Store", button_type="primary")
cancel_button = pn.widgets.Button(name="Cancel", button_type="warning")

scrollable_params = pn.Column(
        store_params,
        sizing_mode="stretch_width"
    )

def make_store_modal_body():
    return pn.Column(
        pn.pane.Markdown("# Add a Data Store"),
        pn.layout.Divider(),
        identifier_input,
        store_id_select,
        pn.layout.Divider(),
        pn.pane.Markdown("#### Store Params"),
        pn.Column(store_params, sizing_mode="stretch_width"),
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
    background_close=True,
    sizing_mode="fixed"
)

toggle_button = modal.create_button("show", name="Add a Data Store")

def on_add_store(event):
    identifier = identifier_input.value.strip()

    if any(s["identifier"] == identifier for s in store_entries):
        pn.state.notifications.error(
            f"A store with identifier '{identifier}' already exists."
        )
        modal.hide()
        return

    def collect_nested_values(widget_map):
        result = {}
        for key, widget in widget_map.items():
            parts = key.split(".")
            d = result
            for p in parts[:-1]:
                d = d.setdefault(p, {})
            d[parts[-1]] = widget.value
        return result

    entry = {
        "identifier": identifier_input.value,
        "store_id": store_id_select.value,
        "store_params":  collect_nested_values(current_params_widgets)
    }

    store_entries.append(entry)
    for selector in [ds_store_selector, var_store_selector]:
        selector.selector.options = [s["identifier"] for s in store_entries]
    update_preview()
    modal.hide()

def on_cancel(event):
    modal.hide()

add_button.on_click(on_add_store)
cancel_button.on_click(on_cancel)

## Gridmapping

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
    ),
    name="Add Grid Mapping"
)

open_gm_modal_btn = gm_modal.create_button("show", name="Add Grid Mapping")

def on_add_gm(event):
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

### datasets

var_identifier = pn.widgets.TextInput(name="Variable Identifier", value="")
var_store = pn.widgets.Select(name="Store", options=[])
var_data_id = pn.widgets.TextInput(name="Data ID", value="")

var_open_params = pn.widgets.TextAreaInput(name="open_params (YAML)", value="")

add_var_btn = pn.widgets.Button(name="Add Variable", button_type="primary")
cancel_var_btn = pn.widgets.Button(name="Cancel", button_type="danger")

var_store_selector = make_store_selector(label="Store")

variable_modal = pn.Modal(
    pn.Column(
        pn.pane.Markdown("# Add Variable Object"),
        pn.layout.Divider(),
        var_identifier,
        var_store_selector.selector,
        var_data_id,
        pn.layout.Divider(),
        pn.pane.Markdown("#### Store Params"),
        var_store_selector.params_ui,
        pn.Row(add_var_btn, cancel_var_btn),
        sizing_mode="stretch_width",
    ),
    open=False,
    name="Add Variable",
    width=520,
    height=620,
    show_close_button=True,
    background_close=True,
    sizing_mode="fixed",
)

open_variable_modal_btn = variable_modal.create_button("show", name="Add Variable")

multi_variable_list = []

def on_add_variable(event):
    entry = {
        "identifier": var_identifier.value,
        "store": var_store.value,
        "data_id": var_data_id.value,
    }

    for key, widget in [
        ("open_params", var_open_params),
    ]:
        val = widget.value
        if val is not None:
            entry[key] = val

    multi_variable_list.append(entry)
    refresh_md_variable_display()
    variable_modal.hide()

cancel_var_btn.on_click(lambda e: variable_modal.hide())
add_var_btn.on_click(on_add_variable)


dataset_type = pn.widgets.RadioBoxGroup(
    name="Dataset Type",
    options=["single", "multi"],
    value="single",
    inline=True,
)


def update_data_ids(event):
    print('getattr(dataset_modal, "open", False)', getattr(dataset_modal, "open", False))
    if not getattr(dataset_modal, "open", False):
        return

    store_id = event.new
    matching = [s for s in store_entries if s["identifier"] == store_id]
    if not matching:
        ds_data_id.options = []
        ds_data_id.value = ""
        pn.state.notifications.error(
            f"No store parameters found for store '{store_id}'. Please add it first."
        )
        dataset_modal.hide()
        return

    store = matching[0]
    params = store["store_params"]

    ds_data_id.loading = True
    try:
        data_ids_schema = MultiSourceDataStore.list_data_ids(
            {store["store_id"]: params}
        )

        try:
            options = []
            for k in getattr(data_ids_schema, "properties", {}):
                enum = getattr(data_ids_schema.properties[k], "enum", None)
                if enum:
                    options = list(enum)
                    break
        except Exception:
            options = list(data_ids_schema) if data_ids_schema else []

        ds_data_id.options = options[:100]
        ds_data_id.value = options[0] if options else ""
    except Exception as e:
        ds_data_id.options = []
        ds_data_id.value = ""
        pn.state.notifications.error(f"Error fetching data IDs: {e}")
    finally:
        ds_data_id.loading = False


ds_identifier = pn.widgets.TextInput(name="Identifier", value="")
ds_store_selector = make_store_selector(label="Store")
ds_grid_mapping = pn.widgets.Select(name="Grid Mapping", options=[])
ds_data_id = pn.widgets.Select(name="Data ID", value="")
ds_format_id = pn.widgets.TextInput(name="Format ID (optional)", value="")

ds_store_selector.selector.param.watch(update_data_ids, "value")

md_identifier = pn.widgets.TextInput(name="Identifier", value="")
md_grid_mapping = pn.widgets.Select(name="Grid Mapping", options=[])
md_format_id = pn.widgets.TextInput(name="Format ID (optional)", value="")
md_var_display = pn.pane.Markdown("No variables added yet.")


def refresh_md_variable_display():
    if not multi_variable_list:
        md_var_display.object = "No variables added yet."
        return
    text = "### Variables Added\n"
    for i, v in enumerate(multi_variable_list, 1):
        text += f"- **{v['identifier']}** (store={v['store']}, data_id={v['data_id']})\n"
    md_var_display.object = text



def dataset_type_changed(event):
    if event.new == "single":
        multi_section.visible = False
        single_section.visible = True
    else:
        single_section.visible = False
        multi_section.visible = True

dataset_type.param.watch(dataset_type_changed, "value")


add_ds_btn = pn.widgets.Button(name="Add Dataset", button_type="primary")
cancel_ds_btn = pn.widgets.Button(name="Cancel", button_type="danger")

single_section = pn.Column(
    pn.pane.Markdown("### Single Dataset"),
    ds_identifier,
    ds_store_selector.selector,
    ds_grid_mapping,
    ds_data_id,
    ds_format_id,
    pn.layout.Divider(),
    pn.pane.Markdown("#### Store Params"),
    ds_store_selector.params_ui,
    sizing_mode="stretch_width",
)

multi_section = pn.Column(
    pn.pane.Markdown("### Multi Dataset"),
    md_identifier,
    md_grid_mapping,
    md_format_id,
    pn.pane.Markdown("### Variables"),
    md_var_display,
    open_variable_modal_btn,
    visible=False,
    sizing_mode="stretch_width",
)

dataset_modal = pn.Modal(
    pn.Column(
        pn.pane.Markdown("## Add Dataset"),
        dataset_type,
        single_section,
        multi_section,
        pn.Row(add_ds_btn, cancel_ds_btn),
        sizing_mode="stretch_width",
    ),
    open=False,
    name="Add Dataset",
    width=520,
    height=620,
    show_close_button=True,
    background_close=True,
    sizing_mode="fixed"
)

open_dataset_modal_btn = dataset_modal.create_button("show", name="Add Dataset")

def on_dataset_modal_open(event):
    if event.new:
        update_data_ids(type("Event", (), {"new": ds_store_selector.selector.value}))

dataset_modal.param.watch(on_dataset_modal_open, "open")


def on_add_dataset(event):
    if dataset_type.value == "single":
        entry = {
            "type": "single",
            "identifier": ds_identifier.value,
            "store": ds_store_selector.selector.value,
            "grid_mapping": ds_grid_mapping.value,
            "data_id": ds_data_id.value,
        }
        if ds_format_id.value:
            entry["format_id"] = ds_format_id.value

        for key, widget in [
            ("open_params", ds_store_selector.selector),
        ]:
            val = widget.value
            if val is not None:
                entry[key] = val

        dataset_entries.append(entry)

    else:
        entry = {
            "type": "multi",
            "identifier": md_identifier.value,
            "grid_mapping": md_grid_mapping.value,
        }
        if md_format_id.value:
            entry["format_id"] = md_format_id.value

        entry["variables"] = list(multi_variable_list)

        dataset_entries.append(entry)

        # multi_variable_list.clear()
        refresh_md_variable_display()

    update_preview()
    dataset_modal.hide()


def on_cancel_ds(event):
    dataset_modal.hide()

add_ds_btn.on_click(on_add_dataset)
cancel_ds_btn.on_click(on_cancel_ds)

layout = pn.Row(
    pn.Column(
        "## Data Stores",
        toggle_button,
        "## Grid Mappings",
        open_gm_modal_btn,
        gm_modal,
        "## Datasets",
        open_dataset_modal_btn,
        dataset_modal,
        variable_modal,
        modal,
        width=400,
        sizing_mode="fixed",
    ),
    preview,
    sizing_mode="stretch_width"
)

layout.servable()
