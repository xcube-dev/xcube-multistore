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

from abc import ABC
from enum import Enum

import tabulate
from xcube.util.assertions import assert_given, assert_instance

from .config import MultiSourceConfig


class GeneratorStatus(Enum):
    """Generator process status."""

    waiting = "waiting"
    started = "started"
    completed = "completed"
    failed = "failed"

    def __str__(self):
        return self.name.upper()

    def __repr__(self):
        return f"{self.__class__.__name__}.{self.name}"


class GeneratorState:
    """Generator state."""

    def __init__(
        self,
        identifier: str,
        status: GeneratorStatus | None = None,
        message: str | None = None,
        exception: BaseException | None = None,
    ):
        assert_given(identifier, name="identifier")
        self.identifier = identifier
        self.status = status
        self.message = message
        self.exception = exception

    def update(self, event: "GeneratorState"):
        """Update this state with the given partial state.

        Args:
            event: the partial state.
        """
        assert_instance(event, GeneratorState, name="event")
        if self.identifier == event.identifier:
            if event.status is not None:
                self.status = event.status
            if event.message is not None:
                self.message = event.message
            if event.exception is not None:
                self.exception = event.exception

    def __str__(self):
        return ", ".join(f"{k}={v}" for k, v in _to_dict(self).items())

    def __repr__(self):
        args = ", ".join(f"{k}={v!r}" for k, v in _to_dict(self).items())
        return f"{self.__class__.__name__}({args})"


class GeneratorDisplay(ABC):
    @classmethod
    def create(cls, states: list[GeneratorState]) -> "GeneratorDisplay":
        try:
            from IPython.display import display
            from IPython import get_ipython

            # Only use IPyGeneratorDisplay if we are actually inside a notebook
            shell = get_ipython().__class__.__name__
            if shell == "ZMQInteractiveShell":
                return IPyGeneratorDisplay(states)
        except (ImportError, NameError, AttributeError):
            pass

        # Default fallback: text-based display
        return GeneratorDisplay(states)

    def __init__(self, states: list[GeneratorState]):
        self.states = states

    def _repr_html_(self) -> str:
        return self.to_html()

    def to_text(self) -> str:
        return self.tabulate(table_format="simple")

    def to_html(self) -> str:
        return self.tabulate(table_format="html")

    def tabulate(self, table_format: str = "simple") -> str:
        """Generate HTML table from job list."""
        rows = [
            [
                state.identifier,
                f"{state.status}" if state.status is not None else "-",
                state.message or "-",
                state.exception or "-",
            ]
            for state in self.states
        ]

        return tabulate.tabulate(
            rows,
            headers=["Dataset identifier", "Status", "Message", "Exception"],
            tablefmt=table_format,
        )

    def show(self):
        """Display the widget container."""
        print(self.to_text())

    def update(self):
        """Update the display."""
        print(self.to_text())

    def display_title(self, title: str):
        """Display a title"""
        print(title)


class IPyGeneratorDisplay(GeneratorDisplay):
    def __init__(self, states: list[GeneratorState]):
        super().__init__(states)
        from IPython import display

        self._ipy_display = display
        self._html_display = None

    def show(self):
        """Display the widget container."""
        self._html_display = self._ipy_display.display(self.to_html(), display_id=True)

    def update(self):
        """Update the display."""
        if self._html_display is None:
            self._ipy_display.display(self.to_html())
        else:
            self._ipy_display.update_display(
                self.to_html(), display_id=self._html_display.display_id
            )

    def display_title(self, title: str):
        """Display a title"""
        self._ipy_display.display(
            self._ipy_display.HTML(f"<b style='font-size: 20px;'>{title}</b>")
        )


class ConfigDisplay(ABC):

    @classmethod
    def create(cls, config: MultiSourceConfig) -> "ConfigDisplay":
        try:
            from IPython.display import display
            from IPython import get_ipython

            # Only use IPyConfigDisplay if we are actually inside a notebook
            shell = get_ipython().__class__.__name__
            if shell == "ZMQInteractiveShell":
                return IPyConfigDisplay(config)
        except (ImportError, NameError, AttributeError):
            pass

        # Default fallback: text-based display
        return ConfigDisplay(config)

    def __init__(self, config: MultiSourceConfig):
        self.config = config

    def _repr_html_(self) -> str:
        return self.to_html()

    def to_text(self) -> str:
        return self.tabulate(table_format="simple")

    def to_html(self) -> str:
        return self.tabulate(table_format="html")

    def tabulate(self, table_format: str = "simple") -> str:
        """Generate HTML table from job list."""
        rows = []
        for dataset_id, config_ds in self.config.datasets.items():
            if "variables" not in config_ds:
                variable = {
                    k: v
                    for k, v in config_ds.items()
                    if k
                    not in [
                        "identifier",
                        "grid_mapping",
                        "format_id",
                        "xr_merge_params",
                    ]
                }
                variable["identifier"] = "-"
                config_ds_new = dict(
                    identifier=config_ds["identifier"],
                    variables=[variable],
                )

                if "grid_mapping" in config_ds:
                    config_ds_new["grid_mapping"] = config_ds["grid_mapping"]
                if "format_id" in config_ds:
                    config_ds_new["format_id"] = config_ds["format_id"]
                if "xr_merge_params" in config_ds:
                    config_ds_new["xr_merge_params"] = config_ds["xr_merge_params"]
                config_ds = config_ds_new

            for config_variable in config_ds["variables"]:
                config_store = self.config.data_stores[config_variable["store"]]
                gm_id = config_ds.get("grid_mapping")
                gm_display = None
                if gm_id:
                    if gm_id in self.config.datasets:
                        gm_display = f"Like {gm_id!r}"
                    elif hasattr(self.config, "grid_mappings"):
                        gm_display = self.config.grid_mappings.get(gm_id)
                    if gm_display is None:
                        gm_display = f"Grid mapping identifier {gm_id} not found."

                row = [
                    dataset_id,
                    config_store["store_id"],
                    _format_params(config_store.get("store_params")),
                    config_variable["data_id"],
                    _format_params(config_variable.get("open_params")),
                    _format_params(gm_display),
                    _format_params(config_variable.get("resample_params")),
                    _format_params(config_ds.get("format_id"), default="Zarr"),
                ]

                rows.append(row)

        return tabulate.tabulate(
            rows,
            headers=[
                "User-defined ID",
                "Data Store ID",
                "Data Store Params",
                "Data ID",
                "Open Data Params",
                "Grid-Mapping",
                "Resample Params",
                "Format",
            ],
            tablefmt=table_format,
            stralign="left",
        )

    def show(self):
        """Display the widget container."""
        print(self.to_text())

    def display_title(self, title: str):
        """Display a title"""
        print(title)


class IPyConfigDisplay(ConfigDisplay):
    def __init__(self, config: MultiSourceConfig):
        super().__init__(config)
        from IPython import display

        self._ipy_display = display
        self._html_display = None

    def show(self):
        """Display the widget container."""
        self._html_display = self._ipy_display.display(self.to_html(), display_id=True)

    def display_title(self, title: str):
        """Display a title"""
        self._ipy_display.display(
            self._ipy_display.HTML(f"<b style='font-size: 20px;'>{title}</b>")
        )


def _to_dict(obj: object):
    return {
        k: v
        for k, v in obj.__dict__.items()
        if isinstance(k, str) and not k.startswith("_") and v is not None
    }


def _format_params(params: dict | str | None = None, default: str = "-") -> str:
    if not params:
        return default
    if isinstance(params, dict):
        return "; ".join(f"{k}: {v}" for k, v in params.items() if k != "identifier")
    return str(params)
