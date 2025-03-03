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


class GeneratorStatus(Enum):
    """Generator process status."""

    waiting = "waiting"
    started = "started"
    stopped = "stopped"
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
            # noinspection PyUnresolvedReferences
            from IPython.display import display

            if display is not None:
                try:
                    return IPyWidgetsGeneratorDisplay(states)
                except ImportError:
                    return IPyGeneratorDisplay(states)
        except ImportError:
            pass
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


class IPyGeneratorDisplay(GeneratorDisplay):
    def __init__(self, states: list[GeneratorState]):
        super().__init__(states)
        from IPython import display

        self._ipy_display = display

    def show(self):
        """Display the widget container."""
        self._ipy_display.display(self.to_html())

    def update(self):
        """Update the display."""
        self._ipy_display.clear_output(wait=True)
        self._ipy_display.display(self.to_html())


class IPyWidgetsGeneratorDisplay(IPyGeneratorDisplay):
    def __init__(self, states: list[GeneratorState]):
        super().__init__(states)
        import ipywidgets

        self._state_table = ipywidgets.HTML(self.to_html())
        self._output = ipywidgets.Output()  # not used yet
        self._container = ipywidgets.VBox([self._state_table, self._output])

    def show(self):
        """Display the widget container."""
        self._ipy_display.display(self._container)

    def update(self):
        """Update the display."""
        self._state_table.value = self.to_html()


def _to_dict(obj: object):
    return {
        k: v
        for k, v in obj.__dict__.items()
        if isinstance(k, str) and not k.startswith("_") and v is not None
    }
