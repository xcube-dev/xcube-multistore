import unittest
from unittest.mock import patch, MagicMock
import sys
from io import StringIO

from xcube_multistore.visualization import (
    GeneratorState,
    GeneratorStatus,
    GeneratorDisplay,
    IPyGeneratorDisplay,
    IPyWidgetsGeneratorDisplay,
)


class TestGeneratorStateMethods(unittest.TestCase):

    def setUp(self):
        self.state = GeneratorState(
            identifier="dataset1",
            status=GeneratorStatus.started,
            message="Processing started",
            exception=None,
        )

    def test_str_method(self):
        expected_str = (
            "identifier=dataset1, status=STARTED, " "message=Processing started"
        )
        self.assertEqual(str(self.state), expected_str)

    def test_repr_method(self):
        expected_repr = (
            "GeneratorState(identifier='dataset1', status=GeneratorStatus.started, "
            "message='Processing started')"
        )
        self.assertEqual(repr(self.state), expected_repr)


class TestGeneratorDisplay(unittest.TestCase):

    def test_create_ipywidgets(self):
        states = [GeneratorState(identifier="dataset1", status=GeneratorStatus.started)]
        display_instance = GeneratorDisplay.create(states)
        self.assertIsInstance(display_instance, IPyWidgetsGeneratorDisplay)

    def test_create_ipy(self):
        with patch("ipywidgets.HTML", side_effect=ImportError):
            states = [
                GeneratorState(identifier="dataset1", status=GeneratorStatus.started)
            ]
            display_instance = GeneratorDisplay.create(states)
            self.assertIsInstance(display_instance, IPyGeneratorDisplay)
            self.assertNotIsInstance(display_instance, IPyWidgetsGeneratorDisplay)

    @patch.dict(sys.modules, {"IPython.display": None})
    def test_create_fallback(self):
        states = [GeneratorState(identifier="dataset1", status=GeneratorStatus.started)]
        display_instance = GeneratorDisplay.create(states)
        self.assertIsInstance(display_instance, GeneratorDisplay)
        self.assertNotIsInstance(display_instance, IPyGeneratorDisplay)

    def test_repr_html(self):
        states = [GeneratorState(identifier="dataset1", status=GeneratorStatus.started)]
        display_instance = GeneratorDisplay(states)
        expected_html = display_instance.to_html()
        self.assertEqual(display_instance._repr_html_(), expected_html)

    def test_to_text(self):
        states = [
            GeneratorState(
                identifier="dataset1",
                status=GeneratorStatus.started,
                message="Started",
                exception=None,
            ),
            GeneratorState(
                identifier="dataset2",
                status=GeneratorStatus.failed,
                message="Error",
                exception=None,
            ),
        ]
        display_instance = GeneratorDisplay(states)

        expected_text = (
            "Dataset identifier    Status    Message    Exception\n"
            "--------------------  --------  ---------  -----------\n"
            "dataset1              STARTED   Started    -\n"
            "dataset2              FAILED    Error      -"
        )

        self.assertEqual(display_instance.to_text(), expected_text)

    def test_to_html(self):
        """Test the to_html method."""
        states = [
            GeneratorState(
                identifier="dataset1",
                status=GeneratorStatus.started,
                message="Started",
                exception=None,
            ),
            GeneratorState(
                identifier="dataset2",
                status=GeneratorStatus.failed,
                message="Error",
                exception=None,
            ),
        ]
        display_instance = GeneratorDisplay(states)

        expected_html = (
            "<table>\n<thead>\n<tr><th>Dataset identifier  </th><th>Status  "
            "</th><th>Message  </th><th>Exception  </th></tr>\n</thead>\n"
            "<tbody>\n<tr><td>dataset1            </td><td>STARTED "
            "</td><td>Started  </td><td>-          </td></tr>\n<tr><td>dataset2"
            "            </td><td>FAILED  </td><td>Error    </td><td>-          "
            "</td></tr>\n</tbody>\n</table>"
        )

        self.assertEqual(display_instance.to_html(), expected_html)

    @patch("sys.stdout", new_callable=StringIO)
    def test_show(self, mock_stdout):
        states = [
            GeneratorState(
                identifier="dataset1",
                status=GeneratorStatus.started,
                message="Processing",
                exception=None,
            )
        ]
        display_instance = GeneratorDisplay(states)
        display_instance.show()
        expected_stdout = (
            "Dataset identifier    Status    Message     Exception\n"
            "--------------------  --------  ----------  -----------\n"
            "dataset1              STARTED   Processing  -\n"
        )
        self.assertEqual(expected_stdout, mock_stdout.getvalue())

    @patch("sys.stdout", new_callable=StringIO)
    def test_update(self, mock_stdout):
        states = [
            GeneratorState(
                identifier="dataset1",
                status=GeneratorStatus.started,
                message="Processing",
                exception=None,
            )
        ]
        display_instance = GeneratorDisplay(states)
        display_instance.update()
        expected_stdout = (
            "Dataset identifier    Status    Message     Exception\n"
            "--------------------  --------  ----------  -----------\n"
            "dataset1              STARTED   Processing  -\n"
        )
        self.assertEqual(expected_stdout, mock_stdout.getvalue())
