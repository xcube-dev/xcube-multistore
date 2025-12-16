import sys
import unittest
from io import StringIO
from unittest.mock import MagicMock, patch

from xcube_multistore.config import MultiSourceConfig
from xcube_multistore.visualization import (
    ConfigDisplay,
    GeneratorDisplay,
    GeneratorState,
    GeneratorStatus,
    IPyConfigDisplay,
    IPyGeneratorDisplay,
    _format_params,
)

from .sample_data import get_config_dict0


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

    def test_create(self):
        states = [GeneratorState(identifier="dataset1", status=GeneratorStatus.started)]
        display_instance = GeneratorDisplay.create(states)
        self.assertIsInstance(display_instance, GeneratorDisplay)

    @patch("IPython.get_ipython")
    def test_create_ipy(self, mock_get_ipython):
        # Simulate Jupyter Notebook environment
        mock_shell = MagicMock()
        mock_shell.__class__.__name__ = "ZMQInteractiveShell"
        mock_get_ipython.return_value = mock_shell

        states = [GeneratorState(identifier="dataset1", status=GeneratorStatus.started)]
        display_instance = GeneratorDisplay.create(states)
        assert isinstance(display_instance, IPyGeneratorDisplay)

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


class TestIPyGeneratorDisplay(unittest.TestCase):
    def setUp(self):
        self.states = [
            GeneratorState(
                identifier="dataset1",
                status=GeneratorStatus.started,
                message="Processing",
                exception=None,
            )
        ]

    @patch("IPython.display.display")
    def test_show_html_display(self, mock_ipy_display):
        mock_disp_obj = MagicMock()
        mock_disp_obj.display_id = "disp1"
        mock_ipy_display.return_value = mock_disp_obj

        display_instance = IPyGeneratorDisplay(self.states)
        display_instance.show()
        mock_ipy_display.assert_called_once_with(
            display_instance.to_html(), display_id=True
        )
        self.assertEqual(display_instance._html_display.display_id, "disp1")

    @patch("IPython.display.update_display")
    @patch("IPython.display.display")
    def test_update(self, mock_display, mock_update_display):
        display_instance = IPyGeneratorDisplay(self.states)
        self.assertIsNone(display_instance._html_display)
        display_instance.update()
        mock_display.assert_called_once_with(display_instance.to_html())

        # set display_instance._html_display to not None
        display_instance.show()

        display_instance.update()
        mock_update_display.assert_called_once_with(
            display_instance.to_html(),
            display_id=display_instance._html_display.display_id,
        )

    @patch("IPython.display.HTML")
    @patch("IPython.display.display")
    def test_display_title(self, mock_display, mock_html):
        mock_html.return_value = "<b style='font-size: 20px;'>My Title</b>"

        display_instance = IPyGeneratorDisplay(self.states)
        display_instance.display_title("My Title")

        mock_html.assert_called_once_with("<b style='font-size: 20px;'>My Title</b>")
        mock_display.assert_called_once_with(mock_html.return_value)


class TestConfigDisplay(unittest.TestCase):
    def setUp(self):
        config_dict = get_config_dict0()
        self.config = MultiSourceConfig(config_dict)
        self.display = ConfigDisplay(self.config)

    def test_to_text(self):
        table_string = self.display.to_text()
        self.assertIn("dataset1", table_string)
        self.assertIn("datasource", table_string)
        self.assertIn("bbox", table_string)
        self.assertIn("spatial_res", table_string)

    def test_to_html(self):
        table_thml = self.display.to_html()
        self.assertIn("<table>", table_thml)
        self.assertIn("datasource", table_thml)
        self.assertIn("bbox", table_thml)
        self.assertIn("spatial_res", table_thml)
        self.assertIn("</th><th>", table_thml)
        self.assertEqual(self.display.to_html(), self.display._repr_html_())

    @patch("sys.stdout", new_callable=StringIO)
    def test_show_prints_table(self, mock_stdout):
        self.display.show()
        output = mock_stdout.getvalue()
        self.assertIn("dataset1", output)
        self.assertIn("bbox", output)

    @patch("sys.stdout", new_callable=StringIO)
    def test_display_title(self, mock_stdout):
        self.display.display_title("My Config Title")
        self.assertEqual("My Config Title\n", mock_stdout.getvalue())

    def test_format_params(self):
        params = {"a": 1, "b": 2}
        self.assertEqual(_format_params(params), "a: 1; b: 2")
        self.assertEqual(_format_params(None), "-")
        self.assertEqual(_format_params("hello"), "hello")

    def test_create_fallback(self):
        inst = ConfigDisplay.create(self.config)
        self.assertIsInstance(inst, ConfigDisplay)

    @patch("IPython.get_ipython")
    def test_create_ipy(self, mock_get_ipython):
        # Simulate Jupyter Notebook environment
        mock_shell = MagicMock()
        mock_shell.__class__.__name__ = "ZMQInteractiveShell"
        mock_get_ipython.return_value = mock_shell

        inst = ConfigDisplay.create(self.config)
        self.assertIsInstance(inst, IPyConfigDisplay)


class TestIPyConfigDisplay(unittest.TestCase):
    def setUp(self):
        config_dict = get_config_dict0()
        self.config = MultiSourceConfig(config_dict)
        self.display = IPyConfigDisplay(self.config)

    @patch("IPython.display.display")
    def test_show_html_display(self, mock_ipy_display):
        mock_disp_obj = MagicMock()
        mock_disp_obj.display_id = "disp1"
        mock_ipy_display.return_value = mock_disp_obj

        self.display.show()
        mock_ipy_display.assert_called_once_with(
            self.display.to_html(), display_id=True
        )
        self.assertEqual(self.display._html_display.display_id, "disp1")

    @patch("IPython.display.HTML")
    @patch("IPython.display.display")
    def test_display_title(self, mock_display, mock_html):
        mock_html.return_value = "<b style='font-size: 20px;'>My Title</b>"

        self.display.display_title("My Title")

        mock_html.assert_called_once_with("<b style='font-size: 20px;'>My Title</b>")
        mock_display.assert_called_once_with(mock_html.return_value)
