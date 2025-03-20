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

import unittest

import numpy as np
import xarray as xr

from xcube_multistore.utils import (
    get_bbox,
    get_utm_zone,
    _normalize_grid_mapping,
    _safe_execute,
)


class TestSafeExecute(unittest.TestCase):

    def test_safe_execute_keyboard_interrupt(self):

        @_safe_execute()
        def test_function():
            raise KeyboardInterrupt

        result = test_function()
        self.assertEqual(result, "Keyboard Interrupt caught! Exiting gracefully.")


class TestGetUtmZone(unittest.TestCase):
    def test_northern_hemisphere(self):
        self.assertEqual(get_utm_zone(45.0, -75.0), "epsg:32618")
        self.assertEqual(get_utm_zone(10.0, 20.0), "epsg:32634")

    def test_southern_hemisphere(self):
        self.assertEqual(get_utm_zone(-10.0, 20.0), "epsg:32734")
        self.assertEqual(get_utm_zone(-33.9, 151.2), "epsg:32756")

    def test_boundary_conditions(self):
        self.assertEqual(get_utm_zone(0.0, 0.0), "epsg:32631")
        self.assertEqual(get_utm_zone(0.0, 179.9), "epsg:32660")
        self.assertEqual(get_utm_zone(0.0, -179.9), "epsg:32601")


class TestGetBBox(unittest.TestCase):

    def test_get_bbox_with_utm(self):
        lat, lon = 45.0, -93.0
        cube_width = 1000
        expected_crs = get_utm_zone(lat, lon)
        bbox, crs = get_bbox(lat, lon, cube_width)
        self.assertEqual(crs, expected_crs)
        self.assertEqual(len(bbox), 4)
        self.assertLess(bbox[0], bbox[2])
        self.assertLess(bbox[1], bbox[3])

    def test_get_bbox_with_specific_crs(self):
        lat, lon = 45.0, -93.0
        cube_width = 1000
        crs_final = "EPSG:3857"
        bbox, crs = get_bbox(lat, lon, cube_width, crs_final)
        self.assertEqual(crs, crs_final)
        self.assertEqual(len(bbox), 4)

    def test_get_bbox_at_equator(self):
        lat, lon = 0.0, 30.0
        cube_width = 500
        expected_crs = get_utm_zone(lat, lon)
        bbox, crs = get_bbox(lat, lon, cube_width)
        self.assertEqual(crs, expected_crs)
        self.assertEqual(len(bbox), 4)

    def test_get_bbox_near_poles(self):
        lat, lon = -89.0, 0.0
        cube_width = 10000
        expected_crs = get_utm_zone(lat, lon)
        bbox, crs = get_bbox(lat, lon, cube_width)
        self.assertEqual(crs, expected_crs)
        self.assertEqual(len(bbox), 4)


class TestNormalizeGridMapping(unittest.TestCase):

    def test_normalize_grid_mapping_no_gm_name(self):
        ds = xr.Dataset(
            {
                "temperature": (("x", "y"), np.random.rand(4, 4)),
                "humidity": (("x", "y"), np.random.rand(4, 4)),
            },
            coords={"x": np.arange(4), "y": np.arange(4)},
        )
        normalized_ds = _normalize_grid_mapping(ds)
        xr.testing.assert_identical(ds, normalized_ds)
