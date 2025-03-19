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

from xcube.core.gridmapping import GridMapping

from .config import MultiSourceConfig


class GridMappings:

    @classmethod
    def setup_grid_mappings(cls, config: MultiSourceConfig):
        for config_gm in config.grid_mappings:
            setattr(
                cls,
                config_gm["identifier"],
                _get_regular_gridmapping(
                    **{k: v for k, v in config_gm.items() if k != "identifier"}
                ),
            )
        return cls


def _get_regular_gridmapping(
    bbox: list[float] | list[int],
    spatial_res: int | float,
    crs: str,
    tile_size: int | tuple[int, int] = 1024,
) -> GridMapping:
    """Creates a regular grid mapping for a given coordinate reference system based on
    the given bounding box and spatial resolution.

    Args:
        bbox: Bounding box coordinates in the format [west, south, east, north].
            The values must be in the given CRS.
        spatial_res: Spatial resolution of the grid.
        crs: Coordinate reference system in a string format (e.g., "EPSG:4326").
        tile_size: Chunk size for the grid; if a single int is given,
            square chunk size is assumed. Defaults to 1024.

    Returns:
        A regular grid mapping object.
    """
    x_size = int((bbox[2] - bbox[0]) / spatial_res)
    y_size = int(abs(bbox[3] - bbox[1]) / spatial_res)
    return GridMapping.regular(
        size=(x_size, y_size),
        xy_min=(bbox[0], bbox[1]),
        xy_res=spatial_res,
        crs=crs,
        tile_size=tile_size,
    )
