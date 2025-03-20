<div style="text-align: right;">
  <figure style="display: inline-flex; align-items: center;">
    <img src="docs/assets/eo-lincs.png" alt="Logo" style="height: 100px;">
    <figcaption style="font-weight: bold; font-size: 10px; margin-left: 10px;">EO-LINCS</figcaption>
  </figure>
</div>

[![Build Status](https://github.com/xcube-dev/xcube-multistore/actions/workflows/unittest-workflow.yml/badge.svg?branch=main)](https://github.com/xcube-dev/xcube-multistore/actions)
[![codecov](https://codecov.io/gh/xcube-dev/xcube-multistore/branch/main/graph/badge.svg)](https://codecov.io/gh/xcube-dev/xcube-multistore)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)



# xcube-multistore

xcube Multi-Source Data Store: Seamlessly Integrating and Harmonizing Data from 
Multiple Sources.


## Development

### Setting up a development environment

The recommended Python distribution for development is 
[miniforge](https://conda-forge.org/download/) which includes 
conda, mamba, and their dependencies.

```shell
git clone https://github.com/xcube-dev/xcube-multistore.git
cd xcube-multistore
mamba env create
mamba activate xcube-multistore
pip install -ve .
```

### Install the library locally and test

```shell
mamba activate xcube-multistore
pip install -ve .
pytest
```

### Documentation

### Setting up a documentation environment

```shell
mamba activate xcube-multistore
pip install .[doc]
```

### Testing documentation changes

```shell
mkdocs serve
```

### Deploying documentation changes

```shell
mkdocs gh-deploy
```
