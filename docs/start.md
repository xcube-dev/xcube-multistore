# Getting Started

By installing the `xcube-multistore` package into an existing Python environment
using

```bash
pip install xcube-multistore
```

or

```bash
conda install -c conda-forge xcube-multistore
```

you are ready to go and use the Mutli-Source Data Store as follows:

```python
from xcube_multistore import MultiSourceDataStore

msds = MultiSourceDataStore("config.yml")
```
