# Getting Started

By installing the `xcube-multistore` package into an existing Python environment
using

```bash
conda install -c conda-forge xcube-multistore
```

you are ready to go and use the Multi-Source Data Store as follows:

```python
from xcube_multistore.multistore import MultiSourceDataStore

msds = MultiSourceDataStore("config.yml")
```
