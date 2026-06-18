# How to ...

### Change the location where final data cubes are stored

The final storage location for generated data cubes is defined by the user in the 
`data_stores` section, as described in the [configuration schema](config.md#entire-configuration-schema).

The storage location can be changed to a local directory using the `file` data store:

```yaml
- identifier: storage
  store_id: file
  store_params:
    root: <realative-path>
```

where `<relative-path>` is the path relative to the directory from which the
Multi-Source Data Store is executed.

Alternatively, the final data cube can be stored in an S3-compatible object
storage using:

```yaml
- identifier: storage
  store_id: s3
  store_params:
    root: <s3-bucket>
    storage_options: 
      anon: False
      key: <S3-key>
      secret: <S3-secret>
```

For additional S3 configuration options (e.g. custom endpoints or other
object-storage settings), refer to:

- [xcube.core.store.get_data_store_params_schema("s3")](https://xcube.readthedocs.io/en/latest/api.html#xcube.core.store.get_data_store_params_schema)
- [MultiSourceDataStore.get_data_store_params_schema("s3")](api/#xcube_multistore.multistore.MultiSourceDataStore.get_data_store_params_schema)
