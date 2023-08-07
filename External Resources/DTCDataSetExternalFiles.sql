CREATE EXTERNAL DATA SOURCE [DTCDataSetExternalFiles]
    WITH (
    TYPE = BLOB_STORAGE,
    LOCATION = N'https://salrsccadfnp.blob.core.windows.net/dtc-external-sourcefiles',
    CREDENTIAL = [DTCDataSourceExternalFilesCred]
    );

