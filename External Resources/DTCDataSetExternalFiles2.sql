CREATE EXTERNAL DATA SOURCE [DTCDataSetExternalFiles2]
    WITH (
    TYPE = BLOB_STORAGE,
    LOCATION = N'https://salrsccadfnp.blob.core.windows.net/dtc-external-sourcefiles',
    CREDENTIAL = [DTCDataSourceExternalFilesCred]
    );

