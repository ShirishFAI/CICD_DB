CREATE EXTERNAL DATA SOURCE [DTCDataSetExternal]
    WITH (
    TYPE = BLOB_STORAGE,
    LOCATION = N'https://salrsccadfnp.blob.core.windows.net/dtc-external-sourcefiles',
    CREDENTIAL = [DTCDataSourceExternalCred]
    );

