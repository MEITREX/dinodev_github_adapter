This folder should contains the graphql schema files.

Folder structure:

- `service` - all schema files for this service

  It is recommended to separate the schema files by domain, for example: `user.graphqls`, `product.graphqls`, etc.

  ⚠️ The file ending must be `.graphqls` to be recognized by the graphql-java library. ⚠️

- `common` - all schema files from the common repo

  To update these external schema files, run the gradle task `fetchAllExternalSchemas`:

   ```shell
    ./gradlew fetchExternalSchemas
    ```

