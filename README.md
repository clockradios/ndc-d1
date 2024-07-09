# Hasura d1 Connector
<a href="https://developers.cloudflare.com/d1/"><img src="https://github.com/hasura/ndc-turso/blob/main/docs/logo.svg" align="right" width="200"></a>

[![Docs](https://img.shields.io/badge/docs-v3.x-brightgreen.svg?style=flat)](https://hasura.io/connectors/d1)
[![ndc-hub](https://img.shields.io/badge/ndc--hub-turso-blue.svg?style=flat)](https://hasura.io/connectors/d1)
[![License](https://img.shields.io/badge/license-Apache--2.0-purple.svg?style=flat)](https://github.com/hasura/ndc-d1/blob/main/LICENSE.txt)
[![Status](https://img.shields.io/badge/status-alpha-yellow.svg?style=flat)](https://github.com/hasura/ndc-d1/blob/main/README.md)

The Hasura d1 Connector allows for connecting to a Cloudflare d1 database to give you an instant GraphQL API on top of your d1 data.

This connector is built using the [Typescript Data Connector SDK](https://github.com/hasura/ndc-sdk-typescript) and implements the [Data Connector Spec](https://github.com/hasura/ndc-spec).

* [Connector information in the Hasura Hub](https://hasura.io/connectors/d1)
* [Hasura V3 Documentation](https://hasura.io/docs/3.0/index/)

## Features

Below, you'll find a matrix of all supported features for the d1 connector:

| Feature                         | Supported | Notes |
| ------------------------------- | --------- | ----- |
| Native Queries + Logical Models | ❌     |       |
| Simple Object Query             | ✅     |       |
| Filter / Search                 | ✅     |       |
| Simple Aggregation              | ❌     |       |
| Sort                            | ✅     |       |
| Paginate                        | ✅     |       |
| Table Relationships             | ✅     |       |
| Views                           | ❌     |       |
| Distinct                        | ❌     |       |
| Remote Relationships            | ✅     |       |
| Custom Fields                   | ❌     |       |
| Mutations                       | ✅     |       |

## Before you get Started

[Prerequisites or recommended steps before using the connector.]

1. The [DDN CLI](https://hasura.io/docs/3.0/cli/installation) and [Docker](https://docs.docker.com/engine/install/) installed
2. A [supergraph](https://hasura.io/docs/3.0/getting-started/init-supergraph)
3. A [subgraph](https://hasura.io/docs/3.0/getting-started/init-subgraph)
4. Have a [Cloudflare](https://cloudflare.com/) hosted database.

The steps below explain how to Initialize and configure a connector for local development. You can learn how to deploy a
connector — after it's been configured — [here](https://hasura.io/docs/3.0/getting-started/deployment/deploy-a-connector).

## Using the d1 connector

### Step 1: Authenticate your CLI session

```bash
ddn auth login
```

### Step 2: Initialize the connector

```bash
ddn connector init d1  --subgraph my_subgraph  --hub-connector hasura/d1
```

In the snippet above, we've used the subgraph `my_subgraph` as an example; however, you should change this
value to match any subgraph which you've created in your project.

### Step 3: Modify the connector's port

When you initialized your connector, the CLI generated a set of configuration files, including a Docker Compose file for
the connector. Typically, connectors default to port `8080`. Each time you add a connector, we recommend incrementing the published port by one to avoid port collisions.

As an example, if your connector's configuration is in `my_subgraph/connector/d1/docker-compose.d1.yaml`, you can modify the published port to reflect a value that isn't currently being used by any other connectors:

```yaml
ports:
  - mode: ingress
    target: 8080
    published: "8082"
    protocol: tcp
```

### Step 4: Add environment variables

Now that our connector has been scaffolded out for us, we need to provide a connection string so that the data source can be introspected and the boilerplate configuration can be taken care of by the CLI.

The CLI has provided an `.env.local` file for our connector in the `my_subgraph/connector/d1` directory. We need to add three environment variables.

| Environment Variable   | Example Value               |
|------------------------|-----------------------------|
| CF_ACCOUNT_ID          | 1234456                     |
| D1_DATABASE_ID         | 543210                      |
| CF_BEARER_TOKEN        | 123456.abcdef.1h5k          |


The file, after adding the variables, should look like this example:

```env
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://local.hasura.dev:4317
OTEL_SERVICE_NAME=my_subgraph_d1
CF_ACCOUNT_ID=1234456
D1_DATABASE_ID=543210
CF_BEARER_TOKEN=123456.abcdef.1h5k
```

### Step 5: Introspect your data source

With the connector configured, we can now use the CLI to introspect our database and create a source-specific configuration file for our connector.

```bash
ddn connector introspect --connector my_subgraph/connector/d1/connector.yaml
```

### Step 6. Create the Hasura metadata

Hasura DDN uses a concept called "connector linking" to take [NDC-compliant](https://github.com/hasura/ndc-spec)
configuration JSON files for a data connector and transform them into an `hml` (Hasura Metadata Language) file as a
[`DataConnectorLink` metadata object](https://hasura.io/docs/3.0/supergraph-modeling/data-connectors#dataconnectorlink-dataconnectorlink).

Basically, metadata objects in `hml` files define our API.

First we need to create this `hml` file with the `connector-link add` command and then convert our configuration files
into `hml` syntax and add it to this file with the `connector-link update` command.

Let's name the `hml` file the same as our connector, `d1`:

```bash
ddn connector-link add d1 --subgraph my_subgraph
```

The new file is scaffolded out at `my_subgraph/metadata/d1/d1.hml`.

### Step 7. Update the environment variables

The generated file has two environment variables — one for reads and one for writes — that you'll need to add to your subgraph's `.env.my_subgraph` file. Each key is prefixed by the subgraph name, an underscore, and the name of the connector. Ensure the port value matches what is published in your connector's docker compose file.

As an example:

```env
MY_SUBGRAPH_D1_READ_URL=http://local.hasura.dev:<port>
MY_SUBGRAPH_D1_WRITE_URL=http://local.hasura.dev:<port>
```

These values are for the connector itself and utilize `local.hasura.dev` to ensure proper resolution within the docker container.

### Step 8. Start the connector's Docker Compose

Let's start our connector's Docker Compose file by running the following from inside the connector's subgraph:

```bash
docker compose -f docker-compose.d1.yaml up
```

### Step 9. Update the new `DataConnectorLink` object

Finally, now that our `DataConnectorLink` has the correct environment variables configured for the connector, we can run the update command to have the CLI look at the configuration JSON and transform it to reflect our database's schema in `hml` format. In a new terminal tab, run:

```bash
ddn connector-link update d1 --subgraph my_subgraph
```

After this command runs, you can open your `my_subgraph/metadata/d1.hml` file and see your metadata completely
scaffolded out for you 🎉

## Documentation

View the full documentation for the d1 connector [here](https://github.com/hasura/ndc-d1/blob/main/docs/index.md).

## Contributing

Check out our [contributing guide](https://github.com/hasura/ndc-d1/blob/main/docs/contributing.md) for more details.

## License

The d1 connector is available under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).