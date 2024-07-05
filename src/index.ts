import {
  SchemaResponse,
  ObjectType,
  QueryRequest,
  QueryResponse,
  MutationRequest,
  MutationResponse,
  CapabilitiesResponse,
  ExplainResponse,
  start,
  Connector,
  Forbidden
} from "@hasura/ndc-sdk-typescript";
import { CAPABILITIES_RESPONSE } from "./constants";
import { do_query } from "./handlers/query";
import { do_mutation } from "./handlers/mutation";
import { do_explain } from "./handlers/explain";
import { do_get_schema } from "./handlers/schema";
import { readFileSync } from "fs"; // Import synchronous file read function


let CF_ACCOUNT_ID = process.env["CF_ACCOUNT_ID"] as string | undefined;
if (CF_ACCOUNT_ID?.length === 0) {
  CF_ACCOUNT_ID = undefined;
}
let D1_DATABASE_ID = process.env["D1_DATABASE_ID"] as string | undefined;
if (D1_DATABASE_ID?.length === 0) {
  D1_DATABASE_ID = undefined;
}
let CF_BEARER_TOKEN = process.env["CF_BEARER_TOKEN"] as string | undefined;
if (CF_BEARER_TOKEN?.length === 0) {
  CF_BEARER_TOKEN = undefined;
}

export type ObjectFieldDetails = {
  field_names: string[];
  field_types: { [k: string]: string };
  primary_keys: string[];
  unique_keys: string[];
  nullable_keys: string[];
  foreign_keys: { [k: string]: { table: string; column: string } };
};

export type ConfigurationSchema = {
  collection_names: string[];
  object_types: { [k: string]: ObjectType };
  object_fields: { [k: string]: ObjectFieldDetails };
};

export type Configuration = {
  config?: ConfigurationSchema;
};

// export type RawConfiguration = Configuration;

export type State = {
  account_id: string | undefined;
  database_id?: string | undefined;
  bearerToken?: string | undefined;
};

const connector: Connector<Configuration, State> = {
  /**
   * Validate the configuration files provided by the user, returning a validated 'Configuration',
   * or throwing an 'Error'. Throwing an error prevents Connector startup.
   * @param configuration
   */
  parseConfiguration(configurationDir: string): Promise<Configuration> {
    try {
      const configLocation = `${configurationDir}/config.json`;
      const fileContent = readFileSync(configLocation, 'utf8');
      const configObject: Configuration = JSON.parse(fileContent);
      return Promise.resolve(configObject);
    } catch (error) {
      console.error("Failed to parse configuration:", error);
      throw new Forbidden(
        "Internal Server Error, server configuration is invalid",
        {}
      );
    }
  },

  /**
   * Initialize the connector's in-memory state.
   *
   * For example, any connection pools, prepared queries,
   * or other managed resources would be allocated here.
   *
   * In addition, this function should register any
   * connector-specific metrics with the metrics registry.
   * @param configuration
   * @param metrics
   */
  tryInitState(_: Configuration, __: unknown): Promise<State> {
    return Promise.resolve({
      account_id: CF_ACCOUNT_ID,
      database_id: D1_DATABASE_ID,
      bearerToken: CF_BEARER_TOKEN,
    });
  },

  /**
   * Get the connector's capabilities.
   *
   * This function implements the [capabilities endpoint](https://hasura.github.io/ndc-spec/specification/capabilities.html)
   * from the NDC specification.
   * @param configuration
   */
  getCapabilities(_: Configuration): CapabilitiesResponse {
    return CAPABILITIES_RESPONSE;
  },

  /**
   * Get the connector's schema.
   *
   * This function implements the [schema endpoint](https://hasura.github.io/ndc-spec/specification/schema/index.html)
   * from the NDC specification.
   * @param configuration
   */
  async getSchema(configuration: Configuration): Promise<SchemaResponse> {
    if (!configuration.config) {
      throw new Forbidden(
        "Internal Server Error, server configuration is invalid",
        {}
      );
    }
    return Promise.resolve(do_get_schema(configuration));
  },

  /**
   * Explain a query by creating an execution plan
   *
   * This function implements the [explain endpoint](https://hasura.github.io/ndc-spec/specification/explain.html)
   * from the NDC specification.
   * @param configuration
   * @param state
   * @param request
   */
  queryExplain(
    configuration: Configuration,
    _: State,
    request: QueryRequest
  ): Promise<ExplainResponse> {
    if (!configuration.config) {
      throw new Forbidden(
        "Internal Server Error, server configuration is invalid",
        {}
      );
    }
    return do_explain(configuration, request);
  },

  /**
   * Explain a mutation by creating an execution plan
   * @param configuration
   * @param state
   * @param request
   */
  mutationExplain(
    configuration: Configuration,
    _: State,
    request: MutationRequest
  ): Promise<ExplainResponse> {
    if (!configuration.config) {
      throw new Forbidden(
        "Internal Server Error, server configuration is invalid",
        {}
      );
    }
    throw new Forbidden("Not implemented", {});
  },

  /**
   * Execute a query
   *
   * This function implements the [query endpoint](https://hasura.github.io/ndc-spec/specification/queries/index.html)
   * from the NDC specification.
   * @param configuration
   * @param state
   * @param request
   */
  query(
    configuration: Configuration,
    state: State,
    request: QueryRequest
  ): Promise<QueryResponse> {
    if (!configuration.config) {
      throw new Forbidden(
        "Internal Server Error, server configuration is invalid",
        {}
      );
    }
    return do_query(configuration, state, request);
  },

  /**
   * Execute a mutation
   *
   * This function implements the [mutation endpoint](https://hasura.github.io/ndc-spec/specification/mutations/index.html)
   * from the NDC specification.
   * @param configuration
   * @param state
   * @param request
   */
  mutation(
    configuration: Configuration,
    state: State,
    request: MutationRequest
  ): Promise<MutationResponse> {
    return do_mutation(configuration, state, request);
  },

  /**
   * Check the health of the connector.
   *
   * For example, this function should check that the connector
   * is able to reach its data source over the network.
   * @param configuration
   * @param state
   */
  healthCheck(_: Configuration, __: State): Promise<undefined> {
    // TODO
    return Promise.resolve(undefined);
  },

  /**
   *
   * Update any metrics from the state
   *
   * Note: some metrics can be updated directly, and do not
   * need to be updated here. This function can be useful to
   * query metrics which cannot be updated directly, e.g.
   * the number of idle connections in a connection pool
   * can be polled but not updated directly.
   * @param configuration
   * @param state
   */
  fetchMetrics(_: Configuration, __: State): Promise<undefined> {
    // TODO: Metrics
    return Promise.resolve(undefined);
  },
};

start(connector);
