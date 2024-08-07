import {
  QueryRequest,
  Expression,
  QueryResponse,
  RowSet,
  Conflict,
  Forbidden,
  Query,
  Relationship,
} from "@hasura/ndc-sdk-typescript";
import { Configuration, State } from "..";
import { MAX_32_INT } from "../constants";
import axios from "axios";
const SqlString = require("sqlstring-sqlite");
// import { format } from "sql-formatter";

const escape_single = (s: any) => SqlString.escape(s);
const escape_double = (s: any) => `"${SqlString.escape(s).slice(1, -1)}"`;

type QueryVariables = {
  [key: string]: any;
};

export type SQLiteQuery = {
  sql: string;
  args: any[];
};

function wrap_data(s: string): string {
  return `
SELECT
(
  ${s}
) as data
`;
}

function wrap_rows(s: string): string {
  return `
SELECT
  JSON_OBJECT('rows', JSON_GROUP_ARRAY(JSON(r)))
FROM
  (
    ${s}
  )
`;
}

function build_where(
  expression: Expression,
  collection_relationships: {
    [k: string]: Relationship;
  },
  args: any[],
  variables: QueryVariables,
  filter_joins: string[],
  config: Configuration,
  prefix: string
): string {
  if (!config.config) {
    throw new Forbidden("Internal Server Error", {});
  }
  let sql = "";
  switch (expression.type) {
    case "unary_comparison_operator":
      switch (expression.operator) {
        case "is_null":
          sql = `${expression.column.name} IS NULL`;
          break;
        default:
          throw new Conflict("Unknown Unary Comparison Operator", {
            "Unknown Operator": "This should never happen.",
          });
      }
      break;
    case "binary_comparison_operator":
      switch (expression.value.type) {
        case "scalar":
          args.push(expression.value.value);
          break;
        case "variable":
          if (variables !== null) {
            args.push(variables[expression.value.name]);
          }
          break;
        case "column":
          throw new Forbidden("Not implemented", {});
        default:
          throw new Conflict("Unknown Binary Comparison Value Type", {});
      }
      const columnName = expression.column.name;

      if (expression.column.type === "column" && expression.column.path.length > 0) {
        let currentAlias = prefix;
        let aliases: { [key: string]: string } = {};

        for (let path_elem of expression.column.path) {
          const collection_relationship = collection_relationships[path_elem.relationship];
          const relationship = JSON.parse(path_elem.relationship);
          const from = relationship[0];
          const to = collection_relationship.target_collection;
          const from_details = config.config.object_fields[from.name];

          let joined = false;
          for (let [key, value] of Object.entries(from_details.foreign_keys)) {
            if (value.table === to) {
              let toAlias;
              if (aliases[to]) {
                toAlias = aliases[to];
              } else {
                toAlias = `${currentAlias}_${to}`;
                aliases[to] = toAlias;
              }
              const join_str = ` JOIN ${escape_double(to)} AS ${escape_double(toAlias)} ON ${escape_double(currentAlias)}.${escape_double(key)} = ${escape_double(toAlias)}.${escape_double(value.column)} `;
              if (!filter_joins.includes(join_str)) {
                filter_joins.push(join_str);
              }
              currentAlias = toAlias;
              joined = true;
              break;
            }
          }

          if (!joined) {
            const to_details = config.config.object_fields[to];
            for (let [key, value] of Object.entries(to_details.foreign_keys)) {
              if (value.table === from.name) {
                let toAlias;
                if (aliases[to]) {
                  toAlias = aliases[to];
                } else {
                  toAlias = `${currentAlias}_${to}`;
                  aliases[to] = toAlias;
                }
                const join_str = ` JOIN ${escape_double(to)} AS ${escape_double(toAlias)} ON ${escape_double(toAlias)}.${escape_double(key)} = ${escape_double(currentAlias)}.${escape_double(value.column)} `;
                if (!filter_joins.includes(join_str)) {
                  filter_joins.push(join_str);
                }
                currentAlias = toAlias;
                joined = true;
                break;
              }
            }
          }
        }

        sql = `${escape_double(currentAlias)}.${escape_double(expression.column.name)} = ?`;
        break;
      }

      switch (expression.operator) {
        case "_eq":
          sql = `${escape_double(prefix)}.${escape_double(columnName)} = ?`;
          break;
        case "_like":
          sql = `${escape_double(prefix)}.${escape_double(columnName)} LIKE ?`;
          break;
        case "_glob":
          sql = `${escape_double(prefix)}.${escape_double(columnName)} GLOB ?`;
          break;
        case "_neq":
          sql = `${escape_double(prefix)}.${escape_double(columnName)} != ?`;
          break;
        case "_gt":
          sql = `${escape_double(prefix)}.${escape_double(columnName)} > ?`;
          break;
        case "_lt":
          sql = `${escape_double(prefix)}.${escape_double(columnName)} < ?`;
          break;
        case "_gte":
          sql = `${escape_double(prefix)}.${escape_double(columnName)} >= ?`;
          break;
        case "_lte":
          sql = `${escape_double(prefix)}.${escape_double(columnName)} <= ?`;
          break;
        default:
          throw new Forbidden("Binary Comparison Custom Operator not implemented", {});
      }
      break;
    case "and":
      if (expression.expressions.length === 0) {
        sql = "1";
      } else {
        const clauses = [];
        for (const expr of expression.expressions) {
          const res = build_where(expr, collection_relationships, args, variables, filter_joins, config, prefix);
          clauses.push(res);
        }
        sql = `(${clauses.join(` AND `)})`;
      }
      break;
    case "or":
      if (expression.expressions.length === 0) {
        sql = "1";
      } else {
        const clauses = [];
        for (const expr of expression.expressions) {
          const res = build_where(expr, collection_relationships, args, variables, filter_joins, config, prefix);
          clauses.push(res);
        }
        sql = `(${clauses.join(` OR `)})`;
      }
      break;
    case "not":
      const not_result = build_where(expression.expression, collection_relationships, args, variables, filter_joins, config, prefix);
      sql = `NOT (${not_result})`;
      break;
    case "exists":
      const { in_collection, predicate } = expression;

      let subquery_sql = "";
      let subquery_alias = `${prefix}_exists`;

      if (in_collection.type === "related") {
        const relationship = collection_relationships[in_collection.relationship];

        let from_collection = relationship.target_collection;
        subquery_sql = `
          SELECT 1
          FROM ${escape_double(from_collection)} AS ${escape_double(subquery_alias)}
          WHERE ${predicate ? build_where(predicate, collection_relationships, args, variables, [], config, subquery_alias) : '1 = 1'}
          AND ${Object.entries(relationship.column_mapping).map(([from, to]) => {
          return `${escape_double(prefix)}.${escape_double(from)} = ${escape_double(subquery_alias)}.${escape_double(to)}`;
        }).join(" AND ")}
        `;
      } else if (in_collection.type === "unrelated") {
        throw new Forbidden("Unrelated collection type not supported!", {});
      }

      sql = `EXISTS (${subquery_sql})`;
      break;
    default:
      throw new Forbidden("Unknown Expression Type!", {});
  }
  return sql;
}

function build_query(
  config: Configuration,
  query_request: QueryRequest,
  collection: string,
  query: Query,
  path: string[],
  variables: QueryVariables,
  args: any[],
  relationship_key: string | null,
  collection_relationships: {
    [k: string]: Relationship;
  }
): SQLiteQuery {
  let sql = "";
  path.push(collection);
  let collection_alias = path.join("_");

  let limit_sql = ``;
  let offset_sql = ``;
  let order_by_sql = ``;
  let collect_rows = [];
  let where_conditions = ["WHERE 1"];
  if (query.aggregates) {
    throw new Forbidden("Aggregates not implemented yet!", {});
  }
  if (query.fields) {
    for (let [field_name, field_value] of Object.entries(query.fields)) {
      collect_rows.push(escape_single(field_name));
      switch (field_value.type) {
        case "column":
          collect_rows.push(`${escape_double(collection_alias)}.${escape_double(field_value.column)}`);
          break;
        case "relationship":
          collect_rows.push(
            `(${build_query(
              config,
              query_request,
              field_name,
              field_value.query,
              path,
              variables,
              args,
              field_value.relationship,
              collection_relationships
            ).sql
            })`
          );
          path.pop();
          break;
        default:
          throw new Forbidden("The types tricked me. 😭", {});
      }
    }
  }
  let from_sql = `${escape_double(collection)} as ${escape_double(
    collection_alias
  )}`;
  if (path.length > 1 && relationship_key !== null) {
    let relationship = query_request.collection_relationships[relationship_key];
    let parent_alias = path.slice(0, -1).join("_");
    from_sql = `${escape_double(
      relationship.target_collection
    )} as ${escape_double(collection_alias)}`;
    where_conditions.push(
      ...Object.entries(relationship.column_mapping).map(([from, to]) => {
        return `${escape_double(parent_alias)}.${escape_double(
          from
        )} = ${escape_double(collection_alias)}.${escape_double(to)}`;
      })
    );
  }

  const filter_joins: string[] = [];

  if (query.predicate) {
    where_conditions.push(`(${build_where(query.predicate, query_request.collection_relationships, args, variables, filter_joins, config, collection_alias)})`);
  }

  if (query.order_by && config.config) {
    let order_elems: string[] = [];
    for (let elem of query.order_by.elements) {
      switch (elem.target.type) {
        case "column":
          if (elem.target.path.length === 0) {
            order_elems.push(
              `${escape_double(elem.target.name)} ${elem.order_direction}`
            );
          } else {
            let currentAlias = collection_alias;
            for (let path_elem of elem.target.path) {
              const relationship = collection_relationships[path_elem.relationship];
              const nextAlias = `${currentAlias}_${relationship.target_collection}`;
              const join_str = `JOIN ${escape_double(relationship.target_collection)} AS ${escape_double(nextAlias)} ON ${Object.entries(relationship.column_mapping).map(([from, to]) => `${escape_double(currentAlias)}.${escape_double(from)} = ${escape_double(nextAlias)}.${escape_double(to)}`).join(" AND ")}`;
              if (!filter_joins.includes(join_str)) {
                filter_joins.push(join_str);
              }
              currentAlias = nextAlias;
            }
            order_elems.push(
              `${escape_double(currentAlias)}.${escape_double(elem.target.name)} ${elem.order_direction}`
            );
          }
          break;
        case "single_column_aggregate":
          throw new Forbidden(
            "Single Column Aggregate not supported yet",
            {}
          );
        case "star_count_aggregate":
          throw new Forbidden(
            "Star Count Aggregate not supported yet",
            {}
          );
        default:
          throw new Forbidden("The types lied 😭", {});
      }
    }
    if (order_elems.length > 0) {
      order_by_sql = `ORDER BY ${order_elems.join(" , ")}`;
    }
  }

  if (query.limit) {
    limit_sql = `LIMIT ${escape_single(query.limit)}`;
  }

  if (query.offset) {
    if (!query.limit) {
      limit_sql = `LIMIT ${MAX_32_INT}`;
    }
    offset_sql = `OFFSET ${escape_single(query.offset)}`;
  }

  sql = wrap_rows(`
SELECT
JSON_OBJECT(${collect_rows.join(",")}) as r
FROM ${from_sql}
${filter_joins.join("")}
${where_conditions.join(" AND ")}
${order_by_sql}
${limit_sql}
${offset_sql}
`);

  if (path.length === 1) {
    sql = wrap_data(sql);
  }

  return {
    sql,
    args,
  };
}

export async function plan_queries(
  configuration: Configuration,
  query: QueryRequest
): Promise<SQLiteQuery[]> {
  if (!configuration.config) {
    throw new Forbidden("Connector is not properly configured", {});
  }

  let query_plan: SQLiteQuery[];
  if (query.variables) {
    let promises = query.variables.map((var_set) => {
      let query_variables: QueryVariables = var_set;
      return build_query(
        configuration,
        query,
        query.collection,
        query.query,
        [],
        query_variables,
        [],
        null,
        query.collection_relationships
      );
    });
    query_plan = await Promise.all(promises);
  } else {
    let promise = build_query(
      configuration,
      query,
      query.collection,
      query.query,
      [],
      {},
      [],
      null,
      query.collection_relationships
    );
    query_plan = [promise];
  }
  return query_plan;
}

async function perform_query(
  state: State,
  query_plans: SQLiteQuery[]
): Promise<RowSet[]> {
  try {
    const { account_id, database_id, bearerToken } = state;

    const queryData = query_plans.map(plan => ({
      sql: plan.sql,
      args: plan.args,
    }));

    const response = await Promise.all(queryData.map(async query => {
      const res = await axios.post(
        `https://api.cloudflare.com/client/v4/accounts/${account_id}/d1/database/${database_id}/query`,
        {
          sql: query.sql,
          params: query.args,
        },
        {
          headers: {
            'Authorization': 'Bearer ' + bearerToken,
            'Content-Type': 'application/json',
          },
        }
      );

      if (res.data.success) {
        return res.data.result.map((result: any) => {
          try {
            const resultString = result.results[0]?.data;
            if (typeof resultString !== 'string') {
              throw new Error('Result is not a valid JSON string');
            }
            const parsedData = JSON.parse(resultString);
            return {
              aggregates: parsedData.aggregates || null,
              rows: parsedData.rows || null,
            } as RowSet;
          } catch (parseError) {
            console.error('Error parsing result:', parseError);
            throw new Error('Failed to parse query result');
          }
        });
      } else {
        throw new Error('Query failed');
      }
    }));

    return response.flat();
  } catch (e) {
    console.error('ERROR', e);
    let errorMessage = 'An unknown error occurred';
    if (e instanceof Error) {
      errorMessage = e.message;
    }
    throw new Error('Failed to perform the Query: ' + errorMessage);
  }
}

export async function do_query(
  configuration: Configuration,
  state: State,
  query: QueryRequest
): Promise<QueryResponse> {
  let query_plans = await plan_queries(configuration, query);
  return perform_query(state, query_plans);
}
