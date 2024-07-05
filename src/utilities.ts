import { ObjectField, RowSet, Type } from "@hasura/ndc-sdk-typescript";
import axios from "axios";
import { State } from ".";

export type TableIntrospectResult = {
  object_types: Record<string, ObjectField>;
  field_names: string[];
  primary_keys: string[];
  unique_keys: string[];
  nullable_keys: string[];
  field_types: { [k: string]: string };
  foreign_keys: { [k: string]: { table: string; column: string } };
};

// TODO: Add support for functional types.
const determine_type = (dataType: string): Type => {
  switch (dataType) {
    case "DATETIME":
      return { type: "named", name: "String" };
    case "INTEGER":
      return { type: "named", name: "Int" };
    case "REAL":
      return { type: "named", name: "Float" };
    case "TEXT":
      return { type: "named", name: "String" };
    case "BLOB":
      throw new Error("BLOB NOT SUPPORTED!");
    default:
      if (dataType.startsWith("NVARCHAR")) {
        return { type: "named", name: "String" };
      } else if (dataType.startsWith("NUMERIC")) {
        return { type: "named", name: "Float" };
      }
      throw new Error("NOT IMPLEMENTED");
  }
};

const wrap_nullable = (
  type: Type,
  isNotNull: boolean,
  isPrimaryKey: boolean
): Type => {
  if (isPrimaryKey) {
    return type; // Primary keys should never be nullable
  }
  return isNotNull ? type : { type: "nullable", underlying_type: type };
};

export const introspect_table = async (
  table_name: string,
  state: State
): Promise<TableIntrospectResult> => {
  const { account_id, database_id, bearerToken } = state;

  let columns_result: RowSet | undefined;

  try {
    const res = await axios.post(
      `https://api.cloudflare.com/client/v4/accounts/${account_id}/d1/database/${database_id}/query`,
      {
        sql: `PRAGMA table_info(${table_name})`,
      },
      {
        headers: {
          'Authorization': 'Bearer ' + bearerToken,
          'Content-Type': 'application/json',
        },
      }
    );

    if (res.data.success) {
      columns_result = {
        rows: res.data.result[0].results.map((r: any) => ({
          cid: r.cid,
          name: r.name,
          type: r.type,
          notnull: r.notnull,
          dflt_value: r.dflt_value,
          pk: r.pk
        }))
      };
    } else {
      throw new Error('Query failed');
    }
  } catch (error) {
    if (axios.isAxiosError(error)) {
      console.error('Error response:', error.response?.data);
      throw new Error(`Request failed: ${error.response?.status} ${error.response?.statusText}`);
    } else {
      console.error('Unexpected error:', error);
      throw new Error('An unexpected error occurred');
    }
  }

  if (!columns_result || !columns_result.rows) {
    throw new Error('No columns data found');
  }

  let response: TableIntrospectResult = {
    object_types: {},
    field_names: [],
    primary_keys: [],
    unique_keys: [],
    nullable_keys: [],
    field_types: {},
    foreign_keys: {},
  };

  try {
    for (const column of columns_result.rows) {
      if (typeof column.name !== "string") {
        throw new Error("Column name must be string");
      }

      const determined_type = determine_type(
        (column.type as string).toUpperCase()
      );
      const final_type = wrap_nullable(
        determined_type,
        column.notnull === 1,
        column.pk === 1
      );

      response.field_names.push(column.name);
      if ((column.pk as number) > 0) {
        response.primary_keys.push(column.name);
      }
      if (column.notnull === 0 && column.pk === 0) {
        response.nullable_keys.push(column.name);
      }
      if (determined_type.type === "named") {
        response.field_types[column.name] = determined_type.name;
      }
      response.object_types[column.name] = {
        type: final_type,
      };
    }

    const foreign_keys_result = await getForeignKeys(table_name, state);
    if (foreign_keys_result.rows && foreign_keys_result.rows.length > 0) {
      for (const fk of foreign_keys_result.rows) {
        response.foreign_keys[fk.from as string] = {
          table: fk.table as string,
          column: fk.to as string,
        };
      }
    }

    const index_list_result = await getIndexList(table_name, state);
    if (index_list_result.rows && index_list_result.rows.length > 0) {
      for (const index of index_list_result.rows) {
        if (index.unique) {
          const index_info_result = await getIndexInfo(index.name as string, state);
          if (index_info_result.rows && index_info_result.rows.length > 0) {
            for (const col of index_info_result.rows) {
              if (!response.unique_keys.includes(col.name as string)) {
                response.unique_keys.push(col.name as string);
              }
            }
          }
        }
      }
    }
  } finally {
    return response;
  }
};

const getForeignKeys = async (table_name: string, state: State): Promise<RowSet> => {
  const { account_id, database_id, bearerToken } = state;
  const res = await axios.post(
    `https://api.cloudflare.com/client/v4/accounts/${account_id}/d1/database/${database_id}/query`,
    {
      sql: `PRAGMA foreign_key_list(${table_name})`,
    },
    {
      headers: {
        'Authorization': 'Bearer ' + bearerToken,
        'Content-Type': 'application/json',
      },
    }
  );

  if (res.data.success) {
    return {
      rows: res.data.result[0].results.map((r: any) => ({
        from: r.from,
        table: r.table,
        to: r.to
      }))
    };
  } else {
    throw new Error('Query failed');
  }
};

const getIndexList = async (table_name: string, state: State): Promise<RowSet> => {
  const { account_id, database_id, bearerToken } = state;
  const res = await axios.post(
    `https://api.cloudflare.com/client/v4/accounts/${account_id}/d1/database/${database_id}/query`,
    {
      sql: `PRAGMA index_list(${table_name})`,
    },
    {
      headers: {
        'Authorization': 'Bearer ' + bearerToken,
        'Content-Type': 'application/json',
      },
    }
  );

  if (res.data.success) {
    const results = res.data.result[0].results;
    return {
      rows: results ? results.map((r: any) => ({
        name: r.name,
        unique: r.unique
      })) : []
    };
  } else {
    throw new Error('Query failed');
  }
};

const getIndexInfo = async (index_name: string, state: State): Promise<RowSet> => {
  const { account_id, database_id, bearerToken } = state;
  const res = await axios.post(
    `https://api.cloudflare.com/client/v4/accounts/${account_id}/d1/database/${database_id}/query`,
    {
      sql: `PRAGMA index_info(${index_name})`,
    },
    {
      headers: {
        'Authorization': 'Bearer ' + bearerToken,
        'Content-Type': 'application/json',
      },
    }
  );

  if (res.data.success) {
    return {
      rows: res.data.result[0].results.map((r: any) => ({
        name: r.name
      }))
    };
  } else {
    throw new Error('Query failed');
  }
};

type Resolver<T> = (value: T | PromiseLike<T>) => void;

export function createBlockingQueue<T>() {
  let queue: T[] = [];
  let waitingResolvers: Resolver<T>[] = [];

  function enqueue(item: T): void {
    if (waitingResolvers.length > 0) {
      // Resolve the first waiting dequeue if available
      const resolve = waitingResolvers.shift()!;
      resolve(item);
    } else {
      queue.push(item);
    }
  }

  async function dequeue(): Promise<T> {
    if (queue.length > 0) {
      // Return an item from the queue immediately
      return Promise.resolve(queue.shift()!);
    } else {
      // Wait for an item to be enqueued
      return new Promise<T>((resolve) => {
        waitingResolvers.push(resolve);
      });
    }
  }

  return { enqueue, dequeue };
}
