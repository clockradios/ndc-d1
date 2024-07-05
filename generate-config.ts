import * as fs from "fs";
import { promisify } from "util";
import { introspect_table } from "./src/utilities";
import { BASE_FIELDS, BASE_TYPES } from "./src/constants";
import { Configuration, ObjectFieldDetails, State } from "./src";
import axios from "axios";
import { RowSet } from "@hasura/ndc-sdk-typescript";
const writeFile = promisify(fs.writeFile);
const readFile = promisify(fs.readFile);
let HASURA_CONFIGURATION_DIRECTORY = process.env["HASURA_CONFIGURATION_DIRECTORY"] as string | undefined;
if (HASURA_CONFIGURATION_DIRECTORY === undefined || HASURA_CONFIGURATION_DIRECTORY.length === 0) {
  HASURA_CONFIGURATION_DIRECTORY = ".";
}

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

if ([CF_ACCOUNT_ID, D1_DATABASE_ID, CF_BEARER_TOKEN].includes(undefined)) {
  throw new Error('Undefined Environment Variables');
}

const state: State = {
  account_id: CF_ACCOUNT_ID,
  database_id: D1_DATABASE_ID,
  bearerToken: CF_BEARER_TOKEN
}

async function main() {
  let tables_result
  const resp = await axios.post(
    `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/d1/database/${D1_DATABASE_ID}/query`,
    {
      sql: "SELECT name FROM sqlite_master WHERE type='table' AND name <> 'sqlite_sequence' AND name <> 'sqlite_stat1'  AND name <> '_cf_KV'",
    },
    {
      headers: {
        'Authorization': 'Bearer ' + CF_BEARER_TOKEN,
        'Content-Type': 'application/json',
      },
    }
  );
  if (resp.data.success) {
    tables_result = resp.data.result[0]
  } else {
    throw new Error('Query failed');
  }
  const table_names = tables_result.results.map((row: any) => String(row.name));
  let object_types: Record<string, any> = {
    ...BASE_TYPES,
  };

  const object_fields: Record<string, ObjectFieldDetails> = {};
  for (const table_name of table_names) {
    const field_dict = await introspect_table(table_name, state);
    object_types[table_name] = {
      fields: {
        ...field_dict.object_types,
        ...BASE_FIELDS,
      },
    };
    object_fields[table_name] = {
      field_names: field_dict.field_names,
      field_types: field_dict.field_types,
      primary_keys: field_dict.primary_keys,
      unique_keys: field_dict.unique_keys,
      nullable_keys: field_dict.nullable_keys,
      foreign_keys: field_dict.foreign_keys,
    };
  }
  const res: Configuration = {
    config: {
      collection_names: table_names,
      object_fields: object_fields,
      object_types: object_types
    },
  };
  const jsonString = JSON.stringify(res, null, 4);
  let filePath = `${HASURA_CONFIGURATION_DIRECTORY}/config.json`;
  try {
    const existingData = await readFile(filePath, 'utf8');
    if (existingData !== jsonString) {
      await writeFile(filePath, jsonString);
      console.log('File updated.');
    } else {
      console.log('No changes detected. File not updated.');
    }
  } catch (error) {
    await writeFile(filePath, jsonString);
    console.log('New file written.');
  }
}

main();
