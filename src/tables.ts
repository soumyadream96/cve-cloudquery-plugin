/* eslint-disable @typescript-eslint/require-await */
/* eslint-disable @typescript-eslint/naming-convention */
import fs from "node:fs/promises";
import Path from "node:path";

import type { DataType } from "@cloudquery/plugin-sdk-javascript/arrow";
import { Utf8, Int64, Float64 } from "@cloudquery/plugin-sdk-javascript/arrow";
import type {
  Column,
  ColumnResolver,
} from "@cloudquery/plugin-sdk-javascript/schema/column";
import type {
  Table,
  TableResolver,
} from "@cloudquery/plugin-sdk-javascript/schema/table";
import { createTable } from "@cloudquery/plugin-sdk-javascript/schema/table";
import { parse } from "csv-parse";
import dayjs from "dayjs";
import customParseFormat from "dayjs/plugin/customParseFormat.js";
import localizedFormat from "dayjs/plugin/localizedFormat.js";
import timezone from "dayjs/plugin/timezone.js";
import utc from "dayjs/plugin/utc.js";
import pMap from "p-map";
import type { Logger } from "winston";

import type { Spec } from "./spec.js";


/* eslint-disable import/no-named-as-default-member */
dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.extend(customParseFormat);
dayjs.extend(localizedFormat);

const getColumnResolver = (c: string): ColumnResolver => {
  return (meta, resource) => {
    const dataItem = resource.getItem();
    resource.setColumData(c, (dataItem as Record<string, unknown>)[c]);
    return Promise.resolve();
  };
};

const getColumnType = (value: string): DataType => {
  const number = Number(value);
  if(Number.isNaN(number)) return new Utf8();
  if(Number.isInteger(number)) return new Int64();
  return new Float64();
};

const getColumnTypes = (row: string[]): DataType[] => {
  return row.map((value)=>getColumnType(value));
};

const getTable = async (
  rows: string[][],
  tableName: string,
): Promise<Table> => {
  if (rows.length === 0) {
    throw new Error("No rows found");
  }
  const columnNames = rows[0];
  const getRecordObjectFromRow = (row: string[]) => {
    const record: Record<string, string | number> = {};
    for (const [index, element] of row.entries()) {
      record[columnNames[index]] = Number.isNaN(Number(element)) ? element : Number(element);
    }
    return record;
  };
  const columnTypes = rows.length > 1 ? getColumnTypes(rows[1]) : rows[0].map(()=>new Utf8());
  // convert all rows except column definitions to an array of Record<string, string> objects
  const tableRecords = rows.filter((_, index) => index > 0).map((r)=>getRecordObjectFromRow(r));
  const columnDefinitions: Column[] = columnNames.map((c, index) => ({
    name: c,
    type: columnTypes[index],
    description: "",
    primaryKey: false,
    notNull: false,
    incrementalKey: false,
    unique: false,
    ignoreInTests: false,
    resolver: getColumnResolver(c),
  }));

  const tableResolver: TableResolver = (clientMeta, parent, stream) => {
    for (const r of tableRecords) stream.write(r)
    return Promise.resolve();
  };
  return createTable({ name: tableName, columns: columnDefinitions, resolver: tableResolver });
};

const getCsvFiles = async (logger: Logger, path: string): Promise<string[]> => {
  const stats = await fs.stat(path);
  if (stats.isDirectory()) {
    const files = await fs.readdir(path, { withFileTypes: true });
    return files.filter((f) => f.isFile()).map((f) => Path.join(path, f.name));
  }
  logger.error("Target path is not a directory.");
  return [];
};

const parseCsvFile = async (path: string, csvDelimiter: string): Promise<string[][]> => {
  const content = await fs.readFile(path);
  return new Promise<string[][]>((resolve, reject) => {
    parse(content, { delimiter: csvDelimiter }, (error, records) => {
      if (error) {
        reject(error);
        return;
      }
      resolve(records);
    });
  });
}


export const getTables = async (
  logger: Logger,
  spec: Spec
): Promise<Table[]> => {
  const { path, csvDelimiter, concurrency } = spec;
  
  const files = await getCsvFiles(logger, path);
  logger.info(`done discovering files. Found ${files.length} files`);

  const allTables = await pMap(
    files,
    async (filePath) => {
      const csvFile = await parseCsvFile(filePath, csvDelimiter);
      return getTable(csvFile, Path.basename(filePath, ".csv"));
    },
    {
      concurrency,
    },
  );
  return allTables;
};
