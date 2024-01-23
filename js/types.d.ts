// Type definitions for @tableland/sqlparser
// Project: @tableland/sqlparser

declare module "@tableland/sqlparser" {
  /**
   * Initialize the @tableland/sqlparser module.
   * This must be called first to populate the sqlparser global namespace.
   *
   * @param input Input is provided primarily for testing purposes. It can be an optional `string`,
   * `Request`, or `URL` specifing a "remote" WASM source, or a `BufferSource` to specify a local WASM file.
   * It is best to leave undefined, and allow the library to use the included local WASM binary.
   * @return A `Promise` that resolves to a WASM `Exports` object.
   */
  export function init(
    input?: string | Request | URL | BufferSource
  ): Promise<WebAssembly.Exports>;

  export default init;

  /**
   * Initialize the @tableland/sqlparser module.
   * This is the synchronous counter-part to `init`, and is included primarily for testing purposes.
   *
   * @param bytes The input `bytes` must be a `BufferSource` to specify a local WASM file.
   * @return A WASM `Exports` object.
   */
  export function initSync(bytes: BufferSource): WebAssembly.Exports;

  /**
   * The WASM `Exports` object cache.
   */
  export const __wasm: WebAssembly.Exports | undefined;

  export type NormalizedStatement = sqlparser.NormalizedStatement;
  export type ValidatedTable = sqlparser.ValidatedTable;
  export type StatementType = sqlparser.StatementType;
}

declare namespace sqlparser {
  // StatementType is the type of SQL statement.
  export type StatementType = "read" | "write" | "create" | "acl";

  // NormalizedStatement is a statement that has been normalized.
  export interface NormalizedStatement {
    type: StatementType;
    statements: string[];
    tables: string[];
  }

  // ValidatedTable is a Table that has been validated.
  export interface ValidatedTable {
    name: string;
    prefix: string;
    chainId: number;
    tableId: number;
  }

  /**
   * Validate and normalize a string containing (possibly multiple) SQL statement(s).
   * @param sql A string containing SQL statement(s).
   * @param nameMap An optional object to use for re-mapping table names.
   * @return A `Promise` that resolves to an array of normalized SQL statements.
   */
  export function normalize(
    sql: string,
    nameMap?: Record<string, string>
  ): Promise<NormalizedStatement>;

  /**
   * Validate a table name.
   * @param tableName A string containing a table name of the form `prefix_chainId_tokenId`.
   * @return A `Promise` that resolves to a validated table object.
   */
  export function validateTableName(
    tableName: string,
    isCreate: true
  ): Promise<Omit<ValidatedTable, "tableId">>;
  export function validateTableName(
    tableName: string,
    isCreate?: boolean
  ): Promise<ValidatedTable>;

  /**
   * Get the set of unique table names from (possibly multiple) SQL statement(s).
   * @param sql A string containing SQL statement(s).
   * @return A `Promise` that resolves to an array of strings.
   */
  export function getUniqueTableNames(sql: string): Promise<string[]>;
}
