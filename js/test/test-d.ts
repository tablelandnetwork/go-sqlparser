/* eslint-disable no-undef */
/* eslint-disable no-unused-expressions */
import { expectType } from "tsd";
import defaultInit, {
  initSync,
  __wasm,
  init,
  NormalizedStatement,
  ValidatedTable,
  StatementType,
  CreateTable,
} from "@tableland/sqlparser";

expectType<Promise<WebAssembly.Exports>>(defaultInit());
expectType<Promise<WebAssembly.Exports>>(init("blah"));
expectType<WebAssembly.Exports>(initSync(new Uint8Array([1, 2, 3])));
expectType<WebAssembly.Exports | undefined>(__wasm);

expectType<Promise<NormalizedStatement>>(
  globalThis.sqlparser.normalize("select * from table where id = 1;", {
    some: "map",
  })
);

const {
  normalize,
  validateTableName,
  getUniqueTableNames,
  createStatementToObject,
  createStatementFromObject,
} = globalThis.sqlparser;

expectType<Promise<CreateTable>>(
  createStatementToObject("select * from table where id = 1;")
);
expectType<Promise<string>>(
  createStatementFromObject({
    Table: { Name: "table", IsTarget: true },
    ColumnsDef: [],
    Constraints: [],
    StrictMode: false,
  })
);

expectType<Promise<NormalizedStatement>>(
  normalize("select * from table where id = 1;")
);
const { type, statements, tables } = await normalize(
  "select * from table where id = 1;"
);
expectType<StatementType>(type);
expectType<string[]>(statements);
expectType<string[]>(tables);
expectType<Promise<string[]>>(
  getUniqueTableNames(
    "select t1.id, t3.* from t1, t2 join t3 join (select * from t4);"
  )
);
expectType<Promise<ValidatedTable>>(validateTableName("valid_name_80001_1"));
expectType<Promise<ValidatedTable>>(
  validateTableName("valid_name_80001_1", false)
);
expectType<Promise<Omit<ValidatedTable, "tableId">>>(
  validateTableName("valid_name_80001_1", true)
);
