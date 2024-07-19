// @ts-check
import { rejects, strictEqual, deepStrictEqual, match } from "assert";
import { test, before, describe } from "mocha";
// eslint-disable-next-line no-unused-vars
import _init, { __wasm, init } from "../esm/out.js";

describe("sqlparser", function () {
  before(async function () {
    await init();
  });

  describe("normalize", function () {
    test("when there is a basic syntax error", async function () {
      await rejects(
        globalThis.sqlparser.normalize(
          "create table blah_5_ (id int, image blah, description text)"
        ),
        (/** @type {any} */ err) => {
          strictEqual(
            err.message,
            "error parsing statement: syntax error at position 40 near 'blah'"
          );
          return true;
        }
      );
    });

    test("when there is a single create statement", async function () {
      const { type, statements } = await globalThis.sqlparser.normalize(
        "CREATE table blah_5_ (id int, image blob, description text);"
      );
      strictEqual(type, "create");
      match(statements[0], /^create table blah_5_.*/);
    });

    test("when there is a single read statement", async function () {
      const { type, statements } = await globalThis.sqlparser.normalize(
        "select * FROM fake_table_1 where something='nothing';"
      );
      strictEqual(type, "read");
      match(statements[0], /^select \* from fake_table_1.*/);
    });

    test("when there is a single grant statement", async function () {
      const { type, statements } = await globalThis.sqlparser.normalize(
        "grant INSERT, update, DELETE on foo_1337_100 to '0xd43c59d569', '0x4afe8e30'"
      );
      strictEqual(type, "acl");
      match(statements[0], /^grant delete,insert,update on foo_1337_100.*/);
    });

    test("when there is a single revoke statement", async function () {
      const { type, statements } = await globalThis.sqlparser.normalize(
        "REVOKE insert, UPDATE, delete ON foo_1337_100 from '0xd43c59d569', '0x4afe8e30'"
      );
      strictEqual(type, "acl");
      match(statements[0], /^revoke delete,insert,update on foo_1337_100.*/);
    });

    test("when there is a single write statement", async function () {
      const { type, statements } = await globalThis.sqlparser.normalize(
        "insert INTO blah_5_ values (1, 'three', 'something');"
      );
      strictEqual(type, "write");
      match(statements[0], /^insert into blah_5_ values.*/);
    });

    test("where no arguments are passed to the function", async function () {
      await rejects(
        // @ts-expect-error error
        globalThis.sqlparser.normalize(),
        (/** @type {any} */ err) => {
          strictEqual(err.message, "missing required argument: statement");
          return true;
        }
      );
    });

    test("when a single statement with mixed case is normalized", async function () {
      const { type, statements } = await globalThis.sqlparser.normalize(
        "select * FrOM fake_table_1 WHere something='nothing';"
      );
      strictEqual(type, "read");
      strictEqual(
        statements.pop(),
        "select * from fake_table_1 where something='nothing'"
      );
    });

    test("when there are multiple write statements", async function () {
      const { type, statements } = await globalThis.sqlparser.normalize(`
      insert into blah_5_ values (1, 'three', 'something');
      update blah_5_ set description='something';
      `);
      strictEqual(type, "write");
      deepStrictEqual(statements, [
        "insert into blah_5_ values(1,'three','something')",
        "update blah_5_ set description='something'",
      ]);
    });

    test("when there is a syntax error in a latter statement", async function () {
      await rejects(
        globalThis.sqlparser.normalize(`
      insert into blah_5_ values (1, 'three', 'something');
      update syn tax err set foo;
      `),
        (/** @type {any} */ err) => {
          strictEqual(
            err.message,
            "error parsing statement: syntax error at position 81 near 'tax'"
          );
          return true;
        }
      );
    });

    test("when there is a non-syntax error", async function () {
      await rejects(
        globalThis.sqlparser.normalize("select AUTOINCREMENT from t;"),
        (/** @type {any} */ err) => {
          strictEqual(
            err.message,
            "error parsing statement: 1 error occurred:\n\t* keyword not allowed: AUTOINCREMENT\n\n"
          );
          return true;
        }
      );
    });

    test("when an empty statement is passed", async function () {
      const result = globalThis.sqlparser.normalize("");
      await rejects(result, (/** @type {any} */ err) => {
        strictEqual(err.message, "error parsing statement: empty string");
        return true;
      });
    });

    test("when create and mutate calls are mixed it fails", async function () {
      await rejects(
        globalThis.sqlparser.normalize(
          "create table blah_5_ (id int, image blob, description text);insert into blah_5_ values (1, 'three', 'something');"
        ),
        (/** @type {any} */ err) => {
          strictEqual(
            err.message,
            "error parsing statement: syntax error at position 66 near 'insert'"
          );
          return true;
        }
      );
    });

    test("when create and query calls are mixed it fails", async function () {
      await rejects(
        globalThis.sqlparser.normalize(
          "create table blah_5_ (id int, image blob, description text);select * from blah_5_;"
        ),
        (/** @type {any} */ err) => {
          strictEqual(
            err.message,
            "error parsing statement: syntax error at position 66 near 'select'"
          );
          return true;
        }
      );
    });

    test("when query and write calls are mixed it fails", async function () {
      await rejects(
        globalThis.sqlparser.normalize(
          "select * from blah_5_;insert into blah_5_ values (1, 'three', 'something');"
        ),
        (/** @type {any} */ err) => {
          strictEqual(
            err.message,
            "error parsing statement: syntax error at position 28 near 'insert'"
          );
          return true;
        }
      );
    });

    test("when a mix of acl and write types are provided the type is write", async function () {
      const { type } = await globalThis.sqlparser.normalize(
        "grant insert on foo_1337_100 to '0xd43c59d569';insert into foo_1337_100 values (1, 'three', 'something');"
      );
      strictEqual(type, "write");
    });

    test("when the ordering of write/acl types doesn't affect the type", async function () {
      const { type } = await globalThis.sqlparser.normalize(
        "insert into foo_1337_100 values (1, 'three', 'something');revoke insert on foo_1337_100 from '0xd43c59d569';"
      );
      strictEqual(type, "write");
    });

    test("when there is a really long statement", async function () {
      await rejects(
        globalThis.sqlparser.normalize(
          "insert INTO blah_5_1 values (1, 'three', 'something');".repeat(700)
        ),
        (/** @type {any} */ err) => {
          strictEqual(
            err.message,
            "statement size error: larger than specified max"
          );
          return true;
        }
      );
    });

    test("when re-mapping table names on the fly", async function () {
      const { statements, tables } = await globalThis.sqlparser.normalize(
        "select `t1`.id, t3.* from t1, t2 join t3 join (select * from t4);",
        { t1: "table1", t2: "table2", t3: "table3" } // Leave t4 "as is"
      );

      // Note the canonical "join" added below to replace the comma
      strictEqual(
        statements.join(""),
        "select `table1`.id,table3.* from table1 join table2 join table3 join(select * from t4)"
      );
      deepStrictEqual(tables, ["table1", "table2", "table3", "t4"]);
    });

    test("when mapping names to something invalid throws an error", async function () {
      await rejects(
        globalThis.sqlparser.normalize(
          "select `t1`.id, t3.* from t1, t2 join t3 join (select * from t4);",
          { t1: "@#$%^&", t2: "valid", t3: "3.14" } // Leave t4 "as is"
        ),
        (/** @type {any} */ err) => {
          strictEqual(
            err.message,
            "error updating statement: table name has wrong format: @#$%^&"
          );
          return true;
        }
      );
    });

    test("when mapping names to table without a prefix", async function () {
      const { tables, statements, type } = await globalThis.sqlparser.normalize(
        "select * from t1;",
        { t1: "_31337_123" }
      );

      deepStrictEqual(tables, ["_31337_123"]);
      deepStrictEqual(statements, [
        'select * from _31337_123',
      ]);
      strictEqual(type, "read");
    });

    test("when mapping ens names to something else", async function () {
      // Also tests escape wrappers ``, [], and ""
      const { tables, statements, type } = await globalThis.sqlparser.normalize(
        'select `table.one.ens`.id, [table.two.ens].* from `table.one.ens`, [table.two.ens] join "table.three.ens" join (select * from t4);',
        {
          "table.one.ens": "t1",
          "table.two.ens": "t2",
          "table.three.ens": "t3",
        } // Leave t4 "as is"
      );
      deepStrictEqual(tables, ["t1", "t2", "t3", "t4"]);
      deepStrictEqual(statements, [
        'select `t1`.id,[t2].* from `t1` join [t2] join "t3" join(select * from t4)',
      ]);
      strictEqual(type, "read");
    });

    test("when insert with flat sub-select", async function () {
      const { type } = await globalThis.sqlparser.normalize(
        "insert into foo_1337_1 (id) select v from blah_1337_2;"
      );
      strictEqual(type, "write");
    });

    test("when insert with group by and having", async function () {
      const { type } = await globalThis.sqlparser.normalize(
        "insert into foo_1337_1 (id) select v from blah_1337_2 group by v having count(v) > 1;"
      );
      strictEqual(type, "write");
    });

    test("when insert with compound select throws an error", async function () {
      await rejects(
        globalThis.sqlparser.normalize(
          "insert into foo_1337_1 (id) select v from blah_1337_2 union select j from blah_1337_3;"
        ),
        (/** @type {any} */ err) => {
          strictEqual(
            err.message,
            "error parsing statement: 1 error occurred:\n\t* compound select is not allowed\n\n"
          );
          return true;
        }
      );
    });
  });

  describe("getUniqueTableNames()", function () {
    test("when there is a statement syntax error", async function () {
      await rejects(
        globalThis.sqlparser.getUniqueTableNames("create nothing;"),
        (/** @type {any} */ err) => {
          strictEqual(
            err.message,
            "error parsing statement: syntax error at position 14 near 'nothing'"
          );
          return true;
        }
      );
    });
    test("where no arguments are passed to the function", async function () {
      await rejects(
        // @ts-expect-error error
        globalThis.sqlparser.getUniqueTableNames(),
        (/** @type {any} */ err) => {
          strictEqual(err.message, "missing required argument: statement");
          return true;
        }
      );
    });
    test("when a create statement is provided", async function () {
      const tables = await globalThis.sqlparser.getUniqueTableNames(
        "create table blah_5_ (id int, image blob, description text);"
      );
      strictEqual(tables.pop(), "blah_5_");
    });

    test("when a write statement is provided", async function () {
      const tables = await globalThis.sqlparser.getUniqueTableNames(
        "insert into blah_5_ values (1, 'three', 'something');"
      );
      strictEqual(tables.pop(), "blah_5_");
    });

    test("when a read statement is provided", async function () {
      const tables = await globalThis.sqlparser.getUniqueTableNames(
        "select t1.id, t3.* from t1, t2 join t3 join (select * from t4);"
      );
      deepStrictEqual(tables, ["t1", "t2", "t3", "t4"]);
    });

    test("when multiple write statements are provided", async function () {
      const tables = await globalThis.sqlparser.getUniqueTableNames(
        "insert into blah_5_ values (1, 'five', 'something');insert into blah_3_ values (1, 'three', 'nothing');"
      );
      deepStrictEqual(tables, ["blah_5_", "blah_3_"]);
    });

    test("when an empty statement is provided", async function () {
      const tables = await globalThis.sqlparser.getUniqueTableNames("");
      deepStrictEqual(tables, []);
    });
  });

  describe("validateTableName()", function () {
    test("when provided with invalid table names", async function () {
      const invalidNames = [
        "t",
        "t2",
        "t_2_",
        "t_",
        "__",
        "t__",
        "t_2__",
        "__1",
      ];
      for (const tableName of invalidNames) {
        await rejects(
          globalThis.sqlparser.validateTableName(tableName),
          (/** @type {any} */ err) => {
            strictEqual(
              err.message,
              `error validating name: table name has wrong format: ${tableName}`
            );
            return true;
          }
        );
      }
    });

    test("when provided with a valid table name", async function () {
      const validatedTable = await globalThis.sqlparser.validateTableName(
        "t_1_2"
      );
      deepStrictEqual(validatedTable, {
        name: "t_1_2",
        chainId: 1,
        tableId: 2,
        prefix: "t",
      });
    });

    test("when provided with a valid table name with multiple chars in prefix", async function () {
      const validatedTable = await globalThis.sqlparser.validateTableName(
        "table_1_2"
      );
      deepStrictEqual(validatedTable, {
        name: "table_1_2",
        chainId: 1,
        tableId: 2,
        prefix: "table",
      });
    });

    test("when provided with a valid table name without prefix", async function () {
      const validatedTable = await globalThis.sqlparser.validateTableName(
        "_1_2"
      );
      deepStrictEqual(validatedTable, {
        name: "_1_2",
        chainId: 1,
        tableId: 2,
        prefix: "",
      });
    });

    test("when provided with a valid create table name", async function () {
      const validatedTable = await globalThis.sqlparser.validateTableName(
        "t_1",
        true
      );
      deepStrictEqual(validatedTable, {
        name: "t_1",
        chainId: 1,
        prefix: "t",
      });
      // strictEqual(validatedTable.tableId, undefined);
    });

    test("when provided with a valid create table name without prefix", async function () {
      const validatedTable = await globalThis.sqlparser.validateTableName(
        "_1",
        true
      );
      deepStrictEqual(validatedTable, {
        name: "_1",
        chainId: 1,
        prefix: "",
      });
    });
  });
});
