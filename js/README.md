# Table of Contents

- [@tableland/sqlparser](#tablelandsqlparser)
- [Table of Contents](#table-of-contents)
- [Background](#background)
- [Install](#install)
- [Usage](#usage)
- [Testing](#testing)
- [Feedback](#feedback)
- [Contributing](#contributing)
- [License](#license)

# Background

Experimental WASM build of Tableland's [sqlparser](https://github.com/tablelandnetwork/sqlparser).

This is a WASM-based Javascript library that wraps Tableland's Go-based [custom SQL parser](https://github.com/tablelandnetwork/sqlparser). The parser is tuned to parse SQL statements as defined by the [Tableland SQL Specification](https://docs.tableland.xyz/sql-specification).

The API for this library is minimal. The main export exposes an initialization function (see [Usage](#usage)) which adds a `sqlparser` object to the global namespace (due to Go WASM build quirks), which includes a the `normalize`, `validateTableName`, and `getUniqueTableNames` functions.

# Install

```
npm i @tableland/sqlparser
```

You should also be able to use the module directly in modern browsers supporting ES modules.

# Usage

```typescript
// Load module
import { init } from "@tableland/sqlparser";
// Initialize module (adds sqlparser object to global namespace)
await init();
// Parse sql statement
const { statements, type, tables } = await sqlparser.normalize(
  "select * FrOM fake_table_1 WHere something = 'nothing';"
);
console.log(statements);
console.log(type);
console.log(tables);
// ["select * from fake_table_1 where something='nothing'"]
// "read"
// ["fake_table_1"]
const tableName = await sqlparser.validateTableName("healthbot_5_1");
console.log(tableName);
// {
//   name: "healthbot_5_1",
//   chainId: 5,
//   tableId: 1,
//   prefix: "healthbot",
// }
const tableNames = await sqlparser.getUniqueTableNames(
  "select t1.id, t3.* from t1, t2 join t3 join (select * from t4);"
);
console.log(tableNames);
// ["t1", "t2", "t3", "t4"]
```

# Testing

Currently, this (experimental) module tests the native ES modules via `mocha`. There is also an `example.html` file in the `tests` folder that can be used for manual browser testing. The tests and example file provide good examples of general usage.

```
npm test
```

# Contributing

To get started clone this repo.

## Install tinygo

We require tinygo version `0.28.1` or greater

```
brew tap tinygo-org/tools
brew install tinygo
```

## Fetch wasm helpers

Use the corresponding tinygo version.
**Warning** this will overwrite any existing `wasm_exec.js` file, which has Tableland specific modifications.

```
wget https://raw.githubusercontent.com/tinygo-org/tinygo/v0.28.1/targets/wasm_exec.js
```

## Build with tinygo

```
tinygo build -gc=leaking -no-debug -o main.wasm -target wasm ./main.go
wasm-opt -O main.wasm -o main.wasm
```

or use the build scripts:

```
npm install
npm run build
```

This will produce `main.wasm`, and should be no more than 440K in size.
