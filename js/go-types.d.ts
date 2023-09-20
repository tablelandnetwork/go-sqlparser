// this file was automatically generated, DO NOT EDIT
// structs
// struct2ts:github.com/tablelandnetwork/sqlparser.Table
export interface Table {
  Name: string;
  IsTarget: boolean;
}

// struct2ts:github.com/tablelandnetwork/sqlparser.Column
export interface Column {
  Name: string;
  TableRef: Table | null;
}

// struct2ts:github.com/tablelandnetwork/sqlparser.ColumnDef
export interface ColumnDef {
  Column: Column | null;
  Type: string;
  Constraints: ColumnConstraint[] | null;
}

// struct2ts:github.com/tablelandnetwork/sqlparser.CreateTable
export interface CreateTable {
  Table: Table | null;
  ColumnsDef: ColumnDef[] | null;
  Constraints: TableConstraint[] | null;
  StrictMode: boolean;
}

// struct2ts:github.com/tablelandnetwork/sqlparser.ColumnConstraintPrimaryKey
export interface ColumnConstraintPrimaryKey {
  Name: string;
  Order: string;
  AutoIncrement: boolean;
}

// struct2ts:github.com/tablelandnetwork/sqlparser.ColumnConstraintNotNull
export interface ColumnConstraintNotNull {
  Name: string;
}

// struct2ts:github.com/tablelandnetwork/sqlparser.ColumnConstraintUnique
export interface ColumnConstraintUnique {
  Name: string;
}

// struct2ts:github.com/tablelandnetwork/sqlparser.ColumnConstraintCheck
export interface ColumnConstraintCheck {
  Name: string;
  Expr: any;
}

// struct2ts:github.com/tablelandnetwork/sqlparser.ColumnConstraintDefault
export interface ColumnConstraintDefault {
  Name: string;
  Expr: any;
  Parenthesis: boolean;
}

// struct2ts:github.com/tablelandnetwork/sqlparser.ColumnConstraintGenerated
export interface ColumnConstraintGenerated {
  Name: string;
  Expr: any;
  GeneratedAlways: boolean;
  IsStored: boolean;
}

// struct2ts:github.com/tablelandnetwork/sqlparser.IndexedColumn
export interface IndexedColumn {
  Column: Column | null;
  CollationName: string;
  Order: string;
}

// struct2ts:github.com/tablelandnetwork/sqlparser.TableConstraintPrimaryKey
export interface TableConstraintPrimaryKey {
  Name: string;
  Columns: IndexedColumn[] | null;
}

// struct2ts:github.com/tablelandnetwork/sqlparser.TableConstraintUnique
export interface TableConstraintUnique {
  Name: string;
  Columns: Column[] | null;
}

// struct2ts:github.com/tablelandnetwork/sqlparser.TableConstraintCheck
export interface TableConstraintCheck {
  Name: string;
  Expr: any;
}

export type ColumnConstraint =
  | ColumnConstraintPrimaryKey
  | ColumnConstraintNotNull
  | ColumnConstraintUnique
  | ColumnConstraintCheck
  | ColumnConstraintDefault
  | ColumnConstraintGenerated & { Type: string };
export type TableConstraint =
  | TableConstraintPrimaryKey
  | TableConstraintUnique
  | TableConstraintCheck;
