%{
package sqlparser

import (
  "bytes"
  "strings"
)

var keywordsNotAllowed = map[string]struct{}{
  // We don't allow non-deterministic keywords as identifiers. 
	"CURRENT_TIME":      {},
	"CURRENT_DATE":      {},
	"CURRENT_TIMESTAMP": {},

  // SQLite reserved keywords that are not part of Tableland spec.
  // We can't allow them as identifiers because it will throw an error in SQLite.
  //
  // SQLite has more reserved keywords (eg. CREATE, INSERT, ...). But those are part of the Tableland grammar,
  // that means that the parser already checks from them.
  //
  // These were identified by running the `TestReservedKeywords` test.
  "REFERENCES" : {},
  "ADD": {},
  "ALTER": {},
  "AUTOINCREMENT": {},
  "COMMIT": {},
  "DEFERRABLE": {},
  "DROP": {},
  "FOREIGN": {},
  "INDEX": {},
  "RETURNING": {},
  "TRANSACTION": {},
}

func isRowID(column Identifier) bool {
	if strings.EqualFold(string(column), "rowid") || strings.EqualFold(string(column), "_rowid_") || strings.EqualFold(string(column), "oid") {
		return true
	}

	return false
}

%}

%union{
  bool bool
  string string
  bytes []byte
  expr Expr 
  exprs Exprs
  column *Column
  table *Table
  convertType ConvertType
  when *When
  whens []*When
  selectColumn SelectColumn
  selectColumnList SelectColumnList
  readStmt ReadStatement
  baseSelect *Select
  where *Where
  limit *Limit
  orderBy OrderBy
  orderingTerm *OrderingTerm
  nulls NullsType
  tableExpr TableExpr
  joinTableExpr *JoinTableExpr
  columnList ColumnList
  indexedColumnList IndexedColumnList
  indexedColumn *IndexedColumn
  subquery *Subquery
  colTuple ColTuple
  statement Statement
  identifier Identifier
  createTableStmt *CreateTable
  columnDefList []*ColumnDef
  columnDef *ColumnDef
  columnConstraint ColumnConstraint
  columnConstraints []ColumnConstraint
  value *Value
  tableConstraint TableConstraint
  tableConstraints []TableConstraint
  insertStmt *Insert
  insertRows []Exprs
  deleteStmt *Delete
  updateStmt *Update
  updateExpression *UpdateExpr
  updateList []*UpdateExpr
  grant *Grant
  revoke *Revoke
  strings []string
  privileges Privileges
  stmts []Statement
  upsertClause Upsert
  onConflictClauseList []*OnConflictClause
  onConflictClause *OnConflictClause
  onConflictTarget *OnConflictTarget
  collateOpt Identifier
  joinOperator *JoinOperator
}

%token <bytes> IDENTIFIER STRING INTEGRAL HEXNUM FLOAT BLOBVAL
%token ERROR 
%token <empty> TRUE FALSE NULL AND
%token <empty> '(' ',' ')' '.' ';'
%token <empty> NONE INTEGER TEXT CAST AS
%token <empty> CASE WHEN THEN ELSE END
%token <empty> SELECT FROM WHERE GROUP BY HAVING LIMIT OFFSET ORDER ASC DESC NULLS FIRST LAST DISTINCT ALL EXISTS FILTER UNION EXCEPT INTERSECT
%token <empty> CREATE TABLE INT BLOB PRIMARY KEY UNIQUE CHECK DEFAULT GENERATED ALWAYS STORED VIRTUAL CONSTRAINT
%token <empty> INSERT INTO VALUES DELETE UPDATE SET CONFLICT DO NOTHING
%token <empty> GRANT TO REVOKE
%token <empty> BLOCK_NUM TXN_HASH

%left <empty> RIGHT FULL INNER LEFT NATURAL OUTER CROSS JOIN
%left <empty> ON USING

%left <empty> OR
%left <empty> ANDOP
%right <empty> NOT
%left <empty> IS ISNOT MATCH GLOB REGEXP LIKE BETWEEN IN ISNULL NOTNULL NE '=' 
%left <empty> '<' '>' LE GE INEQUALITY
%right <empty> ESCAPE 
%left '&' '|' LSHIFT RSHIFT
%left <empty> '+' '-'
%left <empty> '*' '/' '%'
%left <empty> CONCAT JSON_EXTRACT_OP JSON_UNQUOTE_EXTRACT_OP
%left <empty> COLLATE
%right <empty> '~' UNARY

%type <statement> multi_stmt single_stmt
%type <readStmt> select_stmt
%type <baseSelect> base_select
%type <createTableStmt> create_table_stmt
%type <expr> expr literal_value function_call_keyword function_call_generic expr_opt else_expr_opt exists_subquery signed_number function_call_custom
%type <exprs> expr_list expr_list_opt group_by_opt
%type <string> cmp_op cmp_inequality_op like_op between_op asc_desc_opt distinct_opt type_name primary_key_order privilege compound_op
%type <column> column_name 
%type <identifier> as_column_opt col_alias as_table_opt table_alias constraint_name identifier collate_opt
%type <selectColumn> select_column
%type <selectColumnList> select_column_list
%type <table> table_name
%type <where> where_opt having_opt filter_opt
%type <convertType> convert_type
%type <when> when 
%type <whens> when_expr_list
%type <limit> limit_opt
%type <orderBy> order_by_opt order_list
%type <orderingTerm> ordering_term
%type <nulls> nulls
%type <tableExpr> table_expr from_clause
%type <joinTableExpr> join_clause join_constraint
%type <columnList> column_name_list column_name_list_opt
%type <indexedColumnList> indexed_column_list
%type <indexedColumn> indexed_column
%type <subquery> subquery
%type <colTuple> col_tuple
%type <bool> distinct_function_opt is_stored natural_opt outer_opt
%type <columnDefList> column_def_list
%type <columnDef> column_def
%type <columnConstraint> column_constraint
%type <columnConstraints> column_constraints column_constraints_opt
%type <value> numeric_literal
%type <tableConstraint> table_constraint
%type <tableConstraints> table_constraint_list table_constraint_list_opt
%type <insertStmt> insert_stmt
%type <insertRows> insert_rows
%type <deleteStmt> delete_stmt
%type <updateStmt> update_stmt
%type <updateExpression> update_expression
%type <updateList> update_list common_update_list paren_update_list
%type <grant> grant_stmt
%type <revoke> revoke_stmt
%type <strings> roles
%type <privileges> privileges
%type <stmts> stmts multi_stmts
%type <upsertClause> upsert_clause_opt 
%type <onConflictClauseList> on_conflict_clause_list
%type <onConflictClause> on_conflict_clause
%type <onConflictTarget> conflict_target_opt
%type <joinOperator> join_op

%%
start: 
  stmts { yylex.(*Lexer).ast = &AST{Statements: $1} }
;

stmts: 
  single_stmt semicolon_opt
  {
    $$ = []Statement{$1}
  }
| multi_stmts semicolon_opt
  { 
    $$ = $1
  }
;

single_stmt:
  select_stmt
  {
    $$ = $1
  }
| create_table_stmt
  {
    $$ = $1
  }
;

multi_stmts:
  multi_stmt 
  {
    $$ = []Statement{$1}
  }
| multi_stmts ';' multi_stmt
  {
    $$ = append($1, $3)
  }
;

multi_stmt:
  insert_stmt
  {
    yylex.(*Lexer).statementIdx++ 
    $$ = $1
  }
| delete_stmt
  {
    yylex.(*Lexer).statementIdx++ 
    $$ = $1 
  }
| update_stmt
  {
    yylex.(*Lexer).statementIdx++ 
    $$ = $1 
  }
| grant_stmt
  {
    yylex.(*Lexer).statementIdx++ 
    $$ = $1
  }
| revoke_stmt
  {
    yylex.(*Lexer).statementIdx++ 
    $$ = $1
  }
;

semicolon_opt:
  {}
| ';' 
  {}
;

select_stmt:
  base_select order_by_opt limit_opt
  {
    $1.OrderBy = $2
    $1.Limit = $3
    $$ = $1
  }
| base_select compound_op base_select order_by_opt limit_opt
  {
    $$ = &CompoundSelect{Type: $2, Left: $1, Right: $3, OrderBy: $4, Limit: $5}
  }
;

compound_op:
  UNION 
  {
    $$ = CompoundUnionStr
  }
| UNION ALL 
  {
    $$ = CompoundUnionAllStr
  }
| EXCEPT 
  {
    $$ = CompoundExceptStr
  }
| INTERSECT 
  {
    $$ = CompoundIntersectStr
  }
;

base_select:
  SELECT distinct_opt select_column_list from_clause where_opt group_by_opt having_opt
  {
    $$ = &Select{
            Distinct: $2,
            SelectColumnList: $3, 
            From: $4, 
            Where: $5, 
            GroupBy: GroupBy($6), 
            Having: $7,
         }
  }
;

distinct_opt:
  {
    $$ = ""
  }
| DISTINCT 
  {
    $$ = DistinctStr
  }
| ALL
  {
    $$ = AllStr
  }
;

select_column_list:
  select_column
  {
    $$ = SelectColumnList{$1}
  }
| select_column_list ',' select_column
  {
    $$ = append($1, $3)
  }

select_column:
  '*'
  {
    $$ = &StarSelectColumn{}
  }
| expr as_column_opt
  {
    $$ = &AliasedSelectColumn{Expr: $1, As: $2}
  }
| table_name '.' '*'
  {
    $$ = &StarSelectColumn{TableRef: $1}
  }

as_column_opt:
  {
    $$ = Identifier("")
  }
| col_alias
  {
    $$ = $1
  }
| AS col_alias
  {
    $$ = $2
  }

col_alias:
  identifier
  {
    $$ = $1
  }
| STRING
  {
    $$ = Identifier(string($1[1:len($1)-1]))
  }
;

from_clause:
  FROM table_expr
  {
    $$ = $2
  }
| FROM join_clause
  {
    $$ = $2
  }
;

table_expr:
  table_name as_table_opt
  {
    $1.IsTarget = true
    $$ = &AliasedTableExpr{Expr: $1, As: $2}
  }
| '(' select_stmt ')' as_table_opt
  {
    $$ = &AliasedTableExpr{Expr: &Subquery{Select: $2}, As: $4}
  }
| '(' table_expr ')'
  {
    $$ = &ParenTableExpr{TableExpr: $2}
  }
|  '(' join_clause ')'
  {
    $$ = $2
  }
;

as_table_opt:
  {
    $$ = Identifier("")
  }
| table_alias
  {
    $$ = $1
  }
| AS table_alias
  {
    $$ = $2
  }

table_alias:
  identifier
  {
    $$ = $1
  }
| STRING
  {
    $$ = Identifier(string($1[1:len($1)-1]))
  }
;

join_clause:
  table_expr join_op table_expr join_constraint
  {
    if $4 == nil {
      $$ = &JoinTableExpr{LeftExpr: $1, JoinOperator: $2, RightExpr: $3}
    } else {
      if $2.Natural {
        yylex.(*Lexer).AddError(&ErrNaturalJoinWithOnOrUsingClause{})
      }

      $4.LeftExpr = $1
      $4.JoinOperator = $2
      $4.RightExpr = $3
      $$ = $4
    }
  }
| join_clause join_op table_expr join_constraint
  {
    if $4 == nil {
      $$ = &JoinTableExpr{LeftExpr: $1, JoinOperator: $2, RightExpr: $3}
    } else {
      if $2.Natural {
        yylex.(*Lexer).AddError(&ErrNaturalJoinWithOnOrUsingClause{})
      }

      $4.LeftExpr = $1
      $4.JoinOperator = $2
      $4.RightExpr = $3
      $$ = $4
    }
  }
;

join_op:
  JOIN
  {
    $$ = &JoinOperator{Op: JoinStr}
  }
| ','
  {
    $$ =  &JoinOperator{Op: JoinStr}
  }
| CROSS JOIN
  {
    $$ =  &JoinOperator{Op: JoinStr}
  }
| natural_opt LEFT outer_opt JOIN
  {
    $$ =  &JoinOperator{Op: LeftJoinStr, Natural: $1, Outer: $3}
  }
| natural_opt RIGHT outer_opt JOIN
  {
    $$ =  &JoinOperator{Op: RightJoinStr, Natural: $1, Outer: $3}
  }
| natural_opt FULL outer_opt JOIN
  {
    $$ =  &JoinOperator{Op: FullJoinStr, Natural: $1, Outer: $3}
  }
| natural_opt INNER JOIN
  {
    $$ =  &JoinOperator{Op: InnerJoinStr, Natural: $1}
  }
;

natural_opt:
  {
    $$ = false
  }
| NATURAL
  {
    $$ = true
  }
;

outer_opt:
  {
    $$ = false
  }
| OUTER
  {
    $$ = true
  }
;

join_constraint:
  %prec JOIN
  {
    $$ = nil
  }
| ON expr
  {
    $$ = &JoinTableExpr{On: $2}
  }
| USING '(' column_name_list ')'
  {
    $$ = &JoinTableExpr{Using: $3}
  }
;

where_opt:
  {
    $$ = nil
  }
| WHERE expr
{
   $$ = NewWhere(WhereStr, $2)
}
;

group_by_opt:
  {
    $$ = nil
  }
| GROUP BY expr_list
  {
    $$ = $3
  }
;

having_opt:
  {
    $$ = nil
  }
| HAVING expr
  {
    $$ = NewWhere(HavingStr, $2)
  }
;

order_by_opt:
  {
    $$ = nil
  }
| ORDER BY order_list
  {
    $$ = $3
  }
;

order_list:
  ordering_term
  {
    $$ = OrderBy{$1}
  }
| order_list ',' ordering_term
  {
    $$ = append($1, $3)
  }
;

ordering_term:
  expr asc_desc_opt nulls
  {
    $$ = &OrderingTerm{Expr: $1, Direction: $2, Nulls: $3}
  }
;

asc_desc_opt:
  {
    $$ = AscStr
  }
| ASC
  {
    $$ = AscStr
  }
| DESC
  {
    $$ = DescStr
  }
;

nulls:
  {
    $$ = NullsNil
  }
| NULLS FIRST
  {
    $$ = NullsFirst
  }
| NULLS LAST
  {
    $$ = NullsLast
  }
;

limit_opt:
  {
    $$ = nil
  }
| LIMIT expr
  {
    $$ = &Limit{Limit: $2}
  }
| LIMIT expr ',' expr
  {
    $$ = &Limit{Offset: $2, Limit: $4}
  }
| LIMIT expr OFFSET expr
  {
    $$ = &Limit{Offset: $4, Limit: $2}
  }
;

table_name:
  identifier
  { 
     $$ = &Table{Name: $1}
  }
;

expr: 
  literal_value { $$ = $1 }
| column_name { $$ = $1 }
| table_name '.' column_name
  {
      $3.TableRef = $1
      $$ = $3
  }
| expr '+' expr
  {  
    $$ = &BinaryExpr{Left: $1, Operator: PlusStr, Right: $3} 
  }
| expr '-' expr
  {  
    $$ = &BinaryExpr{Left: $1, Operator: MinusStr, Right: $3} 
  }
| expr '*' expr
  {  
    $$ = &BinaryExpr{Left: $1, Operator: MultStr, Right: $3} 
  }
| expr '/' expr
  {  
    $$ = &BinaryExpr{Left: $1, Operator: DivStr, Right: $3} 
  }
| expr '%' expr
  {  
    $$ = &BinaryExpr{Left: $1, Operator: ModStr, Right: $3} 
  }
| expr '&' expr
  {  
    $$ = &BinaryExpr{Left: $1, Operator: BitAndStr, Right: $3} 
  }
| expr '|' expr
  {  
    $$ = &BinaryExpr{Left: $1, Operator: BitOrStr, Right: $3} 
  }
| expr LSHIFT expr
  {  
    $$ = &BinaryExpr{Left: $1, Operator: ShiftLeftStr, Right: $3} 
  }
| expr RSHIFT expr
  {  
    $$ = &BinaryExpr{Left: $1, Operator: ShiftRightStr, Right: $3} 
  }
| expr CONCAT expr
  {  
    $$ = &BinaryExpr{Left: $1, Operator: ConcatStr, Right: $3} 
  }
| expr JSON_EXTRACT_OP expr
  {  
    $$ = &BinaryExpr{Left: $1, Operator: JSONExtractOp, Right: $3} 
  }
| expr JSON_UNQUOTE_EXTRACT_OP expr
  {  
    $$ = &BinaryExpr{Left: $1, Operator: JSONUnquoteExtractOp, Right: $3} 
  }
| expr cmp_op expr %prec IS
  {  
    $$ = &CmpExpr{Left: $1, Operator: $2, Right: $3} 
  }
| expr cmp_inequality_op expr %prec INEQUALITY
  {  
    $$ = &CmpExpr{Left: $1, Operator: $2, Right: $3} 
  }
| expr like_op expr %prec LIKE
  {
    $$ = &CmpExpr{Left: $1, Operator: $2, Right: $3}
  }
| expr like_op expr ESCAPE expr %prec LIKE
  {
    $$ = &CmpExpr{Left: $1, Operator: $2, Right: $3, Escape: $5}
  }
| '-'  expr %prec UNARY
  {
    if value, ok := $2.(*Value); ok && value.Type == IntValue {
      $$ = &Value{Type: IntValue, Value: append([]byte("-"), value.Value...)}
    } else {
      $$ = &UnaryExpr{Operator: UMinusStr, Expr: $2}
    }
  }
| '+'  expr %prec UNARY
  {
    $$ = &UnaryExpr{Operator: UPlusStr, Expr: $2}
  }
| '~'  expr %prec UNARY
  {
    $$ = &UnaryExpr{Operator: TildaStr, Expr: $2}
  }
| expr ANDOP expr 
  {  
    $$ = &AndExpr{Left: $1, Right: $3}
  }
| expr OR expr 
  {  
    $$ = &OrExpr{Left: $1, Right: $3}
  }
| expr IS expr
  {  
    $$ = &IsExpr{Left: $1, Right: $3}
  }
| expr IS ISNOT expr
  {  
    $$ = &IsExpr{Left: $1, Right: &NotExpr{Expr: $4}}
  }
| expr ISNULL
  {  
    $$ = &IsNullExpr{Expr : $1}
  }
| expr NOTNULL
  {  
    $$ = &NotNullExpr{Expr : $1}
  }
| expr NOT NULL
  {  
    $$ = &NotNullExpr{Expr : $1}
  }
| expr between_op expr AND expr %prec BETWEEN
  {  
    $$ = &BetweenExpr{Left : $1, Operator: $2, From: $3 , To: $5}
  }
| CASE expr_opt when_expr_list else_expr_opt END
  {
    $$ = &CaseExpr{Expr: $2, Whens: $3, Else: $4}
  }
| expr COLLATE identifier
  {  
    $$ = &CollateExpr{Expr : $1, CollationName: $3}
  }
| '(' expr ')'
  {
    $$ = &ParenExpr{Expr: $2}
  }
| expr IN col_tuple
  {
    $$ = &CmpExpr{Left: $1, Operator: InStr, Right: $3}
  }
| expr NOT IN col_tuple
  {
    $$ = &CmpExpr{Left: $1, Operator: NotInStr, Right: $4}
  }
| subquery 
  {
    $$ = $1 
  }
| exists_subquery
  {
    $$ = $1
  }
| CAST '(' expr AS convert_type ')'
  {
    $$ = &ConvertExpr{Expr: $3, Type: $5}
  }
| function_call_keyword
| function_call_custom
| function_call_generic
;

literal_value:
  numeric_literal
  {
    $$ = $1
  }
| STRING
  {
    str := $1[1:len($1)-1]
    if len(str) > MaxTextLength {
      yylex.(*Lexer).AddError(&ErrTextTooLong{Length: len(str), MaxAllowed: MaxTextLength})
    }
    $$ = &Value{Type: StrValue, Value: str}
  }
| BLOBVAL
  {
    if len($1) > MaxBlobLength {
      yylex.(*Lexer).AddError(&ErrBlobTooBig{Length: len($1), MaxAllowed: MaxBlobLength})
    }
    $$ = &Value{Type: BlobValue, Value: $1}
  }
| TRUE
  {
    $$ = BoolValue(true)
  }
| FALSE
  {
    $$ = BoolValue(false)
  }
| NULL
  {
    $$ = &NullValue{}
  }
;

column_name:
  identifier
  { 
    $$ = &Column{Name : Identifier(string($1))} 
  }
;

column_name_list:
  column_name
  {
    $$ = ColumnList{$1}
  }
| column_name_list ',' column_name
  {
    $$ = append($1, $3)
  }
;

cmp_op:
  '='
  {
    $$ = EqualStr
  }
| NE
  {
    $$ = NotEqualStr
  }
|  REGEXP
  {
    $$ = RegexpStr
  }
| NOT REGEXP
  {
    $$ = NotRegexpStr
  }
| GLOB
  {
    $$ = GlobStr
  }
| NOT GLOB
  {
    $$ = NotGlobStr
  }
| MATCH
  {
    $$ = MatchStr
  }
| NOT MATCH
  {
    $$ = NotMatchStr
  }
;

cmp_inequality_op:
 '<'
  {
    $$ = LessThanStr
  }
| '>'
  {
    $$ = GreaterThanStr
  }
| LE
  {
    $$ = LessEqualStr
  }
| GE
  {
    $$ = GreaterEqualStr
  }
;

like_op:
    LIKE
    {
        $$ = LikeStr
    }
|   NOT LIKE
    {
        $$ = NotLikeStr
    }
;

between_op:
    BETWEEN
    {
        $$ = BetweenStr
    }
|   NOT BETWEEN
    {
        $$ = NotBetweenStr
    }
;

convert_type:
  NONE { $$ = NoneStr}
| TEXT { $$ = TextStr}
| INTEGER { $$ = IntegerStr}
;

col_tuple:
  '(' ')'
  {
    $$ = Exprs{}
  }
| subquery
  {
    $$ = $1
  }
| '(' expr_list ')'
  {
    $$ = $2
  }
;

subquery:
  '(' select_stmt ')'
  {
    $$ = &Subquery{Select: $2}
  }
;

exists_subquery:
  EXISTS subquery
  {
    $$ = &ExistsExpr{Subquery: $2}
  }
| NOT EXISTS subquery
  {
    $$ = &NotExpr{Expr: &ExistsExpr{Subquery: $3}}
  }
;

function_call_keyword:
  GLOB '(' expr ',' expr ')'
  {
    $$ = &FuncExpr{Name: Identifier("glob"), Args: Exprs{$3, $5}} 
  }
| LIKE '(' expr ',' expr ')'
  {
    $$ = &FuncExpr{Name: Identifier("like"), Args: Exprs{$3, $5}} 
  }
| LIKE '(' expr ',' expr ',' expr ')'
  {
    $$ = &FuncExpr{Name: Identifier("like"), Args: Exprs{$3, $5, $7}} 
  }
;

function_call_generic:
  identifier '(' distinct_function_opt expr_list_opt ')' filter_opt
  {
    lowered := strings.ToLower(string($1))
    if _, ok := AllowedFunctions[lowered]; !ok {
      yylex.(*Lexer).AddError(&ErrNoSuchFunction{FunctionName: string($1)})
    }
    $$ = &FuncExpr{Name: Identifier(lowered), Distinct: $3, Args: $4, Filter: $6}
  }
| identifier '(' '*' ')' filter_opt
  {
    lowered := strings.ToLower(string($1))
    if _, ok := AllowedFunctions[lowered]; !ok {
      yylex.(*Lexer).AddError(&ErrNoSuchFunction{FunctionName: string($1)})
    }
    $$ = &FuncExpr{Name: Identifier(lowered), Distinct: false, Args: nil, Filter: $5}
  }
;

function_call_custom:
  TXN_HASH '(' ')'
  {
    $$ = &CustomFuncExpr{Name: Identifier("txn_hash"), Args: Exprs{}}
  }
| BLOCK_NUM '(' ')'
  {
    $$ = &CustomFuncExpr{Name: Identifier("block_num"), Args: Exprs{}}
  }
| BLOCK_NUM '(' INTEGRAL ')'
  {
    $$ = &CustomFuncExpr{Name: Identifier("block_num"), Args: Exprs{&Value{Type: IntValue, Value: $3}}}
  }
;


distinct_function_opt:
  {
    $$ = false
  }
| DISTINCT 
  {
    $$ = true
  }
;

expr_list:
  expr
  {
    $$ = Exprs{$1}
  }
| expr_list ',' expr
  {
    $$ = append($1, $3)
  }
;

expr_list_opt:
  {
    $$ = nil
  }
| expr_list
  {
    $$ = $1
  }
;

filter_opt:
  {
    $$ = nil
  }
| FILTER '(' WHERE expr ')'
  {
    $$ = &Where{Type: WhereStr, Expr: $4}
  }
;

expr_opt:
  {
    $$ = nil
  }
| expr
  {
    $$ = $1
  }
;

when:
  WHEN expr THEN expr
  {
    $$ = &When{Condition: $2, Value: $4}
  }
;

when_expr_list:
  when
  {
       $$ = []*When{$1}
  }
| when_expr_list when
  {
    $$ = append($1, $2)
  }

else_expr_opt:
  {
    $$ = nil
  }
| ELSE expr 
  {
    $$ = $2
  }
;

create_table_stmt:
  CREATE TABLE table_name '(' column_def_list table_constraint_list_opt ')'
  {
    if len($5) > MaxAllowedColumns {
      yylex.(*Lexer).AddError(&ErrTooManyColumns{ColumnCount: len($5), MaxAllowed: MaxAllowedColumns})
    }

    // We have to replace a primary key table constraint with an equivalent column constraint primary key,
    // so we can add the autoincrement flag, as part of the rules of the Tableland Protocol.
    // 
    // That happens because a primary key table constraint that references a single INTEGER column
    // would be an alias to rowid. For cases where a column becomes an alias to rowid we want to force the AUTOINCREMENT.
    // 
    // The exception to the above rule is when a table constraint primary key has order DESC. In that case, we replace with an 
    // equivalent column constrain without forcing AUTOINCREMENT and avoiding being interpreted as an alias.
    for index, tableConstraint := range $6 {
      if tableConstraintPK, ok := tableConstraint.(*TableConstraintPrimaryKey); ok && len(tableConstraintPK.Columns) == 1 {
        for _, columnDef := range $5 {
          if columnDef.Type == TypeIntegerStr && !columnDef.HasPrimaryKey(){
            if tableConstraintPK != nil && columnDef.Column.Name == tableConstraintPK.Columns[0].Column.Name {
              forceAutoincrement := tableConstraintPK.Columns[0].Order != PrimaryKeyOrderDesc
              columnDef.Constraints = append(columnDef.Constraints, &ColumnConstraintPrimaryKey{Name: tableConstraintPK.Name, AutoIncrement: forceAutoincrement, Order: tableConstraintPK.Columns[0].Order})
              $6 = append($6[:index], $6[index+1:]...)
            }
          }
        }
      }
    }
    $3.IsTarget = true
    $$ = &CreateTable{Table: $3, ColumnsDef: $5, Constraints: $6}
  }
;

column_def_list:
  column_def
  {
    $$ = []*ColumnDef{$1}
  }
| column_def_list ',' column_def
  {
    $$ = append($1, $3)
  }
;

column_def:
  column_name type_name column_constraints_opt
  {
    if isRowID($1.Name) {
      yylex.(*Lexer).AddError(&ErrRowIDNotAllowed{})
    }

    if $2 == TypeIntegerStr {
      for _, constraint := range $3 {
        if primaryKey, ok := constraint.(*ColumnConstraintPrimaryKey); ok {
          if primaryKey.Order != PrimaryKeyOrderDesc {
            primaryKey.AutoIncrement = true
          }
        }
      }
    }
    $$ = &ColumnDef{Column: $1, Type: $2, Constraints: $3}
  }
;

type_name:
  INT { $$ = TypeIntStr}
| INTEGER { $$ = TypeIntegerStr}
| TEXT { $$ = TypeTextStr}
| BLOB { $$ = TypeBlobStr}
;

column_constraints_opt:
  {
    $$ = []ColumnConstraint{}
  }
| column_constraints
  {
    $$ = $1
  }
;

column_constraints:
  column_constraint
  {
    if _, ok := $1.(*ColumnConstraintPrimaryKey); ok {
      if yylex.(*Lexer).createStmtHasPrimaryKey {
        yylex.(*Lexer).AddError(&ErrMultiplePrimaryKey{})
      } else {
        yylex.(*Lexer).createStmtHasPrimaryKey = true
      }
    }
    $$ = []ColumnConstraint{$1}
  }
| column_constraints column_constraint
  {
    if _, ok := $2.(*ColumnConstraintPrimaryKey); ok && yylex.(*Lexer).createStmtHasPrimaryKey {
      yylex.(*Lexer).AddError(&ErrMultiplePrimaryKey{})
    }
    $$ = append($1, $2)
  }
;

column_constraint:
  constraint_name PRIMARY KEY primary_key_order
  {
    $$ = &ColumnConstraintPrimaryKey{Name: $1, Order: $4}
  }
| constraint_name NOT NULL
  {
    $$ = &ColumnConstraintNotNull{Name: $1}
  }
| constraint_name UNIQUE
  {
    $$ = &ColumnConstraintUnique{Name: $1}
  }
| constraint_name CHECK '(' expr ')'
  {
    $$ = &ColumnConstraintCheck{Name: $1, Expr: $4}
  }
| constraint_name DEFAULT '(' expr ')'
  {
    $$ = &ColumnConstraintDefault{Name: $1, Expr: $4, Parenthesis: true}
  }
| constraint_name DEFAULT literal_value
  {
    $$ = &ColumnConstraintDefault{Name: $1, Expr: $3}
  }
| constraint_name DEFAULT signed_number
  {
    $$ = &ColumnConstraintDefault{Name: $1, Expr: $3}
  }
| constraint_name GENERATED ALWAYS AS '(' expr ')' is_stored
  {
    $$ = &ColumnConstraintGenerated{Name: $1, Expr: $6, GeneratedAlways: true, IsStored: $8}
  }
| constraint_name AS '(' expr ')' is_stored
  {
    $$ = &ColumnConstraintGenerated{Name: $1, Expr: $4, GeneratedAlways: false, IsStored: $6}
  }
;

constraint_name:
  {
    $$ = Identifier("")
  }
| CONSTRAINT identifier 
  {
    $$ = $2
  }
;

primary_key_order:
  {
    $$ = PrimaryKeyOrderEmpty
  }
|  ASC
  {
    $$ = PrimaryKeyOrderAsc
  }
| DESC
  {
    $$ = PrimaryKeyOrderDesc
  }
;

signed_number:
  '+' numeric_literal 
  {
    $$ = $2
  }
| '-' numeric_literal %prec UNARY
  {
    $2.Value = append([]byte("-"), $2.Value...)
    $$ = $2
  }
;

numeric_literal:
  INTEGRAL
  {
    $$ = &Value{Type: IntValue, Value: $1}
  }
| FLOAT
  {
    yylex.(*Lexer).AddError(&ErrNumericLiteralFloat{Value: $1})
    $$ = &Value{Type: FloatValue, Value: $1}
  }
| HEXNUM
  {
    $$ = &Value{Type: HexNumValue, Value: $1}
  }
;

is_stored:
  {
    $$ = false
  }
| STORED
  {
    $$ = true
  }
| VIRTUAL
  {
    $$ = false
  }
;

table_constraint_list_opt:
  {
    $$ = []TableConstraint{}
  }
| table_constraint_list
  {
    $$ = $1
  }
;

table_constraint_list:
  ',' table_constraint
  {
    if _, ok := $2.(*TableConstraintPrimaryKey); ok {
      if yylex.(*Lexer).createStmtHasPrimaryKey {
        yylex.(*Lexer).AddError(&ErrMultiplePrimaryKey{})
      } else {
        yylex.(*Lexer).createStmtHasPrimaryKey = true
      }
    }
    $$ = []TableConstraint{$2}
  }
| table_constraint_list ','  table_constraint
  {
    if _, ok := $3.(*TableConstraintPrimaryKey); ok && yylex.(*Lexer).createStmtHasPrimaryKey {
      yylex.(*Lexer).AddError(&ErrMultiplePrimaryKey{})
    }
    $$ = append($1, $3)
  }
;

table_constraint:
  constraint_name PRIMARY KEY '(' indexed_column_list ')'
  {
    $$ = &TableConstraintPrimaryKey{Name: $1, Columns: $5}
  }
| constraint_name UNIQUE '(' column_name_list ')'
  {
    $$ = &TableConstraintUnique{Name: $1, Columns: $4}
  }
| constraint_name CHECK '(' expr ')'
  {
    $$ = &TableConstraintCheck{Name: $1, Expr: $4}
  }
;

indexed_column_list:
  indexed_column
  {
    $$ = IndexedColumnList{$1}
  }
| indexed_column_list ',' indexed_column
  {
    $$ = append($1, $3)
  }
;

indexed_column:
  column_name collate_opt primary_key_order
  {
    $$ = &IndexedColumn{Column : $1, CollationName: $2, Order: $3}
  }
;

collate_opt:
  {
    $$ = Identifier("")
  }
| COLLATE identifier
  {
    $$ = Identifier(string($2))
  }
;

insert_stmt:
  INSERT INTO table_name column_name_list_opt VALUES insert_rows upsert_clause_opt
  {
    for i := 0; i < len($4); i++ {
      if isRowID($4[i].Name) {
        yylex.(*Lexer).AddError(&ErrRowIDNotAllowed{})
      }
    }

    for _, row := range $6 {
      for _, expr := range row {
				if containsSubquery(expr) {
          yylex.(*Lexer).AddError(&ErrStatementContainsSubquery{StatementKind: "insert"})
				}
			}
    }
    $3.IsTarget = true
    $$ = &Insert{Table: $3, Columns: $4, Rows: $6, Upsert: $7}
  }
| INSERT INTO table_name DEFAULT VALUES
  {
    $3.IsTarget = true
    $$ = &Insert{Table: $3, Columns: ColumnList{}, Rows: []Exprs{}, DefaultValues: true}
  }
| INSERT INTO table_name column_name_list_opt select_stmt upsert_clause_opt
  {
    $3.IsTarget = true

    err := $5.walkSubtree(func(node Node) (bool, error) {
      if _, ok := node.(*Subquery); ok {
        return true, &ErrStatementContainsSubquery{StatementKind: "insert+select"}
      }

      if _, ok := node.(*JoinTableExpr); ok {
        return true, &ErrContainsJoinTableExpr{}
      }

      return false, nil
    })
    if err != nil {
       yylex.(*Lexer).AddError(err)
    }

    if sel, ok := $5.(*Select); ok {
      if sel.Having != nil || sel.GroupBy != nil {
        yylex.(*Lexer).AddError(&ErrHavingOrGroupByIsNotAllowed{})
      }

      if sel.OrderBy == nil {
        sel.OrderBy = OrderBy{&OrderingTerm{Expr: &Column{Name: Identifier("rowid")}, Direction: AscStr, Nulls: NullsNil}}
      } else {
        sel.OrderBy = append(sel.OrderBy, &OrderingTerm{Expr: &Column{Name: Identifier("rowid")}, Direction: AscStr, Nulls: NullsNil})
      }

      $$ = &Insert{Table: $3, Columns: ColumnList{}, Rows: []Exprs{}, Select: sel, Upsert: $6}
    } else {
      yylex.(*Lexer).AddError(&ErrCompoudSelectNotAllowed{})
      $$ = &Insert{Table: $3, Columns: ColumnList{}, Rows: []Exprs{},  Upsert: $6}
    }
  }
;

column_name_list_opt:
  {
    $$ = ColumnList{}
  }
| '(' column_name_list ')'
  {
    $$ = $2
  }
;

insert_rows:
  '(' expr_list ')'
  {
    $$ = []Exprs{$2}
  }
| insert_rows ',' '(' expr_list ')'
  {
    $$ = append($1, $4)
  }
;

upsert_clause_opt:
  {
    $$ = nil
  }
| on_conflict_clause_list
  {
    allConflictClausesExceptLast := $1[0:len($1) - 1];
    for _, clause := range allConflictClausesExceptLast {
      if clause.Target == nil {
        yylex.(*Lexer).AddError(&ErrUpsertMissingTarget{})
      }
    }
    $$ = $1
  }
;

on_conflict_clause_list:
  on_conflict_clause
  {
    $$ = []*OnConflictClause{$1}
  }
| on_conflict_clause_list on_conflict_clause
  {
    $$ = append($1, $2)
  }
;

on_conflict_clause:
  ON CONFLICT conflict_target_opt DO NOTHING
  {
    $$ = &OnConflictClause{
      Target: $3,
    }
  }
| ON CONFLICT conflict_target_opt DO UPDATE SET update_list where_opt
  {
    if $8 != nil && containsSubquery($8) {
      yylex.(*Lexer).AddError(&ErrStatementContainsSubquery{StatementKind: "where"})
    }

    $$ = &OnConflictClause{
      Target: $3,
      DoUpdate: &OnConflictUpdate{
        Exprs: $7, 
        Where: $8,
      },
    }
  }
;

conflict_target_opt:
  {
    $$ = nil
  }
| '(' column_name_list ')' where_opt
  {
    if $4 != nil && containsSubquery($4) {
      yylex.(*Lexer).AddError(&ErrStatementContainsSubquery{StatementKind: "where"})
    }

    $$ = &OnConflictTarget{
      Columns : $2,
      Where: $4,
    }
  }
;



delete_stmt:
  DELETE FROM table_name where_opt
  {
    if $4 != nil && containsSubquery($4) {
      yylex.(*Lexer).AddError(&ErrStatementContainsSubquery{StatementKind: "delete"})
    }
    $3.IsTarget = true
    $$ = &Delete{Table: $3, Where: $4}
  }
;

update_stmt:
  UPDATE table_name SET update_list where_opt
  {
    if $5 != nil && containsSubquery($5) {
      yylex.(*Lexer).AddError(&ErrStatementContainsSubquery{StatementKind: "where"})
    }
    $2.IsTarget = true
    $$ = &Update{Table: $2, Exprs: $4, Where: $5}
  }
;

update_list:
  common_update_list
  {
    $$ = $1
  }
| paren_update_list
  {
    $$ = $1
  }
;

common_update_list:
  update_expression
  {
    if containsSubquery($1.Expr) {
      yylex.(*Lexer).AddError(&ErrStatementContainsSubquery{StatementKind: "update"})
    }
    $$ = []*UpdateExpr{$1}
  }
| common_update_list ',' update_expression
  {
    $$ = append($1, $3)
  }
;

paren_update_list:
  '(' column_name_list ')' '=' '(' expr_list ')'
  {
    if len($2) != len($6) {
      yylex.(*Lexer).AddError(&ErrUpdateColumnsAndValuesDiffer{ColumnsCount: len($2), ValuesCount: len($6)})
      $$ = []*UpdateExpr{}
    } else {
      exprs := make([]*UpdateExpr, len($2))
      for i := 0; i < len($2); i++ {
        if isRowID($2[i].Name) {
          yylex.(*Lexer).AddError(&ErrRowIDNotAllowed{})
        }
        exprs[i] = &UpdateExpr{Column: $2[i], Expr: $6[i]}
      }
      $$ = exprs
    }
  }
;

update_expression:
  column_name '=' expr
  {
    if isRowID($1.Name) {
      yylex.(*Lexer).AddError(&ErrRowIDNotAllowed{})
    }
    $$ = &UpdateExpr{Column: $1, Expr: $3}
  }
;

grant_stmt:
  GRANT privileges ON table_name TO roles
  {
    $4.IsTarget = true
    $$ = &Grant{Table: $4, Privileges: $2, Roles: $6}
  }
;

revoke_stmt:
  REVOKE privileges ON table_name FROM roles
  {
    $4.IsTarget = true
    $$ = &Revoke{Table: $4, Privileges: $2, Roles: $6}
  }
;


roles:
  STRING
  {
    $$ = []string{string($1[1:len($1)-1])}
  }
| roles ',' STRING
  {
    $$ = append($1, string($3[1:len($3)-1]))
  }
;

privileges:
  privilege
  {
    privileges := make(map[string]struct{})
    privileges[$1] = struct{}{}
    $$ = Privileges(privileges)
  }
| privileges ',' privilege
  {    
    if _, ok := $1[$3]; ok {
      yylex.(*Lexer).AddError(&ErrGrantRepeatedPrivilege{Privilege: $3})
    } 
    
    $1[$3] = struct{}{}
    $$ = $1
  }
;

privilege:
  INSERT
  {
    $$ = "insert"
  }
| UPDATE
  {
    $$ = "update"
  }
| DELETE
  {
    $$ = "delete"
  }
;

identifier:
  IDENTIFIER
  {
    literalUpper := bytes.ToUpper($1)
    if _, ok := keywordsNotAllowed[string(literalUpper)]; ok {
      yylex.(*Lexer).AddError(&ErrKeywordIsNotAllowed{Keyword: string($1)})
    } 

    $$ = Identifier($1)
  }
%%

