%{
package sqlparser

import "bytes"

var keywordsNotAllowed = map[string]struct{}{
	"CURRENT_TIME":      {},
	"CURRENT_DATE":      {},
	"CURRENT_TIMESTAMP": {},
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
  SelectColumn SelectColumn
  SelectColumnList SelectColumnList
  selectStmt *Select
  where *Where
  limit *Limit
  orderBy OrderBy
  orderingTerm *OrderingTerm
  nulls NullsType
  tableExprList TableExprList
  tableExpr TableExpr
  joinTableExpr *JoinTableExpr
  columnList ColumnList
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
}

%token <bytes> IDENTIFIER STRING INTEGRAL HEXNUM FLOAT BLOBVAL
%token ERROR 
%token <empty> TRUE FALSE NULL AND
%token <empty> '(' ',' ')' '.' ';'
%token <empty> NONE INTEGER NUMERIC REAL TEXT CAST AS
%token <empty> CASE WHEN THEN ELSE END
%token <empty> SELECT FROM WHERE GROUP BY HAVING LIMIT OFFSET ORDER ASC DESC NULLS FIRST LAST DISTINCT ALL EXISTS FILTER
%token <empty> CREATE TABLE INT BLOB ANY PRIMARY KEY UNIQUE CHECK DEFAULT GENERATED ALWAYS STORED VIRTUAL CONSTRAINT
%token <empty> INSERT INTO VALUES DELETE UPDATE SET
%token <empty> GRANT TO REVOKE

%left <empty> JOIN
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
%type <selectStmt> select_stmt
%type <createTableStmt> create_table_stmt
%type <expr> expr literal_value function_call_keyword function_call_generic expr_opt else_expr_opt exists_subquery signed_number
%type <exprs> expr_list expr_list_opt group_by_opt
%type <string> cmp_op cmp_inequality_op like_op between_op asc_desc_opt distinct_opt type_name primary_key_order privilege
%type <column> column_name 
%type <identifier> as_column_opt col_alias as_table_opt table_alias constraint_name identifier
%type <SelectColumn> select_column
%type <SelectColumnList> select_column_list
%type <table> table_name
%type <where> where_opt having_opt filter_opt
%type <convertType> convert_type
%type <when> when 
%type <whens> when_expr_list
%type <limit> limit_opt
%type <orderBy> order_by_opt order_list
%type <orderingTerm> ordering_term
%type <nulls> nulls
%type <tableExprList> table_expr_list from_clause
%type <tableExpr> table_expr
%type <joinTableExpr> join_clause join_constraint
%type <columnList> column_name_list column_name_list_opt
%type <subquery> subquery
%type <colTuple> col_tuple
%type <bool> distinct_function_opt is_stored
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
  SELECT distinct_opt select_column_list from_clause where_opt group_by_opt having_opt order_by_opt limit_opt
  {
    $$ = &Select{
            Distinct: $2,
            SelectColumnList: $3, 
            From: $4, 
            Where: $5, 
            GroupBy: GroupBy($6), 
            Having: $7, 
            OrderBy: $8,
            Limit: $9,
         }
  }

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
  FROM table_expr_list
  {
    $$ = $2
  }
| FROM join_clause
  {
    $$ = TableExprList{$2}
  }
;

table_expr_list:
  table_expr
  {
    $$ = TableExprList{$1}
  }
| table_expr_list ',' table_expr
  {
    $$ = append($$, $3)
  }
;

table_expr:
  table_name as_table_opt
  {
    $$ = &AliasedTableExpr{Expr: $1, As: $2}
  }
| '(' select_stmt ')' as_table_opt
  {
    $$ = &AliasedTableExpr{Expr: &Subquery{Select: $2}, As: $4}
  }
| '(' table_expr_list ')'
  {
    $$ = &ParenTableExpr{TableExprList: $2}
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
    $$ = Identifier(string($1))
  }
;

join_clause:
  table_expr JOIN table_expr join_constraint
  {
    if $4 == nil {
      $$ = &JoinTableExpr{LeftExpr: $1, JoinOperator: JoinStr, RightExpr: $3}
    } else {
      $4.LeftExpr = $1
      $4.JoinOperator = JoinStr
      $4.RightExpr = $3
      $$ = $4
    }
  }
| join_clause JOIN table_expr join_constraint
  {
    if $4 == nil {
      $$ = &JoinTableExpr{LeftExpr: $1, JoinOperator: JoinStr, RightExpr: $3}
    } else {
      $4.LeftExpr = $1
      $4.JoinOperator = JoinStr
      $4.RightExpr = $3
      $$ = $4
    }
  }
;

join_constraint:
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
    $$ = &Table{Name : $1} 
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
| REAL { $$ = RealStr}
| INTEGER { $$ = IntegerStr}
| NUMERIC { $$ = NumericStr}
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
    if _, ok := AllowedFunctions[string($1)]; !ok {
      yylex.(*Lexer).AddError(&ErrNoSuchFunction{FunctionName: string($1)})
    }
    $$ = &FuncExpr{Name: Identifier(string($1)), Distinct: $3, Args: $4, Filter: $6}
  }
| identifier '(' '*' ')' filter_opt
  {
    if _, ok := AllowedFunctions[string($1)]; !ok {
      yylex.(*Lexer).AddError(&ErrNoSuchFunction{FunctionName: string($1)})
    }
    $$ = &FuncExpr{Name: Identifier(string($1)), Distinct: false, Args: nil, Filter: $5}
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
    $$ = &CreateTable{Table: $3, Columns: $5, Constraints: $6}
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
    $$ = &ColumnDef{Name: $1, Type: $2, Constraints: $3}
  }
;

type_name:
  INT { $$ = TypeIntStr}
| INTEGER { $$ = TypeIntegerStr}
| REAL { $$ = TypeRealStr}
| TEXT { $$ = TypeTextStr}
| BLOB { $$ = TypeBlobStr}
| ANY { $$ = TypeAnyStr}
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
    $$ = []ColumnConstraint{$1}
  }
| column_constraints column_constraint
  {
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
    $$ = ColumnConstraintPrimaryKeyOrderEmpty
  }
|  ASC
  {
    $$ = ColumnConstraintPrimaryKeyOrderAsc
  }
| DESC
  {
    $$ = ColumnConstraintPrimaryKeyOrderDesc
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
    $$ = []TableConstraint{$2}
  }
| table_constraint_list ','  table_constraint
  {
    $$ = append($1, $3)
  }
;

table_constraint:
  constraint_name PRIMARY KEY '(' column_name_list ')'
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

insert_stmt:
  INSERT INTO table_name column_name_list_opt VALUES insert_rows
  {
    for _, row := range $6 {
      for _, expr := range row {
				if expr.ContainsSubquery() {
          yylex.(*Lexer).AddError(&ErrStatementContainsSubquery{StatementKind: "insert"})
				}
			}
    }
    $$ = &Insert{Table: $3, Columns: $4, Rows: $6}
  }
| INSERT INTO table_name DEFAULT VALUES
  {
    $$ = &Insert{Table: $3, Columns: ColumnList{}, Rows: []Exprs{}, DefaultValues: true}
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

delete_stmt:
  DELETE FROM table_name where_opt
  {
    if $4 != nil && $4.Expr.ContainsSubquery() {
      yylex.(*Lexer).AddError(&ErrStatementContainsSubquery{StatementKind: "delete"})
    }
    $$ = &Delete{Table: $3, Where: $4}
  }
;

update_stmt:
  UPDATE table_name SET update_list where_opt
  {
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
    if $1.Expr.ContainsSubquery() {
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
        exprs[i] = &UpdateExpr{Column: $2[i], Expr: $6[i]}
      }
      $$ = exprs
    }
  }
;

update_expression:
  column_name '=' expr
  {
    $$ = &UpdateExpr{Column: $1, Expr: $3}
  }
;

grant_stmt:
  GRANT privileges ON table_name TO roles
  {
    $$ = &Grant{Table: $4, Privileges: $2, Roles: $6}
  }
;

revoke_stmt:
  REVOKE privileges ON table_name FROM roles
  {
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
    if len($1) == 3 {
      yylex.(*Lexer).AddError(&ErrGrantPrivilegesCountExceeded{PrivilegesCount: len($1) + 1, MaxAllowed: 3})
    }
    
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

