%{
package sqlparser

const MaxColumnNameLength = 64
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
}

%token <bytes> IDENTIFIER STRING INTEGRAL HEXNUM FLOAT BLOB
%token ERROR 
%token <empty> TRUE FALSE NULL AND
%token <empty> '(' ',' ')' '.'
%token <empty> NONE INTEGER NUMERIC REAL TEXT CAST AS
%token <empty> CASE WHEN THEN ELSE END
%token <empty> SELECT FROM WHERE GROUP BY HAVING LIMIT OFFSET ORDER ASC DESC NULLS FIRST LAST DISTINCT ALL EXISTS FILTER

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

%type <selectStmt> select_stmt
%type <expr> expr literal_value function_call_keyword function_call_generic expr_opt else_expr_opt exists_subquery
%type <exprs> expr_list expr_list_opt group_by_opt
%type <string> cmp_op cmp_inequality_op like_op between_op asc_desc_opt distinct_opt
%type <column> column_name as_column_opt col_alias
%type <SelectColumn> select_column
%type <SelectColumnList> select_column_list
%type <table> table_name as_table_opt table_alias
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
%type <columnList> column_name_list
%type <subquery> subquery
%type <colTuple> col_tuple
%type <bool> distinct_function_opt

%%
start: 
  select_stmt { yylex.(*Lexer).ast = &AST{$1} }
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
    $$ = nil
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
  IDENTIFIER
  {
    $$ = &Column{Name: string($1)}
  }
| STRING
  {
    $$ = &Column{Name: string($1)}
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
    $$ = nil
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
  IDENTIFIER
  {
    $$ = &Table{Name: string($1)}
  }
| STRING
  {
    $$ = &Table{Name: string($1)}
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
  IDENTIFIER
  { 
    $$ = &Table{Name : string($1)} 
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
| expr COLLATE IDENTIFIER
  {  
    $$ = &CollateExpr{Expr : $1, CollationName: string($3)}
  }
| '(' expr_list ')'
  {
    $$ = $2
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
  STRING
  {
    $$ = &Value{Type: StrValue, Value: $1[1:len($1)-1]}
  }
|  INTEGRAL
  {
    $$ = &Value{Type: IntValue, Value: $1}
  }
|  FLOAT
  {
    $$ = &Value{Type: FloatValue, Value: $1}
  }
| BLOB
  {
    $$ = &Value{Type: BlobValue, Value: $1}
  }
|  HEXNUM
  {
    $$ = &Value{Type: HexNumValue, Value: $1}
  }
|  TRUE
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
  IDENTIFIER
  { 
    if len($1) > MaxColumnNameLength {
      yylex.Error(__yyfmt__.Sprintf("column length greater than %d", MaxColumnNameLength))
      return 1
    }
    $$ = &Column{Name : string($1)} 
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
    $$ = &FuncExpr{Name: &Column{Name: "glob"}, Args: Exprs{$3, $5}} 
  }
| LIKE '(' expr ',' expr ')'
  {
    $$ = &FuncExpr{Name: &Column{Name: "like"}, Args: Exprs{$3, $5}} 
  }
| LIKE '(' expr ',' expr ',' expr ')'
  {
    $$ = &FuncExpr{Name: &Column{Name: "like"}, Args: Exprs{$3, $5, $7}} 
  }
;

function_call_generic:
  IDENTIFIER '(' distinct_function_opt expr_list_opt ')' filter_opt
  {
    if _, ok := AllowedFunctions[string($1)]; !ok {
      yylex.Error(__yyfmt__.Sprintf("function '%s' does not exist,", string($1)))
      return 1
    }
    $$ = &FuncExpr{Name: &Column{Name: string($1)}, Distinct: $3, Args: $4, Filter: $6}
  }
| IDENTIFIER '(' '*' ')' filter_opt
  {
    if _, ok := AllowedFunctions[string($1)]; !ok {
      yylex.Error(__yyfmt__.Sprintf("function '%s' does not exist", string($1)))
      return 1
    }
    $$ = &FuncExpr{Name: &Column{Name: string($1)}, Distinct: false, Args: nil, Filter: $5}
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

%%

