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
}

%token <bytes> IDENTIFIER STRING INTEGRAL HEXNUM FLOAT BLOB
%token ERROR 
%token <empty> TRUE FALSE NULL AND
%token <empty> '(' ',' ')' '.'
%token <empty> NONE INTEGER NUMERIC REAL TEXT CAST AS

%left <empty> OR
%left <empty> ANDOP
%right <empty> NOT
%left <empty> IS MATCH GLOB REGEXP LIKE BETWEEN IN ISNULL NOTNULL NE '=' 
%left <empty> '<' '>' LE GE INEQUALITY
%right <empty> ESCAPE 
%left '&' '|' LSHIFT RSHIFT
%left <empty> '+' '-'
%left <empty> '*' '/' '%'
%left <empty> CONCAT JSON_EXTRACT_OP JSON_UNQUOTE_EXTRACT_OP
%left <empty> COLLATE
%right <empty> '~' UNARY

%type <expr> expr literal_value function_call_keyword
%type <exprs> expr_list
%type <string> cmp_op cmp_inequality_op like_op between_op
%type <column> column_name 
%type <table> table_name
%type <convertType> convert_type

%%
start: 
  expr { yylex.(*Lexer).ast = &AST{$1} } ;


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

table_name:
  IDENTIFIER 
  { 
    $$ = &Table{Name : string($1)} 
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

function_call_keyword:
  CAST '(' expr AS convert_type ')'
  {
    $$ = &ConvertExpr{Expr: $3, Type: $5}
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
| expr IS NOT expr
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
| expr COLLATE IDENTIFIER
  {  
    $$ = &CollateExpr{Expr : $1, CollationName: string($3)}
  }
| '(' expr_list ')'
  {
      $$ = $2
  }
| function_call_keyword
;
%%