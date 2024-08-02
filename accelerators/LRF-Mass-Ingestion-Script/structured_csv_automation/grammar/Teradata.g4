/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file is an adaptation of Presto's presto-parser/src/main/antlr4/com/facebook/presto/sql/parser/SqlBase.g4 grammar.
 */

grammar Teradata;

statement
    : query                                                                             #queryCallback
    | createTable                                                                       #createCallback
    | insertInto                                                                        #insertCallback
    | update                                                                            #updateCallback
    | delete                                                                            #deleteCallback
    | dropTable                                                                              #dropCallback
    ;

createTable
    : CREATE (SET | MULTISET)? (GLOBAL TEMPORARY | VOLATILE)? TABLE tableIdentifier (','? tableProperties)*
        (('(' ( columnsList  ','?)+ ')') | (AS '(' query ')' WITH DATA))?
        (tableIndex (',' tableIndex)*)?
        (ON COMMIT PRESERVE ROWS)?
    ;
columnsList
    : identifier dataType ( (FORMAT STRING) | ('(' INTEGER_VALUE ')') )?
    ;
tableProperties
    : NO? FALLBACK
    | NO? LOG
    ;

tableIndex
    : UNIQUE? PRIMARY INDEX identifier? identifierList
    ;

query
    : ctes? queryNoWith
    | transactions
    ;
transactions
    : BT | BEGIN TRANSACTION
    | ET | END TRANSACTION
    ;

insertInto
    : INSERT INTO? tableIdentifier identifierList? (valuesExpression | queryTerm) queryOrganization
    ;

update
    : UPDATE left=relationPrimary
        (fromClause)?
        SET updateSetExpression (',' updateSetExpression)*
        updateCondition?
    ;

updateSetExpression
    : identifier EQ expression
    ;

updateCondition
    : whereClause
    ;

delete
    : DELETE FROM right=relationPrimary
        deleteCondition?
    ;

deleteCondition
    : whereClause
    ;

dropTable
    : DROP TABLE tableIdentifier
    ;

ctes
    : WITH namedQuery (',' namedQuery)*
    ;

namedQuery
    : name=identifier AS? '(' query ')'
    ;

tableProvider
    : USING qualifiedName
    ;

queryNoWith
    : queryTerm queryOrganization
    ;

queryOrganization
    : (ORDER BY order+=sortItem (',' order+=sortItem)*)?
      (CLUSTER BY clusterBy+=expression (',' clusterBy+=expression)*)?
      (DISTRIBUTE BY distributeBy+=expression (',' distributeBy+=expression)*)?
      (SORT BY sort+=sortItem (',' sort+=sortItem)*)?
      windows?
      (LIMIT limit=expression)?
    ;

queryTerm
    : queryPrimary                                                                         #queryTermDefault
    | left=queryTerm operator=(INTERSECT | UNION | EXCEPT | SETMINUS) setQuantifier? right=queryTerm  #setOperation
    ;

queryPrimary
    : querySpecification                                                    #queryPrimaryDefault
    | TABLE tableIdentifier                                                 #table
    | inlineTable                                                           #inlineTableDefault1
    | '(' queryNoWith  ')'                                                  #subquery
    ;

sortItem
    : expression ordering=(ASC | DESC)? (NULLS nullOrder=(LAST | FIRST))?
    ;

querySpecification
    : SELECT setQuantifier? namedExpressionSeq fromClause?
        lateralView*
        whereClause?
        aggregation?
        havingClause?
        qualifyClause?
        windows?
    ;

fromClause
    : FROM relation (',' relation)* lateralView*
    ;

whereClause
    : WHERE booleanExpression
    ;

havingClause
    : HAVING booleanExpression
    ;

qualifyClause
    : QUALIFY booleanExpression
    ;

aggregation
    : GROUP BY groupingExpressions+=expression (',' groupingExpressions+=expression)* (
      WITH kind=ROLLUP
    | WITH kind=CUBE
    | kind=GROUPING SETS '(' groupingSet (',' groupingSet)* ')')?
    ;

groupingSet
    : '(' (expression (',' expression)*)? ')'
    | expression
    ;

lateralView
    : LATERAL VIEW (OUTER)? qualifiedName '(' (expression (',' expression)*)? ')' tblName=identifier (AS? colName+=identifier (',' colName+=identifier)*)?
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

relation
    : relationPrimary joinRelation*
    ;

joinRelation
    : (joinType) JOIN right=relationPrimary joinCriteria?
    | NATURAL joinType JOIN right=relationPrimary
    ;

joinType
    : INNER?
    | CROSS
    | LEFT OUTER?
    | LEFT SEMI
    | RIGHT OUTER?
    | FULL OUTER?
    | LEFT? ANTI
    ;

joinCriteria
    : ON booleanExpression
    | USING '(' identifier (',' identifier)* ')'
    ;

sample
    : TABLESAMPLE '('
      ( (percentage=(INTEGER_VALUE | DECIMAL_VALUE) sampleType=PERCENTLIT)
      | (expression sampleType=ROWS)
      | sampleType=BYTELENGTH_LITERAL
      | (sampleType=BUCKET numerator=INTEGER_VALUE OUT OF denominator=INTEGER_VALUE (ON (identifier | qualifiedName '(' ')'))?))
      ')'
    ;

identifierList
    : '(' identifierSeq ')'
    ;

identifierSeq
    : identifier (',' identifier)*
    ;

orderedIdentifierList
    : '(' orderedIdentifier (',' orderedIdentifier)* ')'
    ;

orderedIdentifier
    : identifier ordering=(ASC | DESC)?
    ;

identifierCommentList
    : '(' identifierComment (',' identifierComment)* ')'
    ;

identifierComment
    : identifier (COMMENT STRING)?
    ;

relationPrimary
    : tableIdentifier sample? (AS? strictIdentifier)?
    | '(' queryNoWith ')' sample? (AS? strictIdentifier)?
    | '(' relation ')' sample? (AS? strictIdentifier)?
    | inlineTable
    | identifier '(' (expression (',' expression)*)? ')'
    ;

inlineTable
    : VALUES? expression (',' expression)*  (AS? identifier identifierList?)?
    ;

valuesExpression
    : VALUES? '(' expression (',' expression)* ')'
    ;

tableIdentifier
    : (db=identifier '.')? table=identifier
    ;

namedExpression
    : expression (AS? (identifier | identifierList))?
    ;

namedExpressionSeq
    : namedExpression (',' namedExpression)*
    | '*'
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : NOT booleanExpression                                        #logicalNot
    | predicated                                                   #booleanDefault
    | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
    | EXISTS '(' query ')'                                         #exists
    ;

// workaround for:
//  https://github.com/antlr/antlr4/issues/780
//  https://github.com/antlr/antlr4/issues/781
predicated
    : valueExpression predicate?
    ;

predicate
    : NOT? kind=BETWEEN lower=valueExpression AND upper=valueExpression
    | NOT? kind=IN '(' expression (',' expression)* ')'
    | NOT? kind=IN '(' query ')'
    | NOT? kind=(RLIKE | LIKE) pattern=valueExpression
    | IS NOT? kind=NULL
    ;

valueExpression
    : primaryExpression '(' dataType ')'                                                     #teradataCast
    | primaryExpression                                                                      #valueExpressionDefault
    | operator=(MINUS | PLUS | TILDE) valueExpression                                        #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT | DIV) right=valueExpression #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression                     #arithmeticBinary
    | left=valueExpression operator=AMPERSAND right=valueExpression                          #arithmeticBinary
    | left=valueExpression operator=HAT right=valueExpression                                #arithmeticBinary
    | left=valueExpression operator=PIPE right=valueExpression                               #arithmeticBinary
    | left=valueExpression comparisonOperator right=valueExpression                          #comparison
    ;

primaryExpression
    : primaryExpression cse=caseSpecific                                                       #caseSpecificExpression
    | primaryExpression (STRING_CONCATENATE primaryExpression)+                                #concatenateExpression
    | name=(CURRENT_DATE | CURRENT_TIMESTAMP | DATE) ('(' precision=INTEGER_VALUE ')')?        #timeFunctionCall
    | SUBSTRING '(' stringExpression=expression FROM n1=expression (FOR n2=expression)? ')'    #substringCall
    | POSITION '(' expression IN expression ')'                                                    #positionFunctionCall
    | TRIM '('
        (trimType=(BOTH | TRAILING | LEADING) trimExpression=expression? FROM)?
        stringExpression=expression ')'                                                        #trimCall
    | CASE valueExpression whenClause+ (ELSE elseExpression=expression)? END                   #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END                                   #searchedCase
    | CAST '(' expression AS dataType? (dataAttribute)? ')'                                    #cast
    | value=primaryExpression '[' index=valueExpression ']'                                    #subscript
    | base=primaryExpression '.' fieldName=identifier                                          #dereference
    | ASTERISK                                                                                 #star
    | qualifiedName '.' ASTERISK                                                               #star
    | qualifiedName '(' (setQuantifier? expression (',' expression)*)? ')' (OVER windowSpec)?  #functionCall
    | ':' identifier                                                                           #columnNameReference
    | identifier                                                                               #columnReference
    | '(' expression (',' expression)+ ')'                                                     #rowConstructor
    | '(' query ')'                                                                            #subqueryExpression
    | '(' expression ')'                                                                       #parenthesizedExpression
    | constant                                                                                 #constantDefault
    ;
dataAttribute
    : (FORMAT|TITLE) dateFormat=STRING
    | dataTypePrecision
    ;

dataTypePrecision
    : '(' INTEGER_VALUE ')'
    ;


constant
    : NULL                                                                                     #nullLiteral
    | interval                                                                                 #intervalLiteral
    | identifier STRING                                                                        #typeConstructor
    | number                                                                                   #numericLiteral
    | booleanValue                                                                             #booleanLiteral
    | STRING+                                                                                  #stringLiteral
    ;

caseSpecific
    : '(' negate=NOT? (CS | CASESPECIFIC) ')'
    ;

comparisonOperator
    : EQ | NEQ | NEQJ | LT | LTE | GT | GTE | NSEQ
    ;

arithmeticOperator
    : PLUS | MINUS | ASTERISK | SLASH | PERCENT | DIV | TILDE | AMPERSAND | PIPE | HAT
    ;

predicateOperator
    : OR | AND | IN | NOT
    ;

booleanValue
    : TRUE | FALSE
    ;

interval
    : INTERVAL intervalField*
    ;

intervalField
    : value=intervalValue unit=identifier (TO to=identifier)?
    ;

intervalValue
    : (PLUS | MINUS)? (INTEGER_VALUE | DECIMAL_VALUE)
    | STRING
    ;

dataType
    : BYTEINT
    | SMALLINT
    | INT
    | INTEGER
    | BIGINT
    | (DECIMAL | DEC | NUMERIC | NUMBER) ('(' INTEGER_VALUE (',' INTEGER_VALUE)? ')')?
    | FLOAT | REAL
    | DOUBLE_PRECISION
    | DATE
    | DATETIME
    | TIME
    | TIME('(' INTEGER_VALUE ')')?
    | TIMESTAMP('(' INTEGER_VALUE ')')?
    | (CHAR | CHARACTER | VARCHAR | CHARACTER_VARYING | CHAR_VARYING) ('(' INTEGER_VALUE ')')?
    | LONG_VARCHAR
    | intervalDataType
    | complexDataType
    ;

intervalDataType
    : INTERVAL identifier
    ;

complexDataType
    : complx=ARRAY '<' dataType '>'
    | complx=MAP '<' dataType ',' dataType '>'
    | complx=STRUCT ('<' colTypeList? '>' | NEQ)
    ;

colTypeList
    : colType (',' colType)*
    ;

colType
    : identifier ':'? dataType (COMMENT STRING)?
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

windows
    : WINDOW namedWindow (',' namedWindow)*
    ;

namedWindow
    : identifier AS windowSpec
    ;

windowSpec
    : name=identifier  #windowRef
    | '('
      ( CLUSTER BY partition+=expression (',' partition+=expression)*
      | ((PARTITION | DISTRIBUTE) BY partition+=expression (',' partition+=expression)*)?
        ((ORDER | SORT) BY sortItem (',' sortItem)*)?)
      windowFrame?
      ')'              #windowDef
    ;

windowFrame
    : frameType=RANGE start=frameBound
    | frameType=ROWS start=frameBound
    | frameType=RANGE BETWEEN start=frameBound AND end=frameBound
    | frameType=ROWS BETWEEN start=frameBound AND end=frameBound
    ;

frameBound
    : UNBOUNDED boundType=(PRECEDING | FOLLOWING)
    | boundType=CURRENT ROW
    | expression boundType=(PRECEDING | FOLLOWING)
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

identifier
    : strictIdentifier
    | ANTI | FULL | INNER | LEFT | SEMI | RIGHT | NATURAL | JOIN | CROSS | ON
    | UNION | INTERSECT | EXCEPT | SETMINUS
    ;

strictIdentifier
    : (DOLLAR)? key=IDENTIFIER
    | quotedIdentifier
    | nonReserved
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

number
    : MINUS? DECIMAL_VALUE            #decimalLiteral
    | MINUS? SCIENTIFIC_DECIMAL_VALUE #scientificDecimalLiteral
    | MINUS? INTEGER_VALUE            #integerLiteral
    | MINUS? BIGINT_LITERAL           #bigIntLiteral
    | MINUS? SMALLINT_LITERAL         #smallIntLiteral
    | MINUS? TINYINT_LITERAL          #tinyIntLiteral
    | MINUS? DOUBLE_LITERAL           #doubleLiteral
    | MINUS? BIGDECIMAL_LITERAL       #bigDecimalLiteral
    ;

nonReserved
    : SHOW | TABLES | COLUMNS | COLUMN | PARTITIONS | FUNCTIONS | DATABASES
    | ADD
    | OVER | PARTITION | RANGE | PRESERVE | ROWS | PRECEDING | FOLLOWING | CURRENT | ROW | LAST | FIRST
    | MAP | ARRAY | STRUCT
    | LATERAL | WINDOW | REDUCE | TRANSFORM | USING | SERDE | SERDEPROPERTIES | RECORDREADER
    | DELIMITED | FIELDS | TERMINATED | COLLECTION | ITEMS | KEYS | ESCAPED | LINES | SEPARATED
    | EXTENDED | REFRESH | CLEAR | CACHE | UNCACHE | LAZY | GLOBAL | TEMPORARY | VOLATILE | OPTIONS
    | GROUPING | CUBE | ROLLUP
    | EXPLAIN | FORMAT | LOGICAL | FORMATTED | CODEGEN
    | TABLESAMPLE | USE | TO | BUCKET | PERCENTLIT | OUT | OF
    | MULTISET| SET | RESET
    | VIEW | REPLACE
    | IF
    | NO | DATA
    | START | TRANSACTION | COMMIT | ROLLBACK
    | SORT | CLUSTER | DISTRIBUTE | UNSET | TBLPROPERTIES | SKEWED | STORED | DIRECTORIES | LOCATION
    | EXCHANGE | ARCHIVE | UNARCHIVE | FILEFORMAT | TOUCH | COMPACT | CONCATENATE | CHANGE
    | CASCADE | RESTRICT | BUCKETS | CLUSTERED | SORTED | PURGE | INPUTFORMAT | OUTPUTFORMAT
    | DBPROPERTIES | DFS | TRUNCATE | COMPUTE | LIST
    | STATISTICS | ANALYZE | PARTITIONED | EXTERNAL | DEFINED | RECORDWRITER
    | REVOKE | GRANT | LOCK | UNLOCK | MSCK | REPAIR | RECOVER | EXPORT | IMPORT | LOAD | VALUES | COMMENT | ROLE
    | ROLES | COMPACTIONS | PRINCIPALS | TRANSACTIONS | INDEX | INDEXES | LOCKS | OPTION | LOCAL | INPATH
    | ASC | DESC | LIMIT | RENAME | SETS | UNIQUE | PRIMARY
    | AT | NULLS | OVERWRITE | ALL | ALTER | AS | BETWEEN | BY | CREATE | DELETE
    | DESCRIBE | DROP | EXISTS | FALSE | FOR | GROUP | IN | INSERT | INTO | IS | LIKE | UPDATE
    | NULL | ORDER | OUTER | TABLE | TRUE | WITH | RLIKE
    | AND | CASE | CAST | DISTINCT | DIV | ELSE | END | FUNCTION
    | BYTEINT | SMALLINT | INT | INTEGER | BIGINT | DECIMAL | DEC | NUMERIC | NUMBER | FLOAT | REAL | DOUBLE_PRECISION
    | DATE | DATETIME | TIMESTAMP | CHAR | CHARACTER | VARCHAR | CHARACTER_VARYING | CHAR_VARYING | LONG_VARCHAR
    | INTERVAL
    | MACRO | OR | STRATIFY | THEN
    | UNBOUNDED | WHEN
    | DATABASE | SELECT | FROM | WHERE | HAVING | TO | TABLE | WITH | NOT | CURRENT_DATE | CURRENT_TIMESTAMP
    ;

SELECT: 'SELECT' | 'SEL' | 'select' | 'sel' | 'Select' | 'Sel' ;
FROM: 'FROM' | 'from' | 'From' ;
ADD: 'ADD' | 'add' | 'Add' ;
AS: 'AS' | 'as' | 'As' ;
ALL: 'ALL' | 'all' | 'All' ;
DISTINCT: 'DISTINCT' | 'distinct' | 'Distinct' ;
WHERE: 'WHERE' | 'where' | 'Where' ;
GROUP: 'GROUP' | 'group' | 'Group' ;
BY: 'BY' | 'by' | 'By' ;
GROUPING: 'GROUPING' | 'grouping' | 'Grouping' ;
SETS: 'SETS' | 'sets' | 'Sets' ;
CUBE: 'CUBE' | 'cube' | 'Cube' ;
ROLLUP: 'ROLLUP' | 'rollup' | 'Rollup' ;
ORDER: 'ORDER' | 'order' | 'Order' ;
HAVING: 'HAVING' | 'having' | 'Having' ;
QUALIFY: 'QUALIFY' | 'qualify' | 'Qualify' ;
LIMIT: 'LIMIT' | 'limit' | 'Limit' ;
AT: 'AT' | 'at' | 'At' ;
OR: 'OR' | 'or' | 'Or' ;
AND: 'AND' | 'and' | 'And' ;
IN: 'IN' | 'in' | 'In' ;
NOT: 'NOT' | '!' | 'not'  | 'Not' ;
NO: 'NO' | 'no' | 'No' ;
EXISTS: 'EXISTS' | 'exists' | 'Exists' ;
BETWEEN: 'BETWEEN' | 'between' | 'Between' ;
LIKE: 'LIKE' | 'like' | 'Like' ;
RLIKE: 'RLIKE' | 'REGEXP' | 'rlike' | 'regexp' | 'Rlike' | 'Regexp' ;
IS: 'IS' | 'is' | 'Is' ;
NULL: 'NULL' | 'null' | 'Null' ;
TRUE: 'TRUE' | 'true' | 'True' ;
FALSE: 'FALSE' | 'false' | 'False' ;
NULLS: 'NULLS' | 'nulls' | 'Nulls' ;
ASC: 'ASC' | 'asc' | 'Asc' ;
DESC: 'DESC' | 'desc' | 'Desc' ;
FOR: 'FOR' | 'for' | 'For' ;
BYTEINT: 'BYTEINT' | 'byteint' | 'Byteint' ;
SMALLINT: 'SMALLINT' | 'smallint' | 'Smallint' ;
INT: 'INT' | 'int' | 'Int' ;
INTEGER: 'INTEGER' | 'integer' | 'Integer' ;
BIGINT: 'BIGINT' | 'bigint' | 'Bigint' ;
DECIMAL: 'DECIMAL' | 'decimal' | 'Decimal' ;
DEC: 'DEC' | 'dec' | 'Dec' ;
NUMERIC: 'NUMERIC' | 'numeric' | 'Numeric' ;
NUMBER: 'NUMBER' | 'number' | 'Number' ;
FLOAT: 'FLOAT' | 'float' | 'Float' ;
REAL: 'REAL' | 'real' | 'Real' ;
DOUBLE_PRECISION: 'DOUBLE PRECISION' | 'double precision' | 'Double Precision' ;
DATE: 'DATE' | 'date' | 'Date' ;
DATETIME: 'DATETIME' | 'datetime' | 'Datetime' ;
TIMESTAMP: 'TIMESTAMP' | 'timestamp' | 'Timestamp' ;
CHAR: 'CHAR' | 'char' | 'Char' ;
CHARACTER: 'CHARACTER' | 'character' | 'Character' ;
VARCHAR: 'VARCHAR' | 'varchar' | 'Varchar' ;
CHARACTER_VARYING: 'CHARACTER VARYING' | 'character varying' | 'Character Varying' ;
CHAR_VARYING: 'CHAR VARYING' | 'char varying' | 'Char Varying' ;
LONG_VARCHAR: 'LONG VARCHAR' | 'long varchar' | 'Long Varchar' ;
INTERVAL: 'INTERVAL' | 'interval' | 'Interval' ;
CASE: 'CASE' | 'case' | 'Case' ;
WHEN: 'WHEN' | 'when' | 'When' ;
THEN: 'THEN' | 'then' | 'Then' ;
ELSE: 'ELSE' | 'else' | 'Else' ;
END: 'END' | 'end' | 'End' ;
JOIN: 'JOIN' | 'join' | 'Join' ;
CROSS: 'CROSS' | 'cross' | 'Cross' ;
OUTER: 'OUTER' | 'outer' | 'Outer' ;
INNER: 'INNER' | 'inner' | 'Inner' ;
LEFT: 'LEFT' | 'left' | 'Left' ;
SEMI: 'SEMI' | 'semi' | 'Semi' ;
RIGHT: 'RIGHT' | 'right' | 'Right' ;
FULL: 'FULL' | 'full' | 'Full' ;
NATURAL: 'NATURAL' | 'natural' | 'Natural' ;
ON: 'ON' | 'on' | 'On' ;
LATERAL: 'LATERAL' | 'lateral' | 'Lateral' ;
WINDOW: 'WINDOW' | 'window' | 'Window' ;
OVER: 'OVER' | 'over' | 'Over' ;
PARTITION: 'PARTITION' | 'partition' | 'Partition' ;
RANGE: 'RANGE' | 'range' | 'Range' ;
PRESERVE: 'PRESERVE' | 'preserve' | 'Preserve' ;
ROWS: 'ROWS' | 'rows' | 'Rows' ;
UNBOUNDED: 'UNBOUNDED' | 'unbounded' | 'Unbounded' ;
PRECEDING: 'PRECEDING' | 'preceding' | 'Preceding' ;
FOLLOWING: 'FOLLOWING' | 'following' | 'Following' ;
CURRENT: 'CURRENT' | 'current' | 'Current' ;
FIRST: 'FIRST' | 'first' | 'First' ;
LAST: 'LAST' | 'last' | 'Last' ;
ROW: 'ROW' | 'row' | 'Row' ;
WITH: 'WITH' | 'with' | 'With' ;
VALUES: 'VALUES' | 'values' | 'Values' ;
CREATE: 'CREATE' | 'create' | 'Create' ;
TABLE: 'TABLE' | 'table' | 'Table' ;
VIEW: 'VIEW' | 'view' | 'View' ;
REPLACE: 'REPLACE' | 'replace' | 'Replace' ;
INSERT: 'INSERT' | 'INS' | 'insert' | 'ins' | 'Insert' | 'Ins' ;
UPDATE: 'UPDATE' | 'UPD' | 'update' | 'upd' | 'Update' | 'Upd' ;
DELETE: 'DELETE' | 'delete' | 'Delete' ;
INTO: 'INTO' | 'into' | 'Into' ;
DESCRIBE: 'DESCRIBE' | 'describe' | 'Describe' ;
EXPLAIN: 'EXPLAIN' | 'explain' | 'Explain' ;
FORMAT: 'FORMAT' | 'format' | 'Format' ;
LOGICAL: 'LOGICAL' | 'logical' | 'Logical' ;
CODEGEN: 'CODEGEN' | 'codegen' | 'Codegen' ;
CAST: 'CAST' | 'cast' | 'Cast' ;
SHOW: 'SHOW' | 'show' | 'Show' ;
TABLES: 'TABLES' | 'tables' | 'Tables' ;
COLUMNS: 'COLUMNS' | 'columns' | 'Columns' ;
COLUMN: 'COLUMN' | 'column' | 'Column' ;
USE: 'USE' | 'use' | 'Use' ;
PARTITIONS: 'PARTITIONS' | 'partitions' | 'Partitions' ;
FUNCTIONS: 'FUNCTIONS' | 'functions' | 'Functions' ;
DROP: 'DROP' | 'drop' | 'Drop' ;
UNION: 'UNION' | 'union' | 'Union' ;
EXCEPT: 'EXCEPT' | 'except' | 'Except' ;
SETMINUS: 'MINUS' | 'minus' | 'Minus' ;
INTERSECT: 'INTERSECT' | 'intersect' | 'Intersect' ;
TO: 'TO' | 'to' | 'To' ;
TABLESAMPLE: 'TABLESAMPLE' | 'tablesample' | 'Tablesample' ;
STRATIFY: 'STRATIFY' | 'stratify' | 'Stratify' ;
ALTER: 'ALTER' | 'alter' | 'Alter' ;
RENAME: 'RENAME' | 'rename' | 'Rename' ;
ARRAY: 'ARRAY' | 'array' | 'Array' ;
MAP: 'MAP' | 'map' | 'Map' ;
STRUCT: 'STRUCT' | 'struct' | 'Struct' ;
COMMENT: 'COMMENT' | 'comment' | 'Comment' ;
MULTISET: 'MULTISET' | 'multiset' | 'Multiset' ;
SET: 'SET' | 'set' | 'Set' ;
RESET: 'RESET' | 'reset' | 'Reset' ;
DATA: 'DATA' | 'data' | 'Data' ;
START: 'START' | 'start' | 'Start' ;
TRANSACTION: 'TRANSACTION' | 'transaction' | 'Transaction' ;
COMMIT: 'COMMIT' | 'commit' | 'Commit' ;
ROLLBACK: 'ROLLBACK' | 'rollback' | 'Rollback' ;
MACRO: 'MACRO' | 'macro' | 'Macro' ;
IF: 'IF' | 'if' | 'If' ;
PERCENTLIT: 'PERCENT' | 'percent' | 'Percent' ;
BUCKET: 'BUCKET' | 'bucket' | 'Bucket' ;
OUT: 'OUT' | 'out' | 'Out' ;
OF: 'OF' | 'of' | 'Of' ;
SORT: 'SORT' | 'sort' | 'Sort' ;
CLUSTER: 'CLUSTER' | 'cluster' | 'Cluster' ;
DISTRIBUTE: 'DISTRIBUTE' | 'distribute' | 'Distribute' ;
OVERWRITE: 'OVERWRITE' | 'overwrite' | 'Overwrite' ;
TRANSFORM: 'TRANSFORM' | 'transform' | 'Transform' ;
REDUCE: 'REDUCE' | 'reduce' | 'Reduce' ;
USING: 'USING' | 'using' | 'Using' ;
SERDE: 'SERDE' | 'serde' | 'Serde' ;
SERDEPROPERTIES: 'SERDEPROPERTIES' | 'serdeproperties' | 'Serdeproperties' ;
RECORDREADER: 'RECORDREADER' | 'recordreader' | 'Recordreader' ;
RECORDWRITER: 'RECORDWRITER' | 'recordwriter' | 'Recordwriter' ;
DELIMITED: 'DELIMITED' | 'delimited' | 'Delimited' ;
FIELDS: 'FIELDS' | 'fields' | 'Fields' ;
TERMINATED: 'TERMINATED' | 'terminated' | 'Terminated' ;
COLLECTION: 'COLLECTION' | 'collection' | 'Collection' ;
ITEMS: 'ITEMS' | 'items' | 'Items' ;
KEYS: 'KEYS' | 'keys' | 'Keys' ;
ESCAPED: 'ESCAPED' | 'escaped' | 'Escaped' ;
LINES: 'LINES' | 'lines' | 'Lines' ;
SEPARATED: 'SEPARATED' | 'separated' | 'Separated' ;
FUNCTION: 'FUNCTION' | 'function' | 'Function' ;
EXTENDED: 'EXTENDED' | 'extended' | 'Extended' ;
REFRESH: 'REFRESH' | 'refresh' | 'Refresh' ;
CLEAR: 'CLEAR' | 'clear' | 'Clear' ;
CACHE: 'CACHE' | 'cache' | 'Cache' ;
UNCACHE: 'UNCACHE' | 'uncache' | 'Uncache' ;
LAZY: 'LAZY' | 'lazy' | 'Lazy' ;
FORMATTED: 'FORMATTED' | 'formatted' | 'Formatted' ;
TEMPORARY: 'TEMPORARY' | 'TEMP' | 'temporary' | 'temp' | 'Temporary' | 'Temp' ;
GLOBAL: 'GLOBAL' | 'global' | 'Global' ;
VOLATILE: 'VOLATILE' | 'volatile' | 'Volatile' ;
OPTIONS: 'OPTIONS' | 'options' | 'Options' ;
UNSET: 'UNSET' | 'unset' | 'Unset' ;
TBLPROPERTIES: 'TBLPROPERTIES' | 'tblproperties' | 'Tblproperties' ;
DBPROPERTIES: 'DBPROPERTIES' | 'dbproperties' | 'Dbproperties' ;
BUCKETS: 'BUCKETS' | 'buckets' | 'Buckets' ;
SKEWED: 'SKEWED' | 'skewed' | 'Skewed' ;
STORED: 'STORED' | 'stored' | 'Stored' ;
DIRECTORIES: 'DIRECTORIES' | 'directories' | 'Directories' ;
LOCATION: 'LOCATION' | 'location' | 'Location' ;
EXCHANGE: 'EXCHANGE' | 'exchange' | 'Exchange' ;
ARCHIVE: 'ARCHIVE' | 'archive' | 'Archive' ;
UNARCHIVE: 'UNARCHIVE' | 'unarchive' | 'Unarchive' ;
FILEFORMAT: 'FILEFORMAT' | 'fileformat' | 'Fileformat' ;
TOUCH: 'TOUCH' | 'touch' | 'Touch' ;
COMPACT: 'COMPACT' | 'compact' | 'Compact' ;
CONCATENATE: 'CONCATENATE' | 'concatenate' | 'Concatenate' ;
CHANGE: 'CHANGE' | 'change' | 'Change' ;
CASCADE: 'CASCADE' | 'cascade' | 'Cascade' ;
RESTRICT: 'RESTRICT' | 'restrict' | 'Restrict' ;
CLUSTERED: 'CLUSTERED' | 'clustered' | 'Clustered' ;
SORTED: 'SORTED' | 'sorted' | 'Sorted' ;
PURGE: 'PURGE' | 'purge' | 'Purge' ;
INPUTFORMAT: 'INPUTFORMAT' | 'inputformat' | 'Inputformat' ;
OUTPUTFORMAT: 'OUTPUTFORMAT' | 'outputformat' | 'Outputformat' ;
DATABASE: 'DATABASE' | 'SCHEMA' | 'database' | 'schema' | 'Database' | 'Schema' ;
DATABASES: 'DATABASES' | 'SCHEMAS' | 'databases' | 'schemas' | 'Databases' | 'Schemas' ;
DFS: 'DFS' | 'dfs' | 'Dfs' ;
TRUNCATE: 'TRUNCATE' | 'truncate' | 'Truncate' ;
ANALYZE: 'ANALYZE' | 'analyze' | 'Analyze' ;
COMPUTE: 'COMPUTE' | 'compute' | 'Compute' ;
LIST: 'LIST' | 'list' | 'List' ;
STATISTICS: 'STATISTICS' | 'statistics' | 'Statistics' ;
PARTITIONED: 'PARTITIONED' | 'partitioned' | 'Partitioned' ;
EXTERNAL: 'EXTERNAL' | 'external' | 'External' ;
DEFINED: 'DEFINED' | 'defined' | 'Defined' ;
REVOKE: 'REVOKE' | 'revoke' | 'Revoke' ;
GRANT: 'GRANT' | 'grant' | 'Grant' ;
LOCK: 'LOCK' | 'lock' | 'Lock' ;
UNLOCK: 'UNLOCK' | 'unlock' | 'Unlock' ;
MSCK: 'MSCK' | 'msck' | 'Msck' ;
REPAIR: 'REPAIR' | 'repair' | 'Repair' ;
RECOVER: 'RECOVER' | 'recover' | 'Recover' ;
EXPORT: 'EXPORT' | 'export' | 'Export' ;
IMPORT: 'IMPORT' | 'import' | 'Import' ;
LOAD: 'LOAD' | 'load' | 'Load' ;
ROLE: 'ROLE' | 'role' | 'Role' ;
ROLES: 'ROLES' | 'roles' | 'Roles' ;
COMPACTIONS: 'COMPACTIONS' | 'compactions' | 'Compactions' ;
PRINCIPALS: 'PRINCIPALS' | 'principals' | 'Principals' ;
TRANSACTIONS: 'TRANSACTIONS' | 'transactions' | 'Transactions' ;
INDEX: 'INDEX' | 'index' | 'Index' ;
INDEXES: 'INDEXES' | 'indexes' | 'Indexes' ;
UNIQUE: 'UNIQUE' | 'unique' | 'Unique' ;
PRIMARY: 'PRIMARY' | 'primary' | 'Primary' ;
LOCKS: 'LOCKS' | 'locks' | 'Locks' ;
OPTION: 'OPTION' | 'option' | 'Option' ;
ANTI: 'ANTI' | 'anti' | 'Anti' ;
LOCAL: 'LOCAL' | 'local' | 'Local' ;
INPATH: 'INPATH' | 'inpath' | 'Inpath' ;
CURRENT_DATE: 'CURRENT_DATE' | 'current_date' | 'Current_date' ;
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP' | 'current_timestamp' | 'Current_timestamp' ;
SUBSTRING: 'SUBSTRING' | 'substring' | 'Substring' ;
TRIM: 'TRIM' | 'trim' | 'Trim' ;
BOTH: 'BOTH' | 'both' | 'Both' ;
TRAILING: 'TRAILING' | 'trailing' | 'Trailing' ;
LEADING: 'LEADING' | 'leading' | 'Leading' ;
CS: 'CS' | 'cs' | 'Cs' ;
CASESPECIFIC: 'CASESPECIFIC' | 'casespecific' | 'Casespecific' ;
LOG: 'LOG' | 'log' | 'Log';
ET: 'ET';
BT: 'BT';
BEGIN: 'BEGIN' | 'begin'| 'Begin';
TIME: 'TIME' | 'time' | 'Time';
FALLBACK: 'FALLBACK' | 'fallback' | 'Fallback';
TITLE: 'TITLE' | 'title' | 'Title';
POSITION: 'POSITION' | 'position' | 'Position';

EQ  : '=' | '==';
NSEQ: '<=>';
NEQ : '<>';
NEQJ: '!=';
LT  : '<';
LTE : '<=' | '!>';
GT  : '>';
GTE : '>=' | '!<';
DOLLAR: '$';
REFERENCE: ':';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
DIV: 'DIV';
TILDE: '~';
AMPERSAND: '&';
PIPE: '|';
HAT: '^';
STRING_CONCATENATE: '||';

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

BIGINT_LITERAL
    : DIGIT+ 'L'
    ;

SMALLINT_LITERAL
    : DIGIT+ 'S'
    ;

TINYINT_LITERAL
    : DIGIT+ 'Y'
    ;

BYTELENGTH_LITERAL
    : DIGIT+ ('B' | 'K' | 'M' | 'G')
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DECIMAL_DIGITS
    ;

SCIENTIFIC_DECIMAL_VALUE
    : DIGIT+ EXPONENT
    | DECIMAL_DIGITS EXPONENT
    ;

DOUBLE_LITERAL
    : DIGIT+ EXPONENT? 'D'
    | DECIMAL_DIGITS EXPONENT? 'D'
    ;

BIGDECIMAL_LITERAL
    : DIGIT+ EXPONENT? 'BD'
    | DECIMAL_DIGITS EXPONENT? 'BD'
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

DOT
    : '.'
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    | '\'' ( ~('\''|'\\') | ('\\' .) )* '\'' DOT '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [a-zA-Z]
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;