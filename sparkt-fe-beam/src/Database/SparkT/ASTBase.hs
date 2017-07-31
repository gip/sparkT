{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, TypeFamilies,
    FlexibleContexts, ScopedTypeVariables, PartialTypeSignatures,
    GADTs, MultiParamTypeClasses, OverloadedStrings,
    AllowAmbiguousTypes, UndecidableInstances #-}
module Database.SparkT.ASTBase(
  module Database.SparkT.AST.SQL ) where

import Prelude hiding (Ordering)

import Database.Beam
import Database.Beam.Backend.SQL.SQL92
import Database.Beam.Backend.SQL.SQL99
import Database.Beam.Query
import Database.Beam.Query.SQL92
import Database.Beam.Schema (TableSettings)

import Data.Text (Text)

import Database.SparkT.AST.SQL

instance Eq a => IsSql92Syntax (Command a) where
  type Sql92SelectSyntax (Command a) = Select a
  type Sql92UpdateSyntax (Command a) = Update a
  type Sql92InsertSyntax (Command a) = Insert a
  type Sql92DeleteSyntax (Command a) = Delete a
  selectCmd = SelectCommand
  insertCmd = InsertCommand
  updateCmd = UpdateCommand
  deleteCmd = DeleteCommand

instance Eq a => IsSql92SelectSyntax (Select a) where
  type Sql92SelectSelectTableSyntax (Select a) = SelectTable a
  type Sql92SelectOrderingSyntax (Select a) = Ordering a
  selectStmt = Select

instance Eq a => IsSql92SelectTableSyntax (SelectTable a) where
  type Sql92SelectTableSelectSyntax (SelectTable a) = Select a
  type Sql92SelectTableExpressionSyntax (SelectTable a) = Expression a
  type Sql92SelectTableProjectionSyntax (SelectTable a) = Projection a
  type Sql92SelectTableFromSyntax (SelectTable a) = From a
  type Sql92SelectTableGroupingSyntax (SelectTable a) = Grouping a
  selectTableStmt = SelectTable
  unionTables = UnionTables
  intersectTables = IntersectTables
  exceptTable = ExceptTable

instance IsSql92InsertSyntax (Insert a) where
  type Sql92InsertValuesSyntax (Insert a) = InsertValues a
  type Sql92InsertValuesInfo (Insert a) = a
  insertStmt info table schema fields values =
    -- TODO: remove the table schema and use the database schema
    Insert info table (Just schema) fields values

instance IsSql92InsertValuesSyntax (InsertValues a) where
  type Sql92InsertValuesExpressionSyntax (InsertValues a) = Expression a
  type Sql92InsertValuesSelectSyntax (InsertValues a) = Select a
  insertSqlExpressions = InsertValues
  insertFromSql = InsertSelect

instance IsSql92UpdateSyntax (Update a) where
  type Sql92UpdateFieldNameSyntax (Update a) = FieldName
  type Sql92UpdateExpressionSyntax (Update a) = Expression a
  updateStmt = Update

instance IsSql92DeleteSyntax (Delete a) where
  type Sql92DeleteExpressionSyntax (Delete a) = Expression a
  deleteStmt = Delete

instance IsSql92FieldNameSyntax FieldName where
  qualifiedField = QualifiedField
  unqualifiedField = UnqualifiedField

instance IsSql92QuantifierSyntax ComparatorQuantifier where
  quantifyOverAll = ComparatorQuantifierAll
  quantifyOverAny = ComparatorQuantifierAny

instance IsSql92AggregationSetQuantifierSyntax SetQuantifier where
  setQuantifierDistinct = SetQuantifierDistinct
  setQuantifierAll = SetQuantifierAll

instance IsSqlExpressionSyntaxStringType Expression Text

instance IsSql92ExpressionSyntax (Expression a) where
  type Sql92ExpressionQuantifierSyntax (Expression a) = ComparatorQuantifier
  type Sql92ExpressionValueSyntax (Expression a) = Value
  type Sql92ExpressionSelectSyntax (Expression a) = Select a
  type Sql92ExpressionFieldNameSyntax (Expression a) = FieldName
  type Sql92ExpressionCastTargetSyntax (Expression a) = CastTarget
  type Sql92ExpressionExtractFieldSyntax (Expression a) = ExtractField
  valueE = ExpressionValue
  rowE = ExpressionRow
  isNullE = ExpressionIsNull
  isNotNullE = ExpressionIsNotNull
  isTrueE = ExpressionIsTrue
  isNotTrueE = ExpressionIsNotTrue
  isFalseE = ExpressionIsFalse
  isNotFalseE = ExpressionIsNotFalse
  isUnknownE = ExpressionIsUnknown
  isNotUnknownE = ExpressionIsNotUnknown
  caseE = ExpressionCase
  coalesceE = ExpressionCoalesce
  nullIfE = ExpressionNullIf
  positionE = ExpressionPosition
  extractE = ExpressionExtract
  castE = ExpressionCast
  fieldE = ExpressionFieldName
  betweenE = ExpressionBetween
  andE = ExpressionBinOp "AND"
  orE = ExpressionBinOp "OR"
  eqE = ExpressionCompOp "=="
  neqE = ExpressionCompOp "<>"
  ltE = ExpressionCompOp "<"
  gtE = ExpressionCompOp ">"
  leE = ExpressionCompOp "<="
  geE = ExpressionCompOp ">="
  addE = ExpressionBinOp "+"
  subE = ExpressionBinOp "-"
  mulE = ExpressionBinOp "*"
  divE = ExpressionBinOp "/"
  modE = ExpressionBinOp "%"
  likeE = ExpressionBinOp "LIKE"
  overlapsE = ExpressionBinOp "OVERLAPS"
  notE = ExpressionUnOp "NOT"
  negateE = ExpressionUnOp "-"
  charLengthE = ExpressionCharLength
  octetLengthE = ExpressionOctetLength
  bitLengthE = ExpressionBitLength
  absE = ExpressionAbs
  subqueryE = ExpressionSubquery
  uniqueE = ExpressionUnique
  existsE = ExpressionExists
  currentTimestampE = ExpressionCurrentTimestamp

instance IsSql99ExpressionSyntax (Expression a) where
  distinctE = ExpressionDistinct
  similarToE = ExpressionBinOp "SIMILAR TO"
  functionCallE = ExpressionFunctionCall
  instanceFieldE = ExpressionInstanceField
  refFieldE = ExpressionRefField

instance IsSql92AggregationExpressionSyntax (Expression a) where
  type Sql92AggregationSetQuantifierSyntax (Expression a) = SetQuantifier
  countAllE = ExpressionCountAll
  countE q = ExpressionAgg "COUNT" q . pure
  sumE q = ExpressionAgg "SUM" q . pure
  minE q = ExpressionAgg "MIN" q . pure
  maxE q = ExpressionAgg "MAX" q . pure
  avgE q = ExpressionAgg "AVG" q . pure

instance IsSql92ProjectionSyntax (Projection a) where
  type Sql92ProjectionExpressionSyntax (Projection a) = Expression a
  projExprs = ProjExprs

instance IsSql92OrderingSyntax (Ordering a) where
  type Sql92OrderingExpressionSyntax (Ordering a) = Expression a
  ascOrdering = OrderingAsc
  descOrdering = OrderingDesc

instance IsSql92GroupingSyntax (Grouping a) where
  type Sql92GroupingExpressionSyntax (Grouping a) = Expression a
  groupByExpressions = Grouping

instance IsSql92TableSourceSyntax (TableSource a) where
  type Sql92TableSourceSelectSyntax (TableSource a) = Select a
  type Sql92TableSourceInfo (TableSource a) = a -- Tout ca pour ca!
  tableNamed = TableNamed
  tableFromSubSelect = TableFromSubSelect

instance IsSql92FromSyntax (From a) where
  type Sql92FromTableSourceSyntax (From a) = TableSource a
  type Sql92FromExpressionSyntax (From a) = Expression a
  fromTable = FromTable
  innerJoin = InnerJoin
  leftJoin = LeftJoin
  rightJoin = RightJoin

instance (Show a, Eq a, Typeable a) => HasSqlValueSyntax Value a where
  sqlValueSyntax = Value

instance Eq a => HasQBuilder (Select a) where
  buildSqlQuery = buildSql92Query' True
