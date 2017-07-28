{-# LANGUAGE DeriveGeneric, TypeSynonymInstances, FlexibleInstances, TypeFamilies,
    FlexibleContexts, ScopedTypeVariables, PartialTypeSignatures,
    GADTs, MultiParamTypeClasses, OverloadedStrings, AllowAmbiguousTypes,
    UndecidableInstances #-}

-- | This module implements an AST type for SQL92. It allows us to realize
--   the call structure of the builders defined in 'Database.Beam.Backend.SQL92'
module Database.SparkT.ASTBase where

import Prelude hiding (Ordering)

import Database.Beam
import Database.Beam.Backend.SQL.SQL92
import Database.Beam.Backend.SQL.SQL99
import Database.Beam.Query
import Database.Beam.Query.SQL92
import Database.Beam.Schema (TableSettings)
import Database.SparkT.Internal

import Data.Text (Text)
import Data.Typeable
import GHC.Generics
import Data.Aeson hiding (Value)

data Command a
  = SelectCommand (Select a)
  | InsertCommand (Insert a)
  | UpdateCommand (Update a)
  | DeleteCommand (Delete a)
  deriving (Show, Eq)

instance Eq a => IsSql92Syntax (Command a) where
  type Sql92SelectSyntax (Command a) = Select a
  type Sql92UpdateSyntax (Command a) = Update a
  type Sql92InsertSyntax (Command a) = Insert a
  type Sql92DeleteSyntax (Command a) = Delete a

  selectCmd = SelectCommand
  insertCmd = InsertCommand
  updateCmd = UpdateCommand
  deleteCmd = DeleteCommand

data Select a
    = Select
    { selectTable :: SelectTable a
    , selectOrdering   :: [ Ordering a ]
    , selectLimit, selectOffset :: Maybe Integer }
    deriving (Show, Eq, Generic)
instance ToJSON a => ToJSON (Select a)

instance Eq a => IsSql92SelectSyntax (Select a) where
  type Sql92SelectSelectTableSyntax (Select a) = SelectTable a
  type Sql92SelectOrderingSyntax (Select a) = Ordering a

  selectStmt = Select

data SelectTable a
  = SelectTable
  { selectProjection :: (Projection a)
  , selectFrom       :: Maybe (From a)
  , selectWhere      :: Maybe (Expression a)
  , selectGrouping   :: Maybe (Grouping a)
  , selectHaving     :: Maybe (Expression a)}
  | UnionTables Bool (SelectTable a) (SelectTable a)
  | IntersectTables Bool (SelectTable a) (SelectTable a)
  | ExceptTable Bool (SelectTable a) (SelectTable a)
  deriving (Show, Eq, Generic)
instance ToJSON a => ToJSON (SelectTable a)

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

data Insert a
  = Insert
  { insertDatabaseInfo :: Maybe a
  , insertTable :: Text
  , insertTableSchema :: TableSchema
  , insertFields :: [ Text ]
  , insertValues :: InsertValues a }
  deriving (Show, Eq, Generic)
instance ToJSON a => ToJSON (Insert a)

instance IsSql92InsertSyntax (Insert a) where
  type Sql92InsertValuesSyntax (Insert a) = InsertValues a
  type Sql92InsertValuesInfo (Insert a) = a

  insertStmt = Insert

data InsertValues a
  = InsertValues
  { insertValuesExpressions :: [ [ Expression a ] ] }
  | InsertSelect
  { insertSelectStmt :: Select a }
  deriving (Show, Eq, Generic)
instance ToJSON a => ToJSON (InsertValues a)

instance IsSql92InsertValuesSyntax (InsertValues a) where
  type Sql92InsertValuesExpressionSyntax (InsertValues a) = Expression a
  type Sql92InsertValuesSelectSyntax (InsertValues a) = Select a

  insertSqlExpressions = InsertValues
  insertFromSql = InsertSelect

data Update a
  = Update
  { updateTable :: Text
  , updateFields :: [ (FieldName, (Expression a)) ]
  , updateWhere :: Maybe (Expression a) }
  deriving (Show, Eq)

instance IsSql92UpdateSyntax (Update a) where
  type Sql92UpdateFieldNameSyntax (Update a) = FieldName
  type Sql92UpdateExpressionSyntax (Update a) = Expression a

  updateStmt = Update

data Delete a
  = Delete
  { deleteTable :: Text
  , deleteWhere :: Maybe (Expression a) }
  deriving (Show, Eq)

instance IsSql92DeleteSyntax (Delete a) where
  type Sql92DeleteExpressionSyntax (Delete a) = Expression a

  deleteStmt = Delete

data FieldName
  = QualifiedField Text Text
  | UnqualifiedField Text
  deriving (Show, Eq, Generic)
instance ToJSON FieldName

instance IsSql92FieldNameSyntax FieldName where
  qualifiedField = QualifiedField
  unqualifiedField = UnqualifiedField

data ComparatorQuantifier
  = ComparatorQuantifierAny
  | ComparatorQuantifierAll
  deriving (Show, Eq, Generic)
instance ToJSON ComparatorQuantifier

instance IsSql92QuantifierSyntax ComparatorQuantifier where
  quantifyOverAll = ComparatorQuantifierAll
  quantifyOverAny = ComparatorQuantifierAny

data ExtractField
  = ExtractFieldTimeZoneHour
  | ExtractFieldTimeZoneMinute

  | ExtractFieldDateTimeYear
  | ExtractFieldDateTimeMonth
  | ExtractFieldDateTimeDay
  | ExtractFieldDateTimeHour
  | ExtractFieldDateTimeMinute
  | ExtractFieldDateTimeSecond
  deriving (Show, Eq, Generic)
instance ToJSON ExtractField

data CastTarget
  = CastTargetDataType DataType
  | CastTargetDomainName Text
  deriving (Show, Eq, Generic)
instance ToJSON CastTarget

data DataType
  = DataTypeChar Bool {- Varying -} (Maybe Int)
  | DataTypeNationalChar Bool (Maybe Int)
  | DataTypeBit Bool (Maybe Int)
  | DataTypeNumeric Int (Maybe Int)
  | DataTypeInteger
  | DataTypeSmallInt
  | DataTypeFloat (Maybe Int)
  | DataTypeReal
  | DataTypeDoublePrecision
  | DataTypeDate
  | DataTypeTime (Maybe Word) {- time fractional seconds precision -} Bool {- With time zone -}
  | DataTypeTimeStamp (Maybe Word) Bool
  | DataTypeInterval ExtractField
  | DataTypeIntervalFromTo ExtractField ExtractField
  deriving (Show, Eq, Generic)
instance ToJSON DataType

data SetQuantifier
  = SetQuantifierAll | SetQuantifierDistinct
  deriving (Show, Eq, Generic)
instance ToJSON SetQuantifier

instance IsSql92AggregationSetQuantifierSyntax SetQuantifier where
  setQuantifierDistinct = SetQuantifierDistinct
  setQuantifierAll = SetQuantifierAll

data Expression a
  = ExpressionValue Value
  | ExpressionRow [ Expression a ]

  | ExpressionIsNull (Expression a)
  | ExpressionIsNotNull (Expression a)
  | ExpressionIsTrue (Expression a)
  | ExpressionIsNotTrue (Expression a)
  | ExpressionIsFalse (Expression a)
  | ExpressionIsNotFalse (Expression a)
  | ExpressionIsUnknown (Expression a)
  | ExpressionIsNotUnknown (Expression a)

  | ExpressionCase [(Expression a, Expression a)] (Expression a)
  | ExpressionCoalesce [ Expression a ]
  | ExpressionNullIf (Expression a) (Expression a)

  | ExpressionFieldName FieldName

  | ExpressionBetween (Expression a) (Expression a) (Expression a)
  | ExpressionBinOp Text (Expression a) (Expression a)
  | ExpressionCompOp Text (Maybe ComparatorQuantifier) (Expression a) (Expression a)
  | ExpressionUnOp Text (Expression a)

  | ExpressionPosition (Expression a) (Expression a)
  | ExpressionCast (Expression a) CastTarget
  | ExpressionExtract ExtractField (Expression a)
  | ExpressionCharLength (Expression a)
  | ExpressionOctetLength (Expression a)
  | ExpressionBitLength (Expression a)
  | ExpressionAbs (Expression a)

  | ExpressionFunctionCall (Expression a) [ Expression a ]
  | ExpressionInstanceField (Expression a) Text
  | ExpressionRefField (Expression a) Text

  | ExpressionCountAll
  | ExpressionAgg Text (Maybe SetQuantifier) [ Expression a ]

  | ExpressionSubquery (Select a)
  | ExpressionUnique (Select a)
  | ExpressionDistinct (Select a)
  | ExpressionExists (Select a)

  | ExpressionCurrentTimestamp
  deriving (Show, Eq, Generic)
instance ToJSON a => ToJSON (Expression a)

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

newtype Projection a
  = ProjExprs [ (Expression a, Maybe Text ) ]
  deriving (Show, Eq, Generic)
instance ToJSON a => ToJSON (Projection a) where
  toJSON (ProjExprs x) = toJSON $ map (\(expr, mtext) -> object ["expr" .= toJSON expr,
                                                                 "as" .= toJSON mtext ]) x

instance IsSql92ProjectionSyntax (Projection a) where
  type Sql92ProjectionExpressionSyntax (Projection a) = Expression a

  projExprs = ProjExprs

data Ordering a
  = OrderingAsc (Expression a)
  | OrderingDesc (Expression a)
  deriving (Show, Eq, Generic)
instance ToJSON a => ToJSON (Ordering a)

instance IsSql92OrderingSyntax (Ordering a) where
  type Sql92OrderingExpressionSyntax (Ordering a) = Expression a

  ascOrdering = OrderingAsc
  descOrdering = OrderingDesc

newtype Grouping a = Grouping [ Expression a ] deriving (Show, Eq, Generic)
instance ToJSON a => ToJSON (Grouping a)

instance IsSql92GroupingSyntax (Grouping a) where
  type Sql92GroupingExpressionSyntax (Grouping a) = Expression a

  groupByExpressions = Grouping

data TableSource a
  = TableNamed (Maybe a) Text TableSchema
  | TableFromSubSelect (Select a)
  deriving (Show, Eq, Generic)
instance ToJSON a => ToJSON (TableSource a)

instance IsSql92TableSourceSyntax (TableSource a) where
  type Sql92TableSourceSelectSyntax (TableSource a) = Select a
  type Sql92TableSourceInfo (TableSource a) = a -- Tout ca pour ca!

  tableNamed = TableNamed
  tableFromSubSelect = TableFromSubSelect

data From a
  = FromTable (TableSource a) (Maybe Text)
  | InnerJoin (From a) (From a) (Maybe (Expression a))
  | LeftJoin (From a) (From a) (Maybe (Expression a))
  | RightJoin (From a) (From a) (Maybe (Expression a))
  | OuterJoin (From a) (From a) (Maybe (Expression a))
  deriving (Show, Eq, Generic)
instance ToJSON a => ToJSON (From a)

instance IsSql92FromSyntax (From a) where
  type Sql92FromTableSourceSyntax (From a) = TableSource a
  type Sql92FromExpressionSyntax (From a) = Expression a

  fromTable = FromTable
  innerJoin = InnerJoin
  leftJoin = LeftJoin
  rightJoin = RightJoin

data Value where
  Value :: (Show a, Eq a, Typeable a, ToJSON a) => a -> Value
instance ToJSON Value where
  toJSON (Value a) = toJSON a

instance (Show a, Eq a, Typeable a, ToJSON a) => HasSqlValueSyntax Value a where
  sqlValueSyntax = Value

instance Eq Value where
  Value a == Value b =
    case cast a of
      Just a' -> a' == b
      Nothing -> False
instance Show Value where
  showsPrec prec (Value a) =
    showParen (prec > app_prec) $
    ("Value " ++ ).
    showsPrec (app_prec + 1) a
    where app_prec = 10

instance Eq a => HasQBuilder (Select a) where
  buildSqlQuery = buildSql92Query' True
