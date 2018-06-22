{-# LANGUAGE DeriveGeneric, GADTs, FlexibleContexts, OverloadedStrings,
             UndecidableInstances, TypeSynonymInstances, FlexibleInstances,
             ScopedTypeVariables, DeriveFoldable, DeriveTraversable,
             AllowAmbiguousTypes #-}
module Database.SparkT.AST.SQL where

import Prelude hiding (Ordering)

import Data.Text (Text)
import Data.Typeable

import GHC.Generics

-- TODO: remove that unused ObsoleteTableSchema thing
type ObsoleteTableSchema = [(String, TypeRep, Bool)]

data Command a
  = SelectCommand (Select a)
  | InsertCommand (Insert a)
  | UpdateCommand (Update a)
  | DeleteCommand (Delete a)
  deriving (Show, Eq)

data Select a
    = Select
       (SelectTable a) -- selectTable
       [Ordering a]    -- selectOrdering
       (Maybe Integer) -- selectLimit
       (Maybe Integer) -- selectOffset
    deriving (Show, Eq, Generic, Foldable, Traversable, Functor)

data SelectTable a
  = SelectTable
     (Projection a)         -- selectProjection
     (Maybe (From a))       -- selectFrom
     (Maybe (Expression a)) -- selectWhere
     (Maybe (Grouping a))   -- selectGrouping
     (Maybe (Expression a)) -- selectHaving
  | UnionTables Bool (SelectTable a) (SelectTable a)
  | IntersectTables Bool (SelectTable a) (SelectTable a)
  | ExceptTable Bool (SelectTable a) (SelectTable a)
  deriving (Show, Eq, Generic, Foldable, Traversable, Functor)

data Insert a
  = Insert
     a
     Text                -- insertTable
     (Maybe ObsoleteTableSchema) -- insertObsoleteTableSchema
     [Text]              -- insertFields
     (InsertValues a)    -- insertValues
  deriving (Show, Eq, Generic, Functor, Foldable)

data InsertValues a
  = InsertValues [[Expression a]]
  | InsertSelect (Select a)
  deriving (Show, Eq, Generic, Foldable, Functor)

data Update a
  = Update
     Text -- updateTable
     [(FieldName, Expression a)] -- updateFields
     (Maybe (Expression a)) -- updateWhere
  deriving (Show, Eq)

data Delete a
  = Delete
     Text -- deleteTable
     (Maybe (Expression a)) -- deleteWhere
  deriving (Show, Eq)

data FieldName
  = QualifiedField Text Text
  | UnqualifiedField Text
  deriving (Show, Eq, Generic)

data ComparatorQuantifier
  = ComparatorQuantifierAny
  | ComparatorQuantifierAll
  deriving (Show, Eq, Generic)

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

data CastTarget
  = CastTargetDataType DataType
  | CastTargetDomainName Text
  deriving (Show, Eq, Generic)

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

data SetQuantifier
  = SetQuantifierAll | SetQuantifierDistinct
  deriving (Show, Eq, Generic)

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
  | ExpressionWindow (Expression a) (Window a)

  | ExpressionSubquery (Select a)
  | ExpressionUnique (Select a)
  | ExpressionDistinct (Select a)
  | ExpressionExists (Select a)

  | ExpressionCurrentTimestamp
  deriving (Foldable, Show, Eq, Generic, Traversable, Functor)

data Window a
  = Window [Expression a]                     -- Partition by
           [Ordering a]                       -- Order by
           (Maybe (WindowFrame a))            -- Frame
  deriving (Foldable, Show, Eq, Generic, Traversable, Functor)

data WindowFrame a
  = WindowFrame Bool {- True for Range, False for Rows -}
                (WindowFramePos a)
                (Maybe (WindowFramePos a)) {- Between if that is Just -}
  deriving (Foldable, Show, Eq, Generic, Traversable, Functor)

data WindowFramePos a
  = WindowFrameUnboundedPreceding
  | WindowFramePreceding Integer
  | WindowFrameCurrentRow
  | WindowFrameUnboundedFollowing
  | WindowFrameFollowing Integer
  deriving (Foldable, Show, Eq, Generic, Traversable, Functor)

newtype Projection a
  = ProjExprs [(Expression a, Maybe Text)]
  deriving (Show, Eq, Generic, Foldable, Traversable, Functor)

data Ordering a
  = OrderingAsc (Expression a)
  | OrderingDesc (Expression a)
  deriving (Show, Eq, Generic, Foldable, Traversable, Functor)

newtype Grouping a = Grouping [ Expression a ]
  deriving (Show, Eq, Generic, Foldable, Traversable, Functor)

data TableSource a
  = TableNamed a Text ObsoleteTableSchema
  | TableFromSubSelect (Select a)
  deriving (Show, Eq, Generic, Foldable, Traversable, Functor)

data From a
  = FromTable (TableSource a) (Maybe Text)
  | InnerJoin (From a) (From a) (Maybe (Expression a))
  | LeftJoin (From a) (From a) (Maybe (Expression a))
  | RightJoin (From a) (From a) (Maybe (Expression a))
  | OuterJoin (From a) (From a) (Maybe (Expression a))
  deriving (Show, Eq, Generic, Foldable, Traversable, Functor)

data Value where
  Value :: (Show a, Eq a, Ord a, Typeable a) => a -> Value

-- Special SQL value null
data Null = Null
  deriving (Show, Eq, Ord, Typeable)

instance Eq Value where
  Value a == Value b =
    case cast a of
      Just a' -> a' == b
      Nothing -> False -- TODO: that should not happen

instance Ord Value where
  Value a <= Value b =
    case cast a of
      Just a' -> a' <= b
      Nothing -> error "wrong dynamic cast"

instance Show Value where
  showsPrec prec (Value a) =
    showParen (prec > app_prec) $
    ("Value " ++ ).
    showsPrec (app_prec + 1) a
    where app_prec = 10
