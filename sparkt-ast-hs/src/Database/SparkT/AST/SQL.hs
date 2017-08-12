{-# LANGUAGE DeriveGeneric, GADTs, FlexibleContexts, OverloadedStrings,
             UndecidableInstances, TypeSynonymInstances, FlexibleInstances,
             ScopedTypeVariables, DeriveFoldable, DeriveTraversable #-}
module Database.SparkT.AST.SQL where

import Prelude hiding (Ordering)

import Control.Monad (msum)

import Data.Text (Text)
import Data.List
import Data.String.Conv (toS)
import Data.Typeable

import GHC.Generics

import Database.SparkT.AST.Internal

-- TODO: use Text, not String
-- TODO: use Generics

data ScalaCtor =
  SSelect | SInsert | SUnop | SBinop | SUnionTables | SIntersectTables |
  SExceptTable | SInsertValues | SInsertSelect | SFrom | SJoin | SAsc | SDesc | STableNamed |
  SInnerJoin | SOuterJoin | SRightJoin | SLeftJoin | SSelectTable |
  SSelectCommand | SFromTable | STableFromSubSelect | SInsertCommand |
  SFieldName | SUnqualifiedField | SQualifiedField | SGrouping |
  SAgg | SSetQuantifierAll | SSetQuantifierDistinct | SCompOp |
  SComparatorQuantifierAny | SComparatorQuantifierAll | SStar
  deriving (Show)

type TableSchema = [(String, TypeRep, Bool)]

cleanShow a = filter (/= '\"') (show a)
notImplemented a = concat ["throw NotImplemented(\"", cleanShow a, "\")"]
unhandledType a = concat ["throw UnhandledType(\"", cleanShow a, "\")"]

data Command a
  = SelectCommand (Select a)
  | InsertCommand (Insert a)
  | UpdateCommand (Update a)
  | DeleteCommand (Delete a)
  deriving (Show, Eq)
instance (ToScalaExpr (Select a),
          ToScalaExpr (Insert a),
          Show a) => ToScalaExpr (Command a) where
  toSE (SelectCommand select) = classCtor SSelectCommand [toSE select]
  toSE (InsertCommand insert) = classCtor SInsertCommand [toSE insert]
  toSE a = notImplemented a

data Select a
    = Select
       (SelectTable a) -- selectTable
       [Ordering a]    -- selectOrdering
       (Maybe Integer) -- selectLimit
       (Maybe Integer) -- selectOffset
    deriving (Show, Eq, Generic, Foldable, Traversable, Functor)
instance (ToScalaExpr a, Show a) => ToScalaExpr (Select a) where
  toSE (Select table ord limit offset) = classCtor SSelect [toSE table, toSE ord, toSE limit, toSE offset]

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
instance (ToScalaExpr (Projection a),
          ToScalaExpr (From a),
          ToScalaExpr (Expression a),
          ToScalaExpr (Grouping a)) => ToScalaExpr (SelectTable a) where
  toSE (SelectTable proj from wher group having) =
    classCtor SSelectTable [toSE proj, toSE from, toSE wher, toSE group, toSE having]
  toSE (UnionTables b ta tb) =
    classCtor SUnionTables [toSE b, toSE ta, toSE tb]
  toSE (IntersectTables b ta tb) =
    classCtor SIntersectTables [toSE b, toSE ta, toSE tb]
  toSE (ExceptTable b ta tb) =
    classCtor SExceptTable [toSE b, toSE ta, toSE tb]

data Insert a
  = Insert
     (Maybe a)           -- insertDatabaseInfo
     Text                -- insertTable
     (Maybe TableSchema) -- insertTableSchema
     [Text]              -- insertFields
     (InsertValues a)    -- insertValues
  deriving (Show, Eq, Generic, Functor, Foldable)
instance (ToScalaExpr a, Show a) => ToScalaExpr (Insert a) where
  toSE (Insert info table schema fields values) =
    -- Schema is *not* used here
    classCtor SInsert [toSE info, show table, toSE fields, toSE values]

data InsertValues a
  = InsertValues [[Expression a]]
  | InsertSelect (Select a)
  deriving (Show, Eq, Generic, Foldable, Functor)
instance (ToScalaExpr (Expression a),
          ToScalaExpr (Select a)) => ToScalaExpr (InsertValues a) where
  toSE (InsertValues values) = classCtor SInsertValues [toSE values]
  toSE (InsertSelect select) = classCtor SInsertSelect [toSE select]

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
instance Show a => ToScalaExpr (Delete a) where
  toSE a = notImplemented a

data FieldName
  = QualifiedField Text Text
  | UnqualifiedField Text
  deriving (Show, Eq, Generic)
instance ToScalaExpr FieldName where
  toSE (UnqualifiedField f) = classCtor SUnqualifiedField [show f]
  toSE (QualifiedField q f) = classCtor SQualifiedField [show q, show f]

data ComparatorQuantifier
  = ComparatorQuantifierAny
  | ComparatorQuantifierAll
  deriving (Show, Eq, Generic)
instance ToScalaExpr ComparatorQuantifier where
  toSE ComparatorQuantifierAny = classCtor SComparatorQuantifierAny []
  toSE ComparatorQuantifierAll = classCtor SComparatorQuantifierAll []

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
instance ToScalaExpr SetQuantifier where
  toSE SetQuantifierAll = classCtor SSetQuantifierAll []
  toSE SetQuantifierDistinct = classCtor SSetQuantifierDistinct []

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
instance (ToScalaExpr (Expression a),
          ToScalaExpr (Select a),
          Show a) => ToScalaExpr (Expression a) where
  toSE (ExpressionValue v) = toSE v
  toSE (ExpressionBinOp op ea eb) = classCtor SBinop [show op, toSE ea, toSE eb]
  toSE (ExpressionUnOp op e) = classCtor SUnop [show op, toSE e]
  toSE (ExpressionIsNull e) = classCtor SUnop ["IsNull", toSE e]
  toSE (ExpressionIsNotNull e) = classCtor SUnop ["IsNotNull", toSE e]
  toSE (ExpressionIsFalse e) = classCtor SUnop ["IsFalse", toSE e]
  toSE (ExpressionIsNotFalse e) = classCtor SUnop ["IsNotFalse", toSE e]
  toSE (ExpressionAbs e) = classCtor SUnop ["Abs", toSE e]
  toSE (ExpressionExists e) = classCtor SUnop ["Exists", toSE e]
  toSE (ExpressionFieldName fn) = classCtor SFieldName [toSE fn]
  toSE (ExpressionAgg name quant exprs) = classCtor SAgg [show name, toSE quant, toSE exprs]
  toSE (ExpressionCompOp op quant lhs rhs) = classCtor SCompOp [show op, toSE quant, toSE lhs, toSE rhs]
  toSE a = notImplemented a

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
instance (ToScalaExpr (Expression a)) => ToScalaExpr (Projection a) where
  toSE (ProjExprs projs) = toSE projs

data Ordering a
  = OrderingAsc (Expression a)
  | OrderingDesc (Expression a)
  deriving (Show, Eq, Generic, Foldable, Traversable, Functor)
instance (ToScalaExpr (Expression a)) => ToScalaExpr (Ordering a) where
  toSE (OrderingAsc e) = classCtor SAsc [toSE e]
  toSE (OrderingDesc e) = classCtor SDesc [toSE e]

newtype Grouping a = Grouping [ Expression a ]
  deriving (Show, Eq, Generic, Foldable, Traversable, Functor)
instance (ToScalaExpr (Expression a)) => ToScalaExpr (Grouping a) where
  toSE (Grouping l) = classCtor SGrouping $ map toSE l

data TableSource a
  = TableNamed (Maybe a) Text TableSchema
  | TableFromSubSelect (Select a)
  deriving (Show, Eq, Generic, Foldable, Traversable, Functor)
instance (ToScalaExpr a,
          ToScalaExpr (Projection a),
          ToScalaExpr (From a),
          ToScalaExpr (Grouping a),
          ToScalaExpr (Expression a),
          Show a) => ToScalaExpr (TableSource a) where
  toSE (TableNamed info name _) = classCtor STableNamed [toSE info, toSE name] -- Schema not used
  toSE (TableFromSubSelect select) =
    classCtor STableFromSubSelect [toSE select]

data From a
  = FromTable (TableSource a) (Maybe Text)
  | InnerJoin (From a) (From a) (Maybe (Expression a))
  | LeftJoin (From a) (From a) (Maybe (Expression a))
  | RightJoin (From a) (From a) (Maybe (Expression a))
  | OuterJoin (From a) (From a) (Maybe (Expression a))
  deriving (Show, Eq, Generic, Foldable, Traversable, Functor)
instance (ToScalaExpr (Expression a),
          ToScalaExpr (Projection a),
          ToScalaExpr (Grouping a),
          ToScalaExpr a,
          Show a) => ToScalaExpr (From a) where
  toSE (FromTable src name) = classCtor SFromTable [toSE src, toSE name]
  toSE (InnerJoin fa fb e) = classCtor SInnerJoin [toSE fa, toSE fb, toSE e]
  toSE (LeftJoin fa fb e) = classCtor SLeftJoin [toSE fa, toSE fb, toSE e]
  toSE (RightJoin fa fb e) = classCtor SRightJoin [toSE fa, toSE fb, toSE e]
  toSE (OuterJoin fa fb e) = classCtor SOuterJoin [toSE fa, toSE fb, toSE e]

data Value where
  Value :: (Show a, Eq a, Typeable a) => a -> Value

-- TODO: the code below is ugly, is there any other concise way to write that code?
instance ToScalaExpr Value where
  toSE a = case msum (map (\f -> f a) [fInt, fStr, fDbl, fTxt, fMInt]) of
    Just s -> s
    _ -> unhandledType a
    where fInt (Value a) = case cast a of Just (i :: Int) -> Just $ "SLitInt(" ++ show i ++ ")"
                                          Nothing -> Nothing
          fStr (Value a) = case cast a of Just (s :: String) -> Just $ "SLitString(" ++ show s ++ ")"
                                          Nothing -> Nothing
          fTxt (Value a) = case cast a of Just (s :: Text) -> Just $ "SLitString(" ++ show s ++ ")"
                                          Nothing -> Nothing
          fDbl (Value a) = case cast a of Just (d :: Double) -> Just $ "SLitDouble(" ++ show d ++ ")"
                                          Nothing -> Nothing
          fMInt (Value a) = case cast a of Just (mi :: Maybe Int) -> -- Ugly, ugly ugly!
                                             case mi of Just i -> Just $ "SLitInt(" ++ show i ++ ")"
                                                        Nothing -> Just "SLitNull()" -- Is that right?
                                           Nothing -> Nothing
          fNull (Value a) = case cast a of Just Null -> Just "SLitNull()"
                                           Nothing -> Nothing
-- Special SQL value null
data Null = Null
  deriving (Show, Eq, Typeable)

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
