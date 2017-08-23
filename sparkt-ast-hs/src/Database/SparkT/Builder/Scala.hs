{-# LANGUAGE DeriveGeneric, GADTs, FlexibleContexts, OverloadedStrings,
             UndecidableInstances, TypeSynonymInstances, FlexibleInstances,
             ScopedTypeVariables, DeriveFoldable, DeriveTraversable,
             AllowAmbiguousTypes #-}
module Database.SparkT.Builder.Scala where

import Prelude hiding (Ordering)

import Control.Monad (msum)

import Data.Text (Text)
import Data.List
import Data.String.Conv (toS)
import Data.Typeable
import Data.Char
import Data.Set.Monad (Set, toList, fromList)

import Database.SparkT.AST.SQL

dQ = "\""

class ToScalaExpr a where
  toSE :: a -> String

data StandardScalaCtor = None | Some | Seq
  deriving (Show, Eq)

instance ToScalaExpr () where
  toSE () = "()"
instance ToScalaExpr a => ToScalaExpr [a] where
  toSE l = classCtor Seq (map toSE l)
instance (ToScalaExpr a, Ord a) => ToScalaExpr (Set a) where
  toSE a = toSE (toList a)
instance ToScalaExpr a => ToScalaExpr (Maybe a) where
  toSE Nothing = classCtor None []
  toSE (Just a) = classCtor Some [toSE a]
instance ToScalaExpr Text where
  toSE = show
instance {-# OVERLAPPING #-} ToScalaExpr String where
  toSE = show
instance ToScalaExpr TypeRep where
  toSE a = "T" ++ show a ++ "()"
instance (ToScalaExpr a, ToScalaExpr b) => ToScalaExpr (a, b) where
  toSE (a,b)= concat ["(", toSE a, ",", toSE b, ")"]
instance (ToScalaExpr a, ToScalaExpr b, ToScalaExpr c) => ToScalaExpr (a, b, c) where
  toSE (a,b,c)= concat ["(", toSE a, ",", toSE b, ",", toSE c, ")"]
instance ToScalaExpr Integer where
  toSE = show
instance ToScalaExpr Bool where
  toSE = map toLower . show

classCtor :: Show a => a -> [String] -> String
classCtor name [] = if show name == "None" then "None" else (show name ++ "()")
classCtor name l = concat [show name, "(", intercalate "," l, ")"]

data ScalaCtor =
  SSelect | SInsert | SUnop | SBinop | SUnionTables | SIntersectTables |
  SExceptTable | SInsertValues | SInsertSelect | SFrom | SJoin | SAsc | SDesc | STableNamed |
  SInnerJoin | SOuterJoin | SRightJoin | SLeftJoin | SSelectTable |
  SSelectCommand | SFromTable | STableFromSubSelect | SInsertCommand |
  SFieldName | SUnqualifiedField | SQualifiedField | SGrouping |
  SAgg | SSetQuantifierAll | SSetQuantifierDistinct | SCompOp |
  SComparatorQuantifierAny | SComparatorQuantifierAll | SStar
  deriving (Show)

cleanShow a = filter (/= '\"') (show a)
notImplemented a = concat ["throw NotImplemented(\"", cleanShow a, "\")"]
unhandledType a = concat ["throw UnhandledType(\"", cleanShow a, "\")"]

-- Command
instance (ToScalaExpr (Select a),
          ToScalaExpr (Insert a),
          Show a) => ToScalaExpr (Command a) where
  toSE (SelectCommand select) = classCtor SSelectCommand [toSE select]
  toSE (InsertCommand insert) = classCtor SInsertCommand [toSE insert]
  toSE a = notImplemented a

-- Select
instance (ToScalaExpr a, Show a) => ToScalaExpr (Select a) where
  toSE (Select table ord limit offset) = classCtor SSelect [toSE table, toSE ord, toSE limit, toSE offset]


-- SelectTable
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

-- Insert
instance (ToScalaExpr a, Show a) => ToScalaExpr (Insert a) where
  toSE (Insert info table schema fields values) =
    classCtor SInsert [toSE info, show table, toSE fields, toSE values]

-- InsertValues
instance (ToScalaExpr (Expression a),
          ToScalaExpr (Select a)) => ToScalaExpr (InsertValues a) where
  toSE (InsertValues values) = classCtor SInsertValues [toSE values]
  toSE (InsertSelect select) = classCtor SInsertSelect [toSE select]

-- Update

-- Delete
instance Show a => ToScalaExpr (Delete a) where
  toSE a = notImplemented a

-- FieldName
instance ToScalaExpr FieldName where
  toSE (UnqualifiedField f) = classCtor SUnqualifiedField [show f]
  toSE (QualifiedField q f) = classCtor SQualifiedField [show q, show f]

-- ComparatorQuantifier
instance ToScalaExpr ComparatorQuantifier where
  toSE ComparatorQuantifierAny = classCtor SComparatorQuantifierAny []
  toSE ComparatorQuantifierAll = classCtor SComparatorQuantifierAll []

-- ExtractField

-- CastTarget

-- DataType

-- SetQuantifier
instance ToScalaExpr SetQuantifier where
  toSE SetQuantifierAll = classCtor SSetQuantifierAll []
  toSE SetQuantifierDistinct = classCtor SSetQuantifierDistinct []

-- Expression
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

-- Window

-- WindowFrame

-- WindowFramePos

-- Projection
instance (ToScalaExpr (Expression a)) => ToScalaExpr (Projection a) where
  toSE (ProjExprs projs) = toSE projs

-- Ordering
instance (ToScalaExpr (Expression a)) => ToScalaExpr (Ordering a) where
  toSE (OrderingAsc e) = classCtor SAsc [toSE e]
  toSE (OrderingDesc e) = classCtor SDesc [toSE e]

-- Grouping
instance (ToScalaExpr (Expression a)) => ToScalaExpr (Grouping a) where
  toSE (Grouping l) = classCtor SGrouping $ map toSE l

-- TableSource
instance (ToScalaExpr a,
          ToScalaExpr (Projection a),
          ToScalaExpr (From a),
          ToScalaExpr (Grouping a),
          ToScalaExpr (Expression a),
          Show a) => ToScalaExpr (TableSource a) where
  toSE (TableNamed info name _) = classCtor STableNamed [toSE info, toSE name] -- Schema not used
  toSE (TableFromSubSelect select) =
    classCtor STableFromSubSelect [toSE select]

-- From
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

-- Value
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
