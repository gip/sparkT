{-# LANGUAGE DeriveGeneric, GADTs, FlexibleContexts, OverloadedStrings,
             UndecidableInstances, TypeSynonymInstances, FlexibleInstances,
             ScopedTypeVariables, DeriveFoldable, DeriveTraversable,
             AllowAmbiguousTypes #-}
module Database.SparkT.Builder.SQL where

import Prelude hiding (Ordering)

import Control.Monad (msum)

import Data.Text (Text)
import Data.List
import Data.Char
import Data.String.Conv (toS)

import Database.SparkT.AST.SQL

class ToSQL a where
  toSQL :: a -> String

instance ToSQL a => ToSQL [a] where
  toSQL l = intercalate ", " $ map toSQL l

-- Select
instance (ToSQL (Expression a), ToSQL (From a)) => ToSQL (Select a) where
  toSQL (Select st
                ordering
                mLimit
                mOffset) =
    concat [toSQL st,
      if null ordering then ""
                       else " ORDER BY " ++ toSQL ordering,
      maybe "" (\limit -> " LIMIT " ++ show limit) mLimit,
      maybe "" (\offset -> " OFFSET " ++ show offset) mOffset
    ]

-- SelectTable
instance (ToSQL (Expression a), ToSQL (From a)) => ToSQL (SelectTable a) where
  toSQL (SelectTable projs mFrom mWhere mGrouping mHaving) =
    concat ["SELECT ", toSQL projs,
      maybe "" (\from -> " FROM " ++ toSQL from) mFrom,
      maybe "" (\wher_ -> " WHERE " ++ toSQL wher_) mWhere,
      maybe "" (\group -> " GROUP BY " ++ toSQL group) mGrouping
    ]
  toSQL (UnionTables isAll tLhs tRhs) =
    concat [ "(", toSQL tLhs, ") UNION ",
             if isAll then "ALL (" else "(",
             toSQL tRhs, ")" ]

-- Insert
instance ToSQL (Insert a) where
  toSQL (Insert _ table _ fields values) =
    concat ["INSERT INTO ", toS table,
      if null fields then " "
                     else " (" ++ intercalate ", " (map toS fields) ++ ") ",
      " ", toSQL values]

-- InsertValues
instance ToSQL (InsertValues a) where
  toSQL (InsertValues l) =
    "(" ++ intercalate ", " (map (\row -> "(" ++ intercalate ", " (map showV row) ++ ")") l) ++ ")"
      where showV (ExpressionValue v) = show v
  toSQL (InsertSelect select) = toSQL select

-- Update

-- Delete

-- FieldName

-- ComparatorQuantifier

-- ExtractField

-- CastTarget

-- DataType

-- SetQuantifier

-- Expression
instance ToSQL (Expression a) where
  toSQL (ExpressionValue (Value v)) = show v
  toSQL (ExpressionBinOp op ea eb) = concat ["(", toSQL ea, " ", toS op, " ", toSQL eb, ")"]
  toSQL (ExpressionCompOp op _ ea eb) = concat ["(", toSQL ea, " ", toS op, " ", toSQL eb, ")"]
  toSQL (ExpressionFieldName (QualifiedField s n)) = concat [toS s, ".", toS n]
  toSQL (ExpressionFieldName (UnqualifiedField n)) = concat [toS n]

-- Window

-- WindowFrame

-- WindowFramePos

-- Projection
instance ToSQL (Expression a) => ToSQL (Projection a) where
  toSQL (ProjExprs projs) = intercalate ", " $ doit projs
    where doit [] = []
          doit ((expr, Nothing):projs) = toSQL expr : doit projs
          doit ((expr, Just as):projs) = concat [toSQL expr, " AS ", toS as] : doit projs

-- Ordering
instance ToSQL (Expression a) => ToSQL (Ordering a) where
  toSQL (OrderingAsc expr) = concat [toSQL expr, " ASC"]
  toSQL (OrderingDesc expr) = concat [toSQL expr, " DESC"]

-- Grouping
instance ToSQL (Expression a) => ToSQL (Grouping a) where
  toSQL (Grouping l) = toSQL l

-- TableSource

-- From
instance ToSQL (Expression a) => ToSQL (From a) where
  toSQL (FromTable (TableNamed _ name _) mAlias) =
    concat [toS name, maybe " " (\a -> " " ++ toS a ++ " ") mAlias]
  toSQL (FromTable (TableFromSubSelect sel) mAlias) =
    concat [" (", toSQL sel, ") ", maybe " " (\a -> toS a ++ " ") mAlias]
  toSQL (InnerJoin fLhs fRhs on) = toSQLJoin "INNER" fLhs fRhs on
  toSQL (LeftJoin fLhs fRhs on) = toSQLJoin "LEFT" fLhs fRhs on
  toSQL (RightJoin fLhs fRhs on) = toSQLJoin "RIGHT" fLhs fRhs on
  toSQL (OuterJoin fLhs fRhs on) = toSQLJoin "OUTER" fLhs fRhs on

toSQLJoin jType fLhs fRhs on =
  concat [toSQL fLhs, " ", jType," JOIN ", toSQL fRhs,
          maybe "" (\expr -> "ON " ++ toSQL expr) on]
