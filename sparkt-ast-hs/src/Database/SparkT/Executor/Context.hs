{-# LANGUAGE FlexibleInstances #-}
module Database.SparkT.Executor.Context where

import Control.Monad
import Control.Monad.Except
import Data.Functor.Identity
import Data.Map as M
import Data.Foldable as F
import Data.Proxy

import GHC.Generics

import Database.SparkT.AST.Database
import Database.SparkT.AST.ETL
import Database.SparkT.AST.SQL

data TypeCheckingError a t =
    ExpressionTypeMismatchError a t t
  | UnionColumnMatchError a
  -- More to be added
  deriving (Eq, Show)

data Row m i r t = Row (i, t, Bool) (m [r])

-- TODO: enforce that the length of type and value lists are equal
data Frame m i r t = Frame [(i, t, Bool)] (m [[r]]) -- Should a Map be used?

instance Eq (Frame Identity i r t) where
  (==) = (==)
instance Show (Frame Identity i r t) where
  show = show

type Context m i r t = Map i (Map i (Frame m i r t)) -- Should we use maps?

frameRows :: Monad m => [Row m i r t] -> Frame m i r t
frameRows [] = Frame [] (return [])
frameRows (Row c mr:rows) = Frame (c:c0) $ liftM2 (:) mr mr0
  where
    Frame c0 mr0 = frameRows rows
