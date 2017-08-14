{-# LANGUAGE FlexibleInstances, FlexibleContexts #-}
module Database.SparkT.Executor.SQL (
      executeInsert
    , executeSelect
    , EType(..)
  ) where

import Control.Monad
import Control.Monad.Except

import Data.Typeable

import Database.SparkT.AST.Database
import Database.SparkT.AST.SQL
import Database.SparkT.Executor.Context

data EType = EString | EInt | EBool | EDouble
  deriving (Show, Eq)

mapEtype :: TypeRep -> EType
mapEtype dt | dt == typeOf (1::Int) = EInt
            | dt == typeOf (1::Integer) = EInt
            | dt == typeOf (True::Bool) = EBool
            | dt == typeOf (1.0::Double) = EDouble

type Context0 m = Context m String EType [Value]

executeInsert :: Insert (Context m String Value EType)
              -> ExceptT (TypeCheckingError String EType) m (Context m String EType [Value])
executeInsert = undefined

-- TODO: can we keep e?
executeSelect :: (MonadError (TypeCheckingError String EType) m)
              => Select (Context m String Value EType)
              -> ExceptT (TypeCheckingError String EType) m (Frame m String Value EType)
executeSelect = evalS

evalS (Select table ordering limit offset) = do
  frame <- evalST table
  return undefined

evalST (SelectTable projs from wher_ grouping having) =
  undefined

evalE :: Context0 m -> Expression a -> ExceptT (TypeCheckingError String EType) m (Row m String Value EType)
evalE = undefined
