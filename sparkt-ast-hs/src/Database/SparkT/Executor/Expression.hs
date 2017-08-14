{-# LANGUAGE FlexibleInstances, FlexibleContexts, OverloadedStrings #-}
module Database.SparkT.Executor.Expression where

import Control.Monad
import Control.Monad.Except

import Data.Typeable
import Data.String.Conv
import Data.Map as M
import Data.List as L

import Database.SparkT.AST.Database
import Database.SparkT.AST.SQL
import Database.SparkT.AST.Error
import Database.SparkT.Executor.Context


evalE frame (ExpressionFieldName (QualifiedField scope name)) =
  getColumn frame (toS name) (Just $ toS scope)
evalE frame (ExpressionFieldName (UnqualifiedField name)) =
  getColumn frame (toS name) Nothing

evalE _ expr = throwError $ ExecutorNotImplementedError "evalE" (show expr)
