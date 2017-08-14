{-# LANGUAGE FlexibleInstances, FlexibleContexts, OverloadedStrings #-}
module Database.SparkT.Executor.SQL (
      executeInsert
    , executeSelect
    , EType(..)
    , evalS
  ) where

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

data EType = EString | EInt | EBool | EDouble
  deriving (Show, Eq)

mapEtype :: TypeRep -> EType
mapEtype dt | dt == typeOf (1::Int) = EInt
            | dt == typeOf (1::Integer) = EInt
            | dt == typeOf (True::Bool) = EBool
            | dt == typeOf (1.0::Double) = EDouble

-- Context for typechecking and execution
type ContextTE m = Context m String Value EType

executeInsert :: Insert (ContextTE m)
              -> ExceptT (Error String String) m (ContextTE m)
executeInsert = undefined

executeSelect :: Monad m -- TODO: understand why constraining with a MonadError breaks the tests
              => Select (ContextTE m)
              -> ExceptT (Error String String) m (Frame m String Value EType)
executeSelect = evalS


evalS (Select table ordering limit offset) = do
  frame <- evalST table
  -- TODO: ordering
  withOffset <- case offset of Nothing -> return frame
                               Just off -> applyRepr (drop $ fromIntegral off) frame
  case limit of Nothing -> return withOffset
                Just lim -> applyRepr (take $ fromIntegral lim) withOffset
  where
    applyRepr f frame = return $ L.map (\row -> row { rRepr = liftM f (rRepr row) }) frame

evalST (SelectTable (ProjExprs [(ExpressionFieldName (UnqualifiedField "*"), Nothing)])
                    mFrom Nothing Nothing Nothing) = do
  frame <- case mFrom of Just from -> evalF from
                         Nothing -> return ([] :: Frame m String Value EType)
  return frame
evalST _ = throwError $ ExecutorNotImplementedError "evalST" "SelectTable"

evalF (FromTable source mScope) = do
  frame <- evalTS source
  -- TODO: add scope if exists
  return frame
evalF _ = throwError $ ExecutorNotImplementedError "evalF" "FromTable"

evalTS (TableNamed ctx name _) = do
  case M.lookup (toS name) ctx of
    Nothing -> throwError $ UnreferencedTableError (toS name) "algorithm error - should never happen"
    Just fr -> return fr
evalTS _ = throwError $ ExecutorNotImplementedError "evalF" "TableSource"

evalE :: ContextTE m
      -> Expression a
      -> ExceptT (Error String String) m (Frame m String Value EType)
evalE = undefined
