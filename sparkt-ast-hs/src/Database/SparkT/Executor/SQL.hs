{-# LANGUAGE FlexibleInstances, FlexibleContexts, OverloadedStrings #-}
module Database.SparkT.Executor.SQL (
      executeSelect
    , EType(..)
    , evalS
  ) where

import Control.Monad
import Control.Monad.Except

import Data.Typeable
import Data.String.Conv
import Data.Maybe (isJust)
import Data.Map as M
import Data.List as L
import Data.Text (Text)

import Database.SparkT.AST.Database
import Database.SparkT.AST.SQL
import Database.SparkT.AST.Error
import Database.SparkT.Executor.Context
import Database.SparkT.Executor.Expression


executeSelect :: (MonadError (Error String String) m, Show (Row m String Value EType)) -- TODO: why is this show needed?
              => Select (ContextTE m)
              -> ExceptT (Error String String) m (Frame m String Value EType)
executeSelect = evalS


-- Select ----------------------------------------------------------------------
evalS (Select table ordering limit offset) = do
  frame <- evalST table
  -- TODO: ordering
  withOffset <- case offset of Nothing -> return frame
                               Just off -> applyRepr (drop $ fromIntegral off) frame
  case limit of Nothing -> return withOffset
                Just lim -> applyRepr (take $ fromIntegral lim) withOffset
  where
    applyRepr f frame = return $ L.map (\row -> row { rRepr = liftM f (rRepr row) }) frame


-- SelectTable -----------------------------------------------------------------
evalST (SelectTable (ProjExprs projs)
                    mFrom Nothing Nothing Nothing) = do
  (scope, frame) <- case mFrom of Just from -> evalF from
                                  Nothing -> return ("", [] :: Frame m String Value EType)
  frame' <- makeProjection frame projs
  return frame'
evalST _ = throwError $ ExecutorNotImplementedError "evalST" "SelectTable"

-- makeProjection :: MonadError (Error String String) m10 => Frame m String Value EType -> [(Expression t, Maybe Text)] -> ExceptT
--                     (Error String String) m10 (Frame m String Value EType)
makeProjection _ [] = return []
makeProjection frame ((expr, asM):projs) = do
  rows <- makeProjection frame projs
  case expr of
    ExpressionFieldName (UnqualifiedField "*") ->
      if isJust asM then throwError $ AliasOnStarProjectionError "evalF" "FromTable"
                    else return $ frame ++ rows
    ExpressionFieldName (QualifiedField scope "*") ->
      if isJust asM then throwError $ AliasOnStarProjectionError "evalF" "FromTable"
                    else return $ (L.filter (\r -> rScope r == toS scope) frame) ++ rows
    _ -> do
      row <- evalE frame expr
      case asM of Just alias -> return (row { rName = toS alias }:rows)
                  Nothing -> return (row:rows)

-- FromTable -------------------------------------------------------------------
evalF (FromTable source mScope) = do
  (scope, frame) <- evalTS source
  case mScope of Nothing -> return (scope, frame)
                 Just scope -> return (scope, L.map (\r -> r { rScope = toS scope }) frame)
evalF _ = throwError $ ExecutorNotImplementedError "evalF" "FromTable"


-- TableSource -----------------------------------------------------------------
evalTS (TableNamed ctx name _) = do
  case M.lookup (toS name) ctx of
    Nothing -> throwError $ UnreferencedTableError (toS name) "algorithm error - should never happen"
    Just fr -> return (name, fr)
evalTS _ = throwError $ ExecutorNotImplementedError "evalF" "TableSource"
