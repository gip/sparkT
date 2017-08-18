{-# LANGUAGE FlexibleInstances, FlexibleContexts, OverloadedStrings, ScopedTypeVariables #-}
module Database.SparkT.Executor.SQL (
      executeSelect
    , EType(..)
    , evalS
  ) where

import Control.Monad
import Control.Monad.Except

import Data.Typeable
import Data.String.Conv
import Data.Maybe (isJust, fromMaybe)
import Data.Map as M
import Data.List as L
import Data.Text (Text)
import Data.Ord as Ord

import Database.SparkT.AST.Database
import Database.SparkT.AST.SQL
import Database.SparkT.AST.Error
import Database.SparkT.Executor.Context
import Database.SparkT.Executor.Expression


import System.IO.Unsafe

executeSelect :: forall m. (MonadError (Error String String) m, Show (Col m String Value EType))
              => Select (ContextTE m)
              -> ExceptT (Error String String) m (Frame m String Value EType)
executeSelect = evalS


-- Select ----------------------------------------------------------------------
evalS (Select table ordering limit offset) = do
  (frame, frame0) <- evalST table
  -- TODO: fix ordering - it doesn't really work in case there is more than one ordering
  frameOrd <- if L.null ordering then return frame
                                 else do ordExpr <- mapM (orderBy frame0) ordering
                                         return $ applyOrderBy ordExpr frame
  frameOff <- case offset of Nothing -> return frameOrd
                             Just off -> applyRepr (drop $ fromIntegral off) frameOrd
  case limit of Nothing -> return frameOff
                Just lim -> applyRepr (take $ fromIntegral lim) frameOff
  where
    applyRepr f frame = return $ L.map (\row -> row { rRepr = liftM f (rRepr row) }) frame
    orderBy frame (OrderingAsc e) = evalE frame e >>= \col -> return (True, col)
    orderBy frame (OrderingDesc e) = evalE frame e >>= \col -> return (False, col)
    applyOrderBy1 frameBy col =
      col { rRepr = liftM2 sortIt colBy (rRepr col) }
      where
        f :: Monad m => (Bool, Col m i r t) -> m [(Bool, r)]
        f (b, col) = rRepr col >>= \repr -> return $ L.map ((,) b) repr
        -- colBy :: m [[(Bool, Value)]]
        colBy = liftM transpose $ sequence $ L.map f frameBy
        compareIt :: ([(Bool, Value)], Value) -> ([(Bool, Value)], Value) -> Ord.Ordering
        compareIt ([(ascLhs, vLhs)], _) ([(ascRhs, vRhs)], _) =
          if ascLhs then compare vLhs vRhs else compare vRhs vLhs
        compareIt ((ascLhs, vLhs):rLhs, dataLhs) ((ascRhs, vRhs):rRhs, dataRhs) =
          case if ascLhs then compare vLhs vRhs else compare vRhs vLhs of
            EQ -> compareIt (rLhs, dataLhs) (rRhs, dataRhs)
            order -> order
        sortIt :: [[(Bool, Value)]] -> [Value] -> [Value]
        sortIt a b = L.map snd $ sortBy compareIt (zip a b)
    applyOrderBy frameBy frame =
      L.map (applyOrderBy1 frameBy) frame

-- SelectTable -----------------------------------------------------------------
evalST (SelectTable (ProjExprs projs)
                    mFrom
                    mWhere   -- where
                    mGroupBy -- grouping
                    Nothing) = do
  (scope, frameF) <- case mFrom of Just from -> evalF from
                                   Nothing -> return ("", [] :: Frame m String Value EType)
  frameW <- case mWhere of Nothing -> return frameF
                           Just wher_ -> do
                             rowCond <- evalE frameF wher_
                             if rType rowCond == EBool
                               then return $ applyWhere rowCond frameF
                               else throwError $ ExpressionTypeMismatchError "Bool" ""
  frameGB <- case mGroupBy of Nothing -> return frameW
                              Just (Grouping exprs) -> do
                                groupCols <- mapM (groupByCol frameW) exprs
                                -- TODO: it's actually a bit complicated :)
                                throwError $ ExecutorNotImplementedError "GROUP BY" ""
                                return frameW -- TODO
  frameP <- makeProjection frameGB projs
  if isJust mFrom then return (frameP, frameGB) -- Can't truncate columns here so some may be infinite lists
                  else return (truncFrame 1 frameP, truncFrame 1 frameGB)
  where
    truncFrame n f = L.map (\row -> row { rRepr = liftM (take n) (rRepr row) }) f
    applyWhere :: Monad m => Col m i Value t -> Frame m i Value t -> Frame m i Value t
    applyWhere rowB frame =
      L.map (\row -> row { rRepr = liftM2 filterB (rRepr rowB) (rRepr row) }) frame
    filterB lB l = L.map snd $ L.filter (forceAsBool . fst) (zip lB l)
    groupByCol frame expr = do
      colGroup <- evalE frame expr
      case rImm colGroup of Just v ->
                              do ii <- getAsInt v
                                 let i = fromIntegral ii
                                 if i>0 && i<= length frame
                                   then return $ frame !! i
                                   else throwError $ PositionNotInListGroupByError "evalST" (show i)
                            Nothing -> throwError $ NonIntegerConstantGroupByError "evalST" ""

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
      case asM of Just alias -> return (row { rScope= "", rName = toS alias }:rows)
                  Nothing -> return (row:rows)

-- FromTable -------------------------------------------------------------------
evalF (FromTable source mScope) = do
  (scope0, frame) <- evalTS source
  let scope = fromMaybe scope0 mScope
  return $ (scope, L.map (\r -> r { rScope = toS scope }) frame)
evalF _ = throwError $ ExecutorNotImplementedError "evalF" "FromTable"


-- TableSource -----------------------------------------------------------------
evalTS (TableNamed ctx name _) = do
  case M.lookup (toS name) ctx of
    Nothing -> throwError $ UnreferencedTableError (toS name) "algorithm error - should never happen"
    Just fr -> return (name, fr)
evalTS _ = throwError $ ExecutorNotImplementedError "evalF" "TableSource"
