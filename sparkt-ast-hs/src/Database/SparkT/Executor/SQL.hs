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
import Data.List as L hiding (groupBy)
import Data.Text (Text)
import Data.Ord as Ord

import Database.SparkT.AST.Database
import Database.SparkT.AST.SQL
import Database.SparkT.AST.Error
import Database.SparkT.Executor.Context
import Database.SparkT.Executor.Expression
import Database.SparkT.Executor.Aggregation

import System.IO.Unsafe

executeSelect :: forall m. (MonadError (Error String String) m,
                            Show (Col m String Value EType),
                            Eq (Col m String Value EType))
              => Select (ContextTE m)
              -> ExceptT (Error String String) m (Frame m String Value (EType))
executeSelect = evalS

-- Select ----------------------------------------------------------------------
evalS (Select table ordering limit offset) = do
  (frame, frame0) <- evalST table
  frameOrd <- if L.null ordering then return frame
                                 else do ordExpr <- mapM (orderBy frame0) ordering
                                         return $ applyOrderBy ordExpr frame
  frameOff <- case offset of Nothing -> return frameOrd
                             Just off -> applyRepr (drop $ fromIntegral off) frameOrd
  case limit of Nothing -> return frameOff
                Just lim -> applyRepr (take $ fromIntegral lim) frameOff
  where
    applyRepr f frame = return $ L.map (\row -> row { rRepr = liftM f (rRepr row) }) frame
    -- There must must be a way to find a simpler way to implement ORDER BY!
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
                                   Nothing -> return ("", [])
  frameW <- case mWhere of Nothing -> return frameF
                           Just wher_ -> do
                             rowCond <- evalE frameF wher_
                             if fst (rType rowCond) == EBool
                               then return $ applyWhere rowCond frameF
                               else throwError $ ExpressionTypeMismatchError "Bool" ""
  (frameGB, frameP) <- case mGroupBy of Nothing -> do -- No GROUP BY clause
                                          frameP <- makeProjection frameW projs
                                          return (frameW, frameMapType fst frameP)
                                        Just (Grouping exprs) -> do -- GROUP BY
                                          frameBy <- mapM (groupByCol exprs frameW) exprs
                                          let field "" n = ExpressionFieldName (UnqualifiedField (toS n))
                                              field s n = ExpressionFieldName (QualifiedField (toS s) (toS n))
                                          let frameW' = L.map (\col ->
                                                col { rType = (rType col, field (rScope col) (rName col)) }) frameW
                                          let frameGrouped = groupBy frameBy frameW'
                                          frameGP <- makeProjectionGrouped frameBy frameGrouped projs
                                          return (frameMapType fst frameGP, frameMapType fst frameGP)
  if isJust mFrom then return (frameP, frameGB) -- Can't truncate columns here so some may be infinite lists
                  else return (truncFrame 1 frameP, truncFrame 1 frameGB)
  where
    truncFrame n f = L.map (\col -> col { rRepr = liftM (take n) (rRepr col) }) f
    applyWhere rowB frame =
      L.map (\row -> row { rRepr = liftM2 filterB (rRepr rowB) (rRepr row) }) frame
    filterB lB l = L.map snd $ L.filter (forceAsBool . fst) (zip lB l)
    groupByCol projs frame expr = do
      colGroup <- evalE frame expr
      case rImm colGroup of Just v ->
                              do ii <- getAsInt v
                                 let i = (fromIntegral ii)-1
                                 if i>=0 && i<= length projs
                                   then evalE frame (projs !! i)
                                   else throwError $ PositionNotInListGroupByError "evalST" (show i)
                            Nothing -> return colGroup

evalST _ = throwError $ ExecutorNotImplementedError "evalST" "SelectTable"

-- Projection and aggregation
makeProjectionGrouped :: (MonadError (Error String String) m, Show a, Eq a)
                      => Frame m String Value (EType, Expression a)   -- The frame used for aggregation
                      -> Frame m String [Value] (EType, Expression a) -- The aggregated frame
                      -> [(Expression a, Maybe Text)]
                      -> ExceptT (Error String String) m (Frame m String Value (EType, Expression a))
makeProjectionGrouped _ _ [] = return []
makeProjectionGrouped frameBy frame ((expr, asM):projs) = do
  cols <- makeProjectionGrouped frameBy frame projs
  case expr of
    ExpressionFieldName (UnqualifiedField "*") -> throwError $ ExecutorNotImplementedError "* selection with group by clause" ""
    ExpressionFieldName (QualifiedField _ "*") -> throwError $ ExecutorNotImplementedError "* selection with group by clause" ""
    _ -> do
      case L.filter (\col -> eqExpr (snd (rType col)) expr) frame of
        [col] -> do
          let col' = col { rRepr = liftM (L.map head) (rRepr col), rImm = Nothing }
          case asM of Just alias -> return $ col' { rScope= "", rName = toS alias }:cols
                      Nothing -> return $ col' : cols
        _ -> throwError $ ExecutorNotImplementedError "aggregation functions on group by" ""

eqExpr (ExpressionFieldName (UnqualifiedField n))
       (ExpressionFieldName (UnqualifiedField n')) = n == n'
eqExpr (ExpressionFieldName (QualifiedField s n))
       (ExpressionFieldName (UnqualifiedField n')) = n == n'
eqExpr (ExpressionFieldName (UnqualifiedField n'))
       (ExpressionFieldName (QualifiedField s n)) = n == n'
eqExpr (ExpressionFieldName (QualifiedField s' n'))
       (ExpressionFieldName (QualifiedField s n)) = (s, n) == (s', n')
eqExpr e e' = e == e'

-- Projection without aggregation
makeProjection :: (MonadError (Error String String) m, Show a)
               => Frame m String Value EType
               -> [(Expression a, Maybe Text)]
               -> ExceptT (Error String String) m (Frame m String Value (EType, Expression a))
makeProjection _ [] = return []
makeProjection frame ((expr, asM):projs) = do
  cols <- makeProjection frame projs
  case expr of
    ExpressionFieldName (UnqualifiedField "*") ->
      if isJust asM then throwError $ AliasOnStarProjectionError "evalF" "FromTable"
                    else return $ frameMapType (\t -> (t, expr)) frame ++ cols
    ExpressionFieldName (QualifiedField scope "*") ->
      if isJust asM then throwError $ AliasOnStarProjectionError "evalF" "FromTable"
                    else return $ frameMapType (\t -> (t, expr))
                                        (L.filter (\r -> rScope r == toS scope) frame) ++ cols
    _ -> do
      col <- evalE frame expr
      case asM of Just alias -> return (col { rScope= "", rName = toS alias }:cols)
                  Nothing -> return (col:cols)

-- FromTable -------------------------------------------------------------------
evalF (FromTable source mScope) = do
  (scope0, frame) <- evalTS source
  let scope = fromMaybe scope0 mScope
  return $ (scope, L.map (\r -> r { rScope = toS scope }) frame)
evalF _ = throwError $ ExecutorNotImplementedError "evalF" "FromTable"


-- TableSource -----------------------------------------------------------------
evalTS e@(TableNamed ctx name _) = do
  case M.lookup (toS name) ctx of
    Nothing -> throwError $ UnreferencedTableError (toS name) "algorithm error - should never happen"
    Just fr -> return (name, fr)
evalTS _ = throwError $ ExecutorNotImplementedError "evalF" "TableSource"
