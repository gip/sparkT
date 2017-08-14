{-# LANGUAGE FlexibleContexts, DeriveTraversable #-}
module Database.SparkT.AST.ETL where

import Control.Monad.Except

import Data.Set.Monad as S
import Data.List as L
import Data.Foldable as F
import Data.Text (Text)

import Database.SparkT.AST.Internal
import Database.SparkT.AST.Database
import Database.SparkT.AST.SQL
import Database.SparkT.AST.Error

data DAGCtor = SDAG | SVertex | SArc | SStep
  deriving (Show)

-- A step
data Step a = Step { stepName :: String,
                                         stepProcess :: Insert a }
  deriving (Functor, Foldable)
instance Show (Step a) where
  show (Step name _) = "Step \"" ++ name ++ "\" <function>"
instance Ord (Step a) where
  compare (Step a _) (Step b _) = compare a b
instance Eq (Step a) where
  (==) (Step a _) (Step b _) = (==) a b
instance (Show a, ToScalaExpr a) => ToScalaExpr (Step a) where
  toSE (Step n i) = classCtor SStep [toSE n, toSE i]

-- ETLs as DAGs
newtype Vertex a = Vertex a
  deriving (Show, Eq, Ord, Functor, Foldable)
instance ToScalaExpr a => ToScalaExpr (Vertex a) where
  toSE (Vertex a) = classCtor SVertex [toSE a]

data Arc a = Arc { predecessors :: Set (Vertex a),
                   successor :: Vertex a,
                   process :: Step a }
  deriving (Show, Ord, Eq, Functor, Foldable)
instance (ToScalaExpr a, Show a, Ord a) => ToScalaExpr (Arc a) where
  toSE (Arc preds succ pro) = classCtor SArc [toSE preds, toSE succ, toSE pro]

-- A Directed Acyclic Graph representing an ETL
data DAG a = DAG { identifier :: String,
                   vertices :: Set (Vertex a),
                   arcs :: Set (Arc a) }
  deriving (Show, Functor, Foldable)
instance (ToScalaExpr a, Show a, Ord a) => ToScalaExpr (DAG a) where
  toSE (DAG n v a) = classCtor SDAG [toSE n, toSE v, toSE a]

-- From a list of ETLs create a DAG representation
computeDAG :: (Ord a, MonadError (Error String String) m) => String -> [Step a] -> m (DAG a)
computeDAG name steps = do
  (vertices, arcs) <- F.foldrM f (empty, empty) steps --
  catchCycle $ DAG name vertices arcs
  where
    f step@(Step name process) (v, a) =
      case succ' of Left e -> throwError e
                    Right succ ->
                      if S.member succ v
                        then throwError $ ImmutabilityViolationError name "only one arc going to a vertex is allowed"
                        else
                          return (S.insert succ v,
                          S.insert (Arc (S.fromList preds) succ step) a)
      where
        succ' = case process of Insert info _ _ _ _ -> Right $ Vertex info
        preds = case process of
                  Insert _ _ _ _ values -> F.foldr (\mapping acc -> Vertex mapping : acc) [] values
    catchCycle dag =
      let hasCycle = False in -- TODO: implement a function to catch cycles
      if hasCycle then throwError $ DAGCycleError name "cycle detected"
                  else return dag
