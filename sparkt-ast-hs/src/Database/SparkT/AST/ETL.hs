{-# LANGUAGE FlexibleContexts, DeriveTraversable #-}
module Database.SparkT.AST.ETL where

import Data.Set.Monad as S
import Data.List as L
import Data.Foldable as F
import Data.Text (Text)

import Database.SparkT.AST.Internal
import Database.SparkT.AST.Database
import Database.SparkT.AST.SQL

data DAGCtor = SDAG | SVertex | SArc | SProcessingStep
  deriving (Show)

-- A step
data ProcessingStep a = ProcessingStep { stepName :: String,
                                         stepProcess :: Insert a }
  deriving (Functor, Foldable)
instance Show (ProcessingStep a) where
  show (ProcessingStep name _) = "ProcessingStep \"" ++ name ++ "\" <function>"
instance Ord (ProcessingStep a) where
  compare (ProcessingStep a _) (ProcessingStep b _) = compare a b
instance Eq (ProcessingStep a) where
  (==) (ProcessingStep a _) (ProcessingStep b _) = (==) a b
instance (Show a, ToScalaExpr a) => ToScalaExpr (ProcessingStep a) where
  toSE (ProcessingStep n i) = classCtor SProcessingStep [toSE n, toSE i]

-- ETLs as DAGs
newtype Vertex a = Vertex a
  deriving (Show, Eq, Ord, Functor, Foldable)
instance ToScalaExpr a => ToScalaExpr (Vertex a) where
  toSE (Vertex a) = classCtor SVertex [toSE a]

data Arc a = Arc { predecessors :: Set (Vertex a),
                   successor :: Vertex a,
                   process :: ProcessingStep a }
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
computeDAG :: Ord a => String -> [ProcessingStep a] -> DAG a
computeDAG name steps = DAG name vertices arcs
  where
    (vertices, arcs) = L.foldr f (empty, empty) steps
    f step@(ProcessingStep name process) (v, a) = (S.insert succ v, S.insert (Arc (S.fromList preds) succ step) a)
      where
        succ = case process of Insert (Just info) _ _ _ _ -> Vertex info
                               _ -> error "Context missing"
        preds = case process of
                  Insert _ _ _ _ values -> F.foldr (\mapping acc -> Vertex mapping : acc) [] values
