{-# LANGUAGE FlexibleContexts #-}
module Database.SparkT.AST.ETL where

import Data.Set as S
import Data.List as L
import Data.Foldable as F
import Data.Text (Text)

import Database.SparkT.AST.Internal
import Database.SparkT.AST.Database
import Database.SparkT.AST.SQL

data DAGCtor = SDAG | SVertex | SArc | SProcessingStep
  deriving (Show)

data ProcessingStep a = ProcessingStep { stepName :: String,
                                         stepProcess :: Insert a }
instance Show (ProcessingStep a) where
  show (ProcessingStep name _) = "ProcessingStep \"" ++ name ++ "\" <function>"
instance Ord (ProcessingStep a) where
  compare (ProcessingStep a _) (ProcessingStep b _) = compare a b
instance Eq (ProcessingStep a) where
  (==) (ProcessingStep a _) (ProcessingStep b _) = (==) a b
instance (Show a, ToScalaExpr a) => ToScalaExpr (ProcessingStep a) where
  toSE (ProcessingStep n i) = classCtor SProcessingStep [toSE n, toSE i]

-- ETLs as DAGs
data Vertex a = Vertex a
  deriving (Show, Eq, Ord)
instance ToScalaExpr a => ToScalaExpr (Vertex a) where
  toSE (Vertex a) = classCtor SVertex [toSE a]

data Arc a = Arc { predecessors :: Set (Vertex a),
                   successor :: Vertex a,
                   process :: ProcessingStep a }
  deriving (Show, Ord, Eq)
instance (ToScalaExpr a, Show a) => ToScalaExpr (Arc a) where
  toSE (Arc preds succ pro) = classCtor SArc [toSE preds, toSE succ, toSE pro]

-- A Directed Acyclic Graph representing an ETL
data DAG a = DAG { vertices :: Set (Vertex a),
                   arcs :: Set (Arc a) }
  deriving (Show)
instance (ToScalaExpr a, Show a) => ToScalaExpr (DAG a) where
  toSE (DAG v a) = classCtor SDAG [toSE v, toSE a]

-- From a list of ETLs create a representation of the DAG
computeDAG :: Ord a => [ProcessingStep a] -> DAG a
computeDAG steps = L.foldr f (DAG empty empty) steps
  where
    f step@(ProcessingStep name process) (DAG v a) = DAG (S.insert succ v) (S.insert (Arc (S.fromList preds) succ step) a)
      where
        succ = case process of Insert (Just info) _ _ _ _ -> Vertex info
                               _ -> error "Database info misssing"
        preds = case process of
                  Insert _ _ _ _ values -> F.foldr (\mapping acc -> Vertex mapping : acc) [] values
