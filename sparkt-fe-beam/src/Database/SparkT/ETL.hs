{-# LANGUAGE FlexibleContexts #-}
module Database.SparkT.ETL where

import Data.Set as S
import Data.List as L
import Data.Foldable as F
import Data.Text (Text)

import Database.SparkT.AST.Database
import Database.SparkT.AST

data ProcessingStep a = ProcessingStep { stepName :: String,
                                         stepProcess :: Versioned -> Insert a }
instance Show (ProcessingStep a) where
  show (ProcessingStep name _) = "ProcessingStep \"" ++ name ++ "\" <function>"
instance Ord (ProcessingStep a) where
  compare (ProcessingStep a _) (ProcessingStep b _) = compare a b
instance Eq (ProcessingStep a) where
  (==) (ProcessingStep a _) (ProcessingStep b _) = (==) a b

-- ETLs as DAGs
data Vertex a = Vertex a
  deriving (Show, Eq, Ord)

data Arc a = Arc { predecessors :: Set (Vertex a),
                   successor :: Vertex a,
                   process :: ProcessingStep a }
  deriving (Show, Ord, Eq)

data DAG a = DAG { vertices :: Set (Vertex a),
                   arcs :: Set (Arc a) }
  deriving (Show)

-- From a list of ETLs create a representation of the DAG
computeDAG :: Ord a => [ProcessingStep a] -> Versioned -> DAG a
computeDAG steps versioned = L.foldr f (DAG empty empty) steps
  where
    f step@(ProcessingStep name process) (DAG v a) = DAG (S.insert succ v) (S.insert (Arc (S.fromList preds) succ step) a)
      where
        succ = case process versioned of Insert (Just info) _ _ _ _ -> Vertex info
                                         _ -> error "Database info misssing"
        preds = case process versioned of
                  Insert _ _ _ _ values -> F.foldr (\mapping acc -> Vertex mapping : acc) [] values
