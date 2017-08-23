{-# LANGUAGE FlexibleInstances, FlexibleContexts, OverloadedStrings,
             ScopedTypeVariables #-}
module Database.SparkT.Executor.Aggregation where

import Control.Monad.Identity
import Data.List as L
import Data.Ord as O

import Database.SparkT.AST.Database
import Database.SparkT.AST.SQL
import Database.SparkT.AST.Error
import Database.SparkT.Executor.Context

groupBy :: (Monad m, Ord r)
        => Frame m i r t    -- The aggregating frame
        -> Frame m i r t    -- The frame to aggregate
        -> Frame m i [r] t  -- Aggregated frame
groupBy frameBy frame =
  L.foldr (groupBy1) (L.map toAgg frame) frameBy
  where
    toAgg col = col { rImm = Nothing, rRepr = liftM return (rRepr col) }

groupBy1 :: (Monad m, Ord r)
        => Col m i r t
        -> Frame m i [r] t  -- Aggregated frame
        -> Frame m i [r] t  -- Aggregated frame
groupBy1 colBy frame = L.map (groupBy11 colBy) frame
  where
    groupBy11 colBy col =
      col { rImm = Nothing, rRepr = liftM2 groupIt (rRepr colBy) (rRepr col) }
    groupIt reprBy repr =
      L.concatMap (L.map (L.map snd) . L.groupBy eqIt . sortBy compareIt . zip reprBy) repr
    compareIt a b = compare (fst a) (fst b)
    eqIt a b = fst a == fst b
