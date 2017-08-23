{-# LANGUAGE OverloadedStrings, ScopedTypeVariables, FlexibleInstances #-}
module Database.SparkT.Client.Inspect where

import Control.Monad

import Data.Set (toList)

import Database.SparkT.AST.Database
import Database.SparkT.AST.ETL
import Database.SparkT.Builder.SQL
import Database.SparkT.Executor.Context

class Inspectable a where
  inspect :: a -> IO ()

instance Inspectable (ETL (DatabaseMapping a)) where
  inspect (DAG identifier _ arcs) = do
    putStrLn $ concat ["ETL -> id -> ", identifier]
    forM_ arcs $ \arc -> do
      putStrLn $ concat ["       arc -> id -> ", stepName $ process arc]
      putStrLn $ concat ["              sql -> ", toSQL $ step $ process arc]
      putStrLn $ concat ["              successor -> ", table $ unVertex $ successor arc, " (path ", url $ unVertex $ successor arc, ")"]
      forM_ (predecessors arc) $ \pred -> do
        putStrLn $ concat ["              predecessor -> ", table $ unVertex $ pred, " (path ", url $ unVertex $ pred,")"]

-- instance Inspectable (ETL (ContextTE m)) where
--   inspect (DAG identifier _ arcs) = do
--     putStrLn $ concat ["ETL -> id -> ", identifier]
--     forM_ arcs $ \arc -> do
--       putStrLn $ concat ["       arc -> id -> ", stepName $ process arc]
--       putStrLn $ concat ["              sql -> ", toSQL $ step $ process arc]
--       putStrLn $ concat ["              successor -> ", table $ unVertex $ successor arc]
--       forM_ (predecessors arc) $ \pred -> do
--         putStrLn $ concat ["              predecessor -> ", table $ unVertex $ pred]

help = putStrLn "I shall help you"
