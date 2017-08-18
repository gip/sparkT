module Main where

import Control.Monad.Except

import Database.SparkT.AST as AST
import Database.SparkT.AST.Protocol
import Database.SparkT.AST.Database
import Database.SparkT.AST.ETL
import Database.SparkT.Client

import Example.Schemata
import Example.ETLs

main :: IO ()
main = do
  print $ ETLStatement 0 False dag
  executeSinglePhrase $ ETLStatement 0 False dag
  where Right dag = computeDAG "easyDAG" [downsampleStep (Versioned "WW40" 2)]
