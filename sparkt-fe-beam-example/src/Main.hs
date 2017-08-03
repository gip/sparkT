module Main where

import Database.SparkT.AST as AST
import Database.SparkT.AST.Protocol
import Database.SparkT.AST.Database
import Database.SparkT.AST.ETL
import Database.SparkT.Client

import Example.Databases
import Example.ETLs

main :: IO ()
main = do
  print $ ETLStatement 0 False dag
  executeSinglePhrase $ ETLStatement 0 False dag
  where dag = computeDAG [command (Versioned "WW40" 2)]
