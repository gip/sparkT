module Main where

import Control.Monad.Except
import System.Environment
import Data.List

import Database.SparkT.AST.Protocol
import Database.SparkT.AST.Database
import Database.SparkT.AST.ETL
import Database.SparkT.Client.Websocket

import Example.Schemata
import Example.ETLs

main :: IO ()
main = do
  args <- getArgs
  case args of
    [batch, rev] ->
      case acmeETLWithData (Versioned batch (read rev)) of
        Left _ -> putStrLn "ETL creation failed"
        Right etl -> do
          executeSinglePhrase $ ETLStatement 0 True (dropPrefix etl)
    _ -> putStrLn "Usage: sparkt-fe-beam-example <bath> <revision>"

dropPrefix etl =
  fmap (\ctx -> ctx { url = (case stripPrefix "s3://myprefix" (url ctx) of Just p -> ("./data" ++ p))  } ) etl
