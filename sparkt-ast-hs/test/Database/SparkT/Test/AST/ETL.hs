{-# LANGUAGE RecordWildCards, NamedFieldPuns, PartialTypeSignatures #-}
module Database.SparkT.Test.AST.ETL ( tests ) where

import Text.Parsec (parse, ParseError)
import Text.Parsec.Combinator (eof)
import Text.Parsec.Char

import Database.SparkT.AST.SQL
import Database.SparkT.AST.ETL

import Data.Either

import Test.Tasty
import Test.Tasty.HUnit

tests :: TestTree
tests = testGroup "ETL"
                  []
