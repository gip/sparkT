module Main where

import Test.Tasty

import qualified Database.SparkT.Test.Parser.Expression as Expression
import qualified Database.SparkT.Test.Parser.Select as Select
import qualified Database.SparkT.Test.Executor.Select as SelectE

main :: IO ()
main = defaultMain (testGroup "AST tests"
                              [ Select.tests,
                                Expression.tests,
                                SelectE.tests ])
