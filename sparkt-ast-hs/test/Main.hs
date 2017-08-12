module Main where

import Test.Tasty

import qualified Database.SparkT.Test.Parser.Select as Select
import qualified Database.SparkT.Test.Parser.Expression as Expression

main :: IO ()
main = defaultMain (testGroup "sparkt-ast tests"
                              [ Select.tests,
                                Expression.tests ])
