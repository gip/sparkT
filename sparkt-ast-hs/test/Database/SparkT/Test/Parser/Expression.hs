{-# LANGUAGE RecordWildCards, NamedFieldPuns, PartialTypeSignatures #-}
module Database.SparkT.Test.Parser.Expression ( tests ) where

import Text.Parsec (parse, ParseError)
import Text.Parsec.Combinator (eof)
import Text.Parsec.Char

import Database.SparkT.AST.SQL
import Database.SparkT.Parser.SQL

import Data.Either

import Test.Tasty
import Test.Tasty.HUnit

tests :: TestTree
tests = testGroup "Expression parser"
                  [ operatorPrecedence
                  , valueEncoding
                  , cases ]

p :: String -> Either ParseError (Expression ())
p = parse (parseExpression <* eof) ""

n = Nothing

operatorPrecedence :: TestTree
operatorPrecedence = testGroup "Operator precedence"
  [ testCase "Mul over add" $ p "1+2*9" @?= p "1+(2*9)"
  , testCase "Low precedence for casts" $ p "1+2::Int" @?= p "CAST(1+2 AS int)"
  ]

valueEncoding :: TestTree
valueEncoding = testGroup "Value encoding"
  [
    testCase "Encoding null" $ p "NULL" @?= Right (ExpressionValue $ Value Null)
  , testCase "Encoding int" $ p "3" @?= Right (ExpressionValue $ Value (3::Integer))
  , testCase "Encoding string" $ p "'a string'" @?= Right (ExpressionValue $ Value ("a string" :: String))
  , testCase "Encoding double" $ p "3.14116" @?= Right (ExpressionValue $ Value (3.14116 :: Double))
  ]

-- https://stackoverflow.com/questions/4622/sql-case-statement-syntax
cases :: TestTree
cases = testGroup "Cases"
  [
    testCase "Correct semantic for simple vs searched"
        $ p "CASE y WHEN x THEN '1' WHEN z+1 THEN '2' ELSE '3' END"
      @?= p "CASE WHEN y=x THEN '1' WHEN y=z+1 THEN '2' ELSE '3' END"
  , testCase "Default value is null"
        $ p "CASE y WHEN x THEN '1' WHEN z*9.8 THEN '2' END"
      @?= p "CASE WHEN y=x THEN '1' WHEN y=z*9.8 THEN '2' ELSE null END"
  , testCase "At least one choice" $ assertBool "" $ isLeft $ p "CASE END"
  ]
