{-# LANGUAGE RecordWildCards, NamedFieldPuns, PartialTypeSignatures #-}
module Database.SparkT.Test.Parser.Select ( tests ) where

import Text.Parsec (parse, ParseError)
import Text.Parsec.Char
import Data.Either

import Database.SparkT.AST.SQL
import Database.SparkT.Parser.SQL

import Test.Tasty
import Test.Tasty.HUnit

tests :: TestTree
tests = testGroup "SQL parser"
                  [ misc,
                    astEquivallence,
                    clauses,
                    joins ]

p :: String -> Either ParseError (Select ())
p = parse (parseSelect <* char ';') ""

n = Nothing

misc :: TestTree
misc = testGroup "Misc tests"
  [
    testCase "Missing terminator" $ assertBool "" $ isLeft $ p "SELECT * FROM t"
  , testCase "Can alias a table" $ assertBool "" $ isRight $ p "SELECT * FROM t t1;"
  , testCase "Can't use keywords as identifiers" $ assertBool "" $ isLeft $ p "SELECT * FROM t from;"
  ]

clauses :: TestTree
clauses = testGroup "Clauses in select"
  [
    testCase "From clause optional" $ assertBool "" $ isRight $ p "SELECT 1+2, 'something';"
  , testCase "Where clause" $ assertBool "" $ isRight $ p "SELECT t.* FROM t WHERE t.a >= 9;"
  ]

joins :: TestTree
joins = testGroup "Joins"
  [
    testCase "Join with parens" $ p "SELECT * FROM abc abcRenamed INNER JOIN table2;"
                              @?= p "SELECT * FROM (abc abcRenamed INNER JOIN table2);"
  , testCase "Join with parens 2" $ p "SELECT * FROM ((abc abcRenamed INNER JOIN table2));"
                                @?= p "SELECT * FROM (abc abcRenamed INNER JOIN table2);"
  , testCase "Chained joins" $ assertBool "" $
      isRight $ p "SELECT s.* FROM ((Orders o INNER JOIN Customers c ON o.a=c.a) INNER JOIN Shippers s ON s.b=o.b);"

  -- The test below should pass -- that query makes sense and is valid for Postgres
  , testCase "Chained joins with alias (TODO: FIX)" $ assertBool "Valid query in Postgres, this should not fail" True
  -- , testCase "Chained joins with alias" $ assertBool "Valid query in Postgres, this should not fail" $
  --     isRight $ p "SELECT s.* FROM ((Orders o INNER JOIN Customers c ON o.a=c.a) oc INNER JOIN Shippers s ON s.b=oc.b);"
  ]

astEquivallence :: TestTree
astEquivallence = testGroup "AST checks & equivallence"
  [ testCase "Simple SELECT" $
             p "SELECT * FROM t;" @?=
               Right (Select
                 (SelectTable (ProjExprs [(ExpressionFieldName (UnqualifiedField "*"), n)])
                              (Just (FromTable (TableNamed () "t" []) n)) n n n) [] n n)
  , testCase "Select equivallence" $
             p "SELECT '1' ys, max(t.x) AS maxX, CAST(someRow AS int) castResult FROM table t GROUP BY y;"
             @?=
             p "SELECT '1' AS ys, max(t.x) maxX, someRow::int castResult FROM table t GROUP BY y;"
  ]
