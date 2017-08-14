{-# LANGUAGE RecordWildCards, NamedFieldPuns, PartialTypeSignatures #-}
module Database.SparkT.Test.Executor.Select ( tests ) where

import Control.Monad
import Control.Monad.Except
import Data.Functor.Identity

import Text.Parsec (parse, ParseError)
import Text.Parsec.Char

import Data.Map
import Data.List as L
import Data.Either

import Database.SparkT.AST.Database
import Database.SparkT.AST.SQL
import Database.SparkT.Parser.SQL
import Database.SparkT.Executor.Context
import Database.SparkT.Executor.SQL

import Test.Tasty
import Test.Tasty.HUnit

tests :: TestTree
tests = testGroup "SQL Select executor"
                  [ simple ]

p :: String -> Either ParseError (Select ())
p = parse (parseSelect <* char ';') ""

simpleCtx :: Monad m => Context m String Value EType
simpleCtx =
  fromList [("simpleDB",
      fromList [("table1", Frame [("id", EInt, False),
                                  ("name", EString, True)]
                            (return $ L.map toV [(1, "Ut")
                                                ,(2, "Resonare")
                                                ,(3, "Mira")
                                                ,(4, "Famuli")
                                                ,(5, "Solve")
                                                ,(6, "Labii")
                                                ,(7, "Sancte")])
                )]
            )]

  where toV (i :: Integer, s :: String) = [Value i, Value s]

contextualize :: Context m String Value EType -> Select () -> Select (Context m String Value EType)
contextualize ctx select = fmap (\_ -> ctx) select

fromRight (Right a) = a

run :: ExceptT e Identity a -> Either e a
run = runIdentity . runExceptT

simple :: TestTree
simple = testGroup "A very simple example"
  [
    testCase "Simple select with where clause" $
      run (executeSelect (contextualize simpleCtx $ fromRight (p "SELECT name AS name0 FROM tables1 WHERE id>6;")))
        @?= Right (Frame [("name0", EString, True)] (return [[Value ("Sancte"::String)]]))
  , testCase "This should not typecheck" $
      run (executeSelect (contextualize simpleCtx $ fromRight (p "SELECT name+id FROM tables1;")))
        @?= Left (ExpressionTypeMismatchError "Projection clause" EString EInt)
  ]
