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
import Database.SparkT.AST.Error
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
  fromList [("table1", simpleFrame)]

simpleFrame :: Monad m => Frame m String Value EType
simpleFrame = makeFrame [("id", EInt, False), ("name", EString, True)]
                            (L.map toV [(1, "Ut")
                                       ,(2, "Resonare")
                                       ,(3, "Mira")
                                       ,(4, "Famuli")
                                       ,(5, "Solve")
                                       ,(6, "Labii")
                                       ,(7, "Sancte")])
  where toV (i :: Integer, s :: String) = [Value i, Value s]

makeFrame h v = make h (transpose v)
  where make [] [] = []
        make ((na, ty, nu):hs) (v:vs) = Row na "" ty nu (return v) : make hs vs
        make _ _ = error "wrong input"

contextualize :: Context m String Value EType -> Select () -> Select (Context m String Value EType)
contextualize ctx select = fmap (\_ -> ctx) select

fromRight (Right a) = a

run = join . runExceptT

simple :: TestTree
simple = testGroup "A very simple example"
  [
    testCase "No clause" $
      run (executeSelect (contextualize simpleCtx $ fromRight (p "SELECT * FROM table1;")))
        @?= Right simpleFrame

  , testCase "Limit clause" $
      run (executeSelect (contextualize simpleCtx $ fromRight (p "SELECT * FROM table1 LIMIT 2;")))
        @?= Right (L.map (\r -> r { rRepr = liftM (take 2) $ rRepr r}) simpleFrame)

  , testCase "Offset clause" $
      run (executeSelect (contextualize simpleCtx $ fromRight (p "SELECT * FROM table1 OFFSET 2;")))
        @?= Right (L.map (\r -> r { rRepr = liftM (drop 2) $ rRepr r}) simpleFrame)

  , testCase "Offset and limit clause" $
      run (executeSelect (contextualize simpleCtx $ fromRight (p "SELECT * FROM table1 LIMIT 1 OFFSET 5;")))
        @?= Right [Row "id" "" EInt False (return [Value (6::Integer)]),
                   Row "name" "" EString True (return [Value ("Labii"::String)])]

  , testCase "Simple projection" $
      run (executeSelect (contextualize simpleCtx $ fromRight (p "SELECT id FROM table1 OFFSET 5;")))
        @?= Right [Row "id" "" EInt False (return [Value (6::Integer), Value (7::Integer)])]

  -- , testCase "Simple select with where clause" $
  --     run (executeSelect (contextualize simpleCtx $ fromRight (p "SELECT name AS name0 FROM table1 WHERE id>6;")))
  --       @?= Right [Row "name0" "" EString True (return [Value ("Labii"::String)])]
  --
  -- , testCase "This should not typecheck" $
  --     run (executeSelect (contextualize simpleCtx $ fromRight (p "SELECT name+id FROM table1;")))
  --       @?= Left (ExpressionTypeMismatchError "Projection clause" $ L.concat [show EString, " vs ", show EInt])
  ]
