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

valueD d = Value (d::Double)
valueI i = Value (i::Integer)
valueS s = Value (s::String)

simpleFrame :: Monad m => String -> Frame m String Value EType
simpleFrame n = makeFrame n [("id", EInt, False), ("name", EString, True)]
                                   (L.map toV [(1, "Ut")
                                              ,(2, "Resonare")
                                              ,(3, "Mira")
                                              ,(4, "Famuli")
                                              ,(5, "Solve")
                                              ,(6, "Labii")
                                              ,(7, "Sancte")])
  where toV (i :: Integer, s :: String) = [Value i, Value s]

simpleCtx :: Monad m => Context m String Value EType
simpleCtx =
  fromList [("table1", simpleFrame "table1")]

easyFrame :: Monad m => String -> Frame m String Value EType
easyFrame n = makeFrame n [("id", EInt, False), ("name", EString, True), ("score", EDouble, False)]
                                   (L.map toV [(1, "Do", 344.67)
                                              ,(2, "Re", 23.01)
                                              ,(3, "Re", 0.1)
                                              ,(4, "Fa", 0.1)
                                              ,(5, "Fa", 9.8)
                                              ,(6, "Fa", 0.1)
                                              ,(7, "Si", 4.0)])
  where toV (i :: Integer, s :: String, d :: Double) = [Value i, Value s, Value d]

easyCtx :: Monad m => Context m String Value EType
easyCtx =
  fromList [("table1", easyFrame "table1")]

makeFrame n h v = make h (transpose v)
  where make [] [] = []
        make ((na, ty, nu):hs) (v:vs) = Col na n ty nu Nothing (return v) : make hs vs
        make _ _ = error "wrong input"

contextualize :: Context m String Value EType -> Select () -> Select (Context m String Value EType)
contextualize ctx select = fmap (\_ -> ctx) select

fromRight (Right a) = a

run = join . runExceptT

runDF a = join $ mapM dataFrame $ run a

simple :: TestTree
simple = testGroup "Simple queries"
  [
    testCase "No from clause" $
    run (executeSelect (contextualize simpleCtx $ fromRight (p "SELECT 1+2;")))
      @?= Right [Col "?" "" EInt False Nothing (return [Value (3::Integer)])]

  , testCase "Only a from clause" $
      run (executeSelect (contextualize simpleCtx $ fromRight (p "SELECT * FROM table1;")))
        @?= Right (simpleFrame "table1")

  , testCase "Limit clause" $
      run (executeSelect (contextualize simpleCtx $ fromRight (p "SELECT * FROM table1 LIMIT 2;")))
        @?= Right (L.map (\r -> r { rRepr = liftM (take 2) $ rRepr r}) (simpleFrame "table1"))

  , testCase "Offset clause" $
      run (executeSelect (contextualize simpleCtx $ fromRight (p "SELECT * FROM table1 OFFSET 2;")))
        @?= Right (L.map (\r -> r { rRepr = liftM (drop 2) $ rRepr r}) (simpleFrame "table1"))

  , testCase "Offset and limit clause" $
      run (executeSelect (contextualize simpleCtx $ fromRight (p "SELECT * FROM table1 LIMIT 1 OFFSET 5;")))
        @?= Right [Col "id" "table1" EInt False Nothing (return [Value (6::Integer)]),
                   Col "name" "table1" EString True Nothing (return [Value ("Labii"::String)])]

  , testCase "Simple projection" $
      run (executeSelect (contextualize simpleCtx $ fromRight (p "SELECT id FROM table1 OFFSET 5;")))
        @?= Right [Col "id" "table1" EInt False Nothing (return [Value (6::Integer), Value (7::Integer)])]

  , testCase "Complex projection" $
      runDF (executeSelect (contextualize simpleCtx $ fromRight (p "SELECT 0 ZERO, id+id*2 newcol FROM table1 OFFSET 5;")))
        @?= Right (DataFrame [("ZERO","",EInt,False),("newcol","",EInt,False)]
                             (return [[Value (0::Integer),Value (0::Integer)],[Value (18::Integer),Value (21::Integer)]]) )

  , testCase "Simple select with where clause 1/2" $
      run (executeSelect (contextualize simpleCtx $ fromRight (p "SELECT name AS name0 FROM table1 WHERE id=6;")))
        @?= Right [Col "name0" "" EString True Nothing (return [Value ("Labii"::String)])]

  , testCase "Simple select with where clause 2/2" $
      run (executeSelect (contextualize simpleCtx $ fromRight (p "SELECT name FROM table1 WHERE id>3 AND id<=6 AND id<>5;")))
        @?= Right [Col "name" "table1" EString True Nothing (return [Value ("Famuli"::String),Value ("Labii"::String)])]

  , testCase "This should not typecheck" $
      run (executeSelect (contextualize simpleCtx $ fromRight (p "SELECT name+id FROM table1;")))
        @?= Left (ExpressionTypeMismatchError "EString" "EInt")

  , testCase "Order by asc" $
      runDF (executeSelect (contextualize easyCtx $ fromRight (p "SELECT score, id FROM table1 ORDER BY score;")))
        @?= Right (DataFrame [("score","table1",EDouble,False),
                             ("id","table1",EInt,False)]
                            (return [[valueD 0.1,valueD 0.1,valueD 0.1,valueD 4.0,valueD 9.8,valueD 23.01,valueD 344.67],
                                     [valueI 3,valueI 4,valueI 6,valueI 7,valueI 5,valueI 2,valueI 1]]) )

   , testCase "Order by desc" $
       runDF (executeSelect (contextualize easyCtx $ fromRight (p "SELECT score, id FROM table1 ORDER BY score DESC;")))
         @?= Right (DataFrame [("score","table1",EDouble,False),
                              ("id","table1",EInt,False)]
                             (return [reverse [valueD 0.1,valueD 0.1,valueD 0.1,valueD 4.0,valueD 9.8,valueD 23.01,valueD 344.67],
                                              [valueI 1,valueI 2,valueI 5,valueI 7,valueI 3,valueI 4,valueI 6]]) )

    , testCase "Order by asc and then desc" $
        runDF (executeSelect (contextualize easyCtx $ fromRight (p "SELECT name, score, id FROM table1 ORDER BY name ASC, score DESC;")))
          @?= Right (DataFrame [("name","table1",EString,True),("score","table1",EDouble,False),("id","table1",EInt,False)]
                              (return [[valueS "Do",valueS "Fa",valueS "Fa",valueS "Fa",valueS "Re",valueS "Re",valueS "Si"],
                                       [valueD 344.67,valueD 9.8,valueD 0.1,valueD 0.1,valueD 23.01,valueD 0.1,valueD 4.0],
                                       [valueI 1,valueI 5,valueI 4,valueI 6,valueI 2,valueI 3,valueI 7]]))

    , testCase "Order by asc and then asc" $
        runDF (executeSelect (contextualize easyCtx $ fromRight (p "SELECT name, score, id FROM table1 ORDER BY name ASC, score ASC;")))
          @?= Right (DataFrame [("name","table1",EString,True),("score","table1",EDouble,False),("id","table1",EInt,False)]
                              (return [[valueS "Do",valueS "Fa",valueS "Fa",valueS "Fa",valueS "Re",valueS "Re",valueS "Si"],
                                       [valueD 344.67,valueD 0.1,valueD 0.1,valueD 9.8,valueD 0.1,valueD 23.01,valueD 4.0],
                                       [valueI 1,valueI 4,valueI 6,valueI 5,valueI 3,valueI 2,valueI 8]]))
  ]
