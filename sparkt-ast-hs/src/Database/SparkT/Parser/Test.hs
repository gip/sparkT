{-# LANGUAGE OverloadedStrings #-}
module Database.SparkT.Parser.Test where

import Text.Parsec (parse, try)
import Text.Parsec.String (Parser)
import Text.Parsec.Char
import Text.Parsec.Combinator
import Text.Parsec.Expr
import Text.Parsec.Language
import qualified Text.Parsec.Token as P
import Text.ParserCombinators.Parsec.Prim

import Control.Applicative ((<$>),(<*), (*>),(<*>), (<$), (<|>), many)
import Control.Monad
import Data.Functor
import Data.Char
import Data.Text (Text, pack)
import Data.Set (Set, fromList, notMember)

import Database.SparkT.AST.SQL
import Database.SparkT.Parser.SQL

-- Testing it
selects = [
 "SELECT * FROM (t ttt INNER JOIN tt ON 1=1) u ;"

 ,"SELECT func('gillou') AS luc, CAST(someRow AS int) castResult FROM abc abcRenamed;"
 ,"SELECT func('gillou') luc, someRow::int castResult FROM abc abcRenamed;"
 ,"SELECT gillou AS luc, EEE FROM abc abcRenamed;"
 ,"SELECT gillou.annie AS luc, EEE FROM abc abcRenamed;"
 ,"SELECT `gillou.annie` AS luc, EEE FROM abc abcRenamed;"
 ,"SELECT max(moi) over (partition by toi order by yyy ASC rows between 4 preceding and 5 following) FROM abc abcRenamed;"
 ,"SELECT a+2*b::Int my_field FROM abc abcRenamed INNER JOIN table2;"
 ,"SELECT CAST( a+(2*b) AS int) AS my_field FROM (abc abcRenamed INNER JOIN table2);"
 ,"SELECT EEE, EEE FROM ((abc abcRenamed INNER JOIN table2));"
 ,"SELECT EEE, EEE FROM abc t1 INNER JOIN table2 t2 ON EEE;"
 ,"SELECT EEE, EEE FROM (abc t1 INNER JOIN table2 t2 ON EEE);"
 ,"SELECT EEE, EEE FROM abc t1 INNER JOIN (table2 t2 OUTER JOIN table3 t3);"
 ,"SELECT eee FROM (Orders b INNER JOIN Customers a ON b.val=a.val);"


 ,"SELECT s.* FROM ((Orders o INNER JOIN Customers c ON o.a=c.a) INNER JOIN Shippers s ON s.b=oc.b);"
 ,"SELECT s.* FROM (Orders o INNER JOIN Customers c ON o.a=c.a INNER JOIN Shippers s ON s.b=oc.b);"

 ,"SELECT 'gillou' AS luc, CAST(someRow AS int) + 'abc' castResult FROM abc abcRenamed;"
 ,"SELECT 'gillou' IS NULL FROM abc abcRenamed;"
 ,"SELECT 89=abcRenamed.column1 FROM abc abcRenamed;"
 ,"SELECT numtransactions_rolling_8weeks*1/Least(Member_Tenure/7,8) :: int AvgWeeklyTrx_8w_rolling FROM abc abcRenamed WHERE EXISTS (SELECT a FROM table0 t0);"

 ,"SELECT CASE a WHEN b THEN c ELSE d END FROM tbl;"
 ,"SELECT CASE WHEN a=b THEN c ELSE d END FROM tbl;"
 ,"SELECT c.*, a::date FROM tbl;"
 ,"SELECT column_name(s) FROM table1 UNION SELECT column_name(s) FROM table2;"
 ,"SELECT column_name(s) FROM (SELECT column_name(s) FROM table2) UNION (SELECT column_name(s) FROM table2);"
 ]
selectsAst = map (\sel -> (sel, parse (parseSelect <* char ';') "" sel)) selects

test = mapM_ f selectsAst
  where
    f (sel, ast) = case ast of
              Right _ -> putStr "PASS     " >> putStrLn sel
              Left _ -> putStr "FAIL ### " >> putStrLn sel


aSql = parseFromFile (parseSelect <* char ';') "./a.sql"
bSql = parseFromFile (parseSelect <* char ';') "./b.sql"
cSql = parseFromFile (parseSelect <* char ';') "./c.sql"
