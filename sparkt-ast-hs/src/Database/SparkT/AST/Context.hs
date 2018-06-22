{-# LANGUAGE MultiParamTypeClasses #-}
module Database.SparkT.AST.Context where

type TableSchema t = [(String, t, Bool)]

class Contextable a m i r t where
  getTableSchema :: a m i r t -> i -> Maybe i -> Maybe (TableSchema t)
  getColumn :: a m i r t -> String -> m [r]
  setColumn :: a m i r t -> String -> m [r] -> a m i r t
