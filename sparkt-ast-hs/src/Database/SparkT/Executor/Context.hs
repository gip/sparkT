{-# LANGUAGE FlexibleInstances, FlexibleContexts #-}
module Database.SparkT.Executor.Context where

import Control.Monad
import Control.Monad.Except
import Data.Functor.Identity
import Data.Map as M
import Data.Foldable as F
import Data.List as L
import Data.String.Conv

import GHC.Generics

import Database.SparkT.AST.Database
import Database.SparkT.AST.ETL
import Database.SparkT.AST.SQL
import Database.SparkT.AST.Error

filteri :: (a -> Bool) -> [a] -> [(Int, a)]
filteri f l = doit 0 l
  where doit n (x:xs) = if f x then (n, x):doit (n+1) xs
                               else doit (n+1) xs

data Row m i r t = Row { rName      :: i,        -- colomn name
                         rScope     :: i,        -- scope
                         rType      :: t,        -- type
                         rNullable  :: Bool,     -- nullable
                         rRepr      :: m [r] }   -- representation

-- TODO: enforce that the length of type and value lists are equal
type Frame m i r t = [Row m i r t]

instance (Eq i, Eq t, Eq r) => Eq (Row Identity i r t) where
  (==) (Row i s t b r) (Row i' s' t' b' r') =
    (==) (i,s,t,b,runIdentity r) (i',s',t',b',runIdentity r')
instance (Show i, Show r, Show t) => Show (Row m i r t) where -- TODO: better!
  show (Row i s t b _) = concat ["Row ", show i, " ",
                                         show s, " ",
                                         show t, " ",
                                         show b, " <values>"]

type Context m i r t = Map i                  -- Table name
                             (Frame m i r t)  -- Frames

getColumn :: (Eq i, Show i, MonadError (Error i String) m1)
          => Frame m i r t -> i -> Maybe i -> ExceptT (Error i String) m1 (Row m i r t)
getColumn fr name scope =
  case filteri (\(Row n s _ _ _) ->  n==name && scopeMatch scope s) fr of
    [] -> throwError $ UnreferencedColumnError name ""
    [(i,c)] -> return $ c
    cs -> throwError $ AmbiguousColumnError name "found in multiple scopes"
  where
    scopeMatch Nothing s = True
    scopeMatch (Just scope) s = scope==s
