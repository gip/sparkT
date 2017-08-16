{-# LANGUAGE FlexibleInstances, FlexibleContexts, UndecidableInstances #-}
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
  where doit _ [] = []
        doit n (x:xs) = if f x then (n, x):doit (n+1) xs
                               else doit (n+1) xs

-- Col -------------------------------------------------------------------------
-- TODO: should probably be Column
data Col m i r t = Col { rName      :: i,        -- colomn name
                         rScope     :: i,        -- scope
                         rType      :: t,        -- type
                         rNullable  :: Bool,     -- nullable
                         rRepr      :: m [r] }   -- representation

-- Frame -----------------------------------------------------------------------
-- TODO: enforce that the length of type and value lists are equal
type Frame m i r t = [Col m i r t]

instance (Eq i, Eq t, Eq r, Eq (m e [r])) => Eq (Col (m e) i r t) where
  (==) (Col i s t b r) (Col i' s' t' b' r') =
    (==) (i,s,t,b,r) (i',s',t',b',r')

instance (Show i, Show r, Show t, Show (m e [r])) => Show (Col (m e) i r t) where -- TODO: better!
  show (Col i s t b r) = concat ["Col ", show i, " ",
                                         show s, " ",
                                         show t, " ",
                                         show b, " ", show r]

-- DataFrame -------------------------------------------------------------------
data DataFrame m i r t = DataFrame [(i, i, t, Bool)] (m [[r]])

instance (Eq i, Eq t, Eq r, Eq (m [[r]])) => Eq (DataFrame m i r t) where
  (==) (DataFrame a b) (DataFrame a' b') =
    (==) (a, b) (a', b')

instance (Show i, Show r, Show t, Show (m [[r]])) => Show (DataFrame m i r t) where -- TODO: better!
  show (DataFrame a r) = concat ["DataFrame ", show a, " ", show r, " "]

-- There is no going back!
dataFrame :: Monad m => Frame m i r t -> m (DataFrame m i r t)
dataFrame frame = do
  l <- sequence $ L.map rRepr frame
  return $ DataFrame types (return $ L.map (take (doit l 10)) l)
  where
    types = L.map (\(Col i s t n _) -> (i, s, t, n)) frame
    doit l n =
      let n' = minimum (L.map length (return $ L.map (take n) l)) in
      if n' < n
        then n'
        else doit l (n*2)

-- Context ---------------------------------------------------------------------
type Context m i r t = Map i                  -- Table name
                             (Frame m i r t)  -- Frames

getColumn :: (Eq i, Show i, MonadError (Error i String) m1)
          => Frame m i r t -> i -> Maybe i -> ExceptT (Error i String) m1 (Col m i r t)
getColumn fr name scope =
  case filteri (\(Col n s _ _ _) -> n == name && scopeMatch scope s) fr of
    [] -> throwError $ UnreferencedColumnError name ""
    [(i,c)] -> return $ c
    cs -> throwError $ AmbiguousColumnError name "found in multiple scopes"
  where
    scopeMatch Nothing s = True
    scopeMatch (Just scope) s = scope==s

getColumnType (Col _ _ t _ _) = t

-- Context specialized used for typechecking and execution
type ContextTE m = Context m String Value EType

data EType = EString | EInt | EBool | EDouble
  deriving (Show, Eq)
