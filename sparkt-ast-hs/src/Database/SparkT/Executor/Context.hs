{-# LANGUAGE FlexibleInstances, FlexibleContexts,
             UndecidableInstances, DeriveFunctor #-}
module Database.SparkT.Executor.Context where

import Control.Monad
import Control.Monad.Except
import Data.Functor.Identity
import Data.Map as M
import Data.Foldable as F
import Data.List as L
import Data.String.Conv
import Data.Typeable

import GHC.Generics

import Database.SparkT.AST.Database
import Database.SparkT.AST.ETL
import Database.SparkT.AST.SQL
import Database.SparkT.AST.Error


-- Column ----------------------------------------------------------------------
data Col m i r t = Col { rName      :: i,        -- colomn name
                         rScope     :: i,        -- scope
                         rType      :: t,        -- type (may also include annotation)
                         rNullable  :: Bool,     -- nullable
                         rImm       :: Maybe r,  -- immediate value?
                         rRepr      :: m [r] }   -- representation
  deriving (Functor)
-- Frame -----------------------------------------------------------------------
-- TODO: enforce that the length of type and value lists are equal
type Frame m i r t = [Col m i r t]

instance (Eq i, Eq t, Eq r, Eq (m e [r])) => Eq (Col (m e) i r t) where
  (==) (Col i s t b _ r) (Col i' s' t' b' _ r') =
    (==) (i,s,t,b,r) (i',s',t',b',r')

instance (Show i, Show r, Show t, Show (m e [r])) => Show (Col (m e) i r t) where -- TODO: better!
  show (Col i s t b _ r) = concat ["Col ", show i, " ",
                                           show s, " ",
                                           show t, " ",
                                           show b, " ", show r]

getColumn :: (Eq i, Show i, MonadError (Error i String) m1)
          => Frame m i r t -> i -> Maybe i -> ExceptT (Error i String) m1 (Col m i r t)
getColumn fr name scope =
 case filteri (\(Col n s _ _ _ _) -> n == name && scopeMatch scope s) fr of
   [] -> throwError $ UnreferencedColumnError name ""
   [(i,c)] -> return c
   cs -> throwError $ AmbiguousColumnError name "found in multiple scopes"
 where
   scopeMatch Nothing s = True
   scopeMatch (Just scope) s = scope == s
   filteri f l = L.filter (f . snd) (zip [0..] l)
getColumnType (Col _ _ t _ _ _) = t

frameMapType f = L.map (\col -> col { rType = f (rType col) })

-- DataFrame -------------------------------------------------------------------
-- Easier to manipulate than frame but can't be converted back
data DataFrame m i r t = DataFrame [(i, i, t, Bool)] (m [[r]])

instance (Eq i, Eq t, Eq r, Eq (m [[r]])) => Eq (DataFrame m i r t) where
  (==) (DataFrame a b) (DataFrame a' b') =
    (==) (a, b) (a', b')

instance (Show i, Show r, Show t, Show (m [[r]])) => Show (DataFrame m i r t) where -- TODO: better!
  show (DataFrame a r) = concat ["DataFrame ", show a, " ", show r, " "]

dataFrame :: Monad m => Frame m i r t -> m (DataFrame m i r t)
dataFrame frame = do
  l <- mapM rRepr frame
  return $ DataFrame types (return $ L.map (L.take (doit l 10)) l)
  where
    types = L.map (\(Col i s t n _ _) -> (i, s, t, n)) frame
    doit l n =
      let n' = minimum (L.map (length. L.take n) l) in
      if n' < n
        then n'
        else doit l (n*2)

-- Context ---------------------------------------------------------------------
type Context m i r t = Map i                  -- Table name
                             (Frame m i r t)  -- Frames


-- Context specialized for typechecking and local execution --------------------
type ContextTE m = Context m String Value EType

data EType = EString | EInt | EBool | EDouble
  deriving (Show, Eq)

-- Conversion ------------------------------------------------------------------
toContextTE :: Monad m => DatabaseMapping TypeRep -> ContextTE m
toContextTE dbMapping = fromList $ L.map makeTbl (snd $ schema dbMapping)
  where
    makeTbl (tblName, tlbSchema) = (tblName, L.map makeCol tlbSchema)
    makeCol (name, typ, nullable) = Col name "" (eType typ) nullable Nothing (return [])
    eType typ
      | typ == typeRep (Proxy :: Proxy Int) = EInt
      | typ == typeRep (Proxy :: Proxy String) = EString
      | typ == typeRep (Proxy :: Proxy Double) = EDouble
