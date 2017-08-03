{-# LANGUAGE DeriveGeneric, GADTs, FlexibleContexts, OverloadedStrings,
             UndecidableInstances, TypeSynonymInstances, FlexibleInstances,
             ScopedTypeVariables #-}
module Database.SparkT.AST.Internal where

import Data.List
import Data.Set (Set, toList)
import Data.Text (Text)
import Data.Typeable
import Data.Char (toLower)

dQ = "\""

class ToScalaExpr a where
  toSE :: a -> String

data StandardScalaCtor = None | Some | Seq
  deriving (Show, Eq)

instance ToScalaExpr () where
  toSE () = "()"
instance ToScalaExpr a => ToScalaExpr [a] where
  toSE l = classCtor Seq (map toSE l)
instance ToScalaExpr a => ToScalaExpr (Set a) where
  toSE a = toSE (toList a)
instance ToScalaExpr a => ToScalaExpr (Maybe a) where
  toSE Nothing = classCtor None []
  toSE (Just a) = classCtor Some [toSE a]
instance ToScalaExpr Text where
  toSE = show
instance {-# OVERLAPPING #-} ToScalaExpr String where
  toSE = show
instance ToScalaExpr TypeRep where
  toSE a = "T" ++ show a ++ "()"
instance (ToScalaExpr a, ToScalaExpr b) => ToScalaExpr (a, b) where
  toSE (a,b)= concat ["(", toSE a, ",", toSE b, ")"]
instance (ToScalaExpr a, ToScalaExpr b, ToScalaExpr c) => ToScalaExpr (a, b, c) where
  toSE (a,b,c)= concat ["(", toSE a, ",", toSE b, ",", toSE c, ")"]
instance ToScalaExpr Integer where
  toSE = show
instance ToScalaExpr Bool where
  toSE = map toLower . show

classCtor :: Show a => a -> [String] -> String
classCtor name [] = if show name == "None" then "None" else (show name ++ "()")
classCtor name l = concat [show name, "(", intercalate "," l, ")"]
