{-# LANGUAGE DeriveGeneric, TypeSynonymInstances, FlexibleInstances, TypeFamilies,
    FlexibleContexts, ScopedTypeVariables, PartialTypeSignatures,
    GADTs, MultiParamTypeClasses, OverloadedStrings, AllowAmbiguousTypes,
    TypeOperators, DefaultSignatures #-}

module Database.SparkT.AST( module Database.SparkT.ASTBase ) where

import Database.Beam hiding (C)
import Database.Beam.Backend.SQL
import Database.Beam.Backend.SQL.Builder

import Data.ByteString.Builder
import Data.String.Conv
import Data.Proxy
import Data.Data
import Data.Aeson
import GHC.Generics

import Database.SparkT.ASTBase

-- Instance not in beam currently
-- Probably because encoding doubles in SQL is backend-depedent
instance HasSqlValueSyntax SqlSyntaxBuilder Double where
  sqlValueSyntax x = SqlSyntaxBuilder $
    byteString (toS (show x))
