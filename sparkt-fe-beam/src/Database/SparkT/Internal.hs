{-# LANGUAGE DeriveGeneric, TypeSynonymInstances, FlexibleInstances, TypeFamilies,
    FlexibleContexts, ScopedTypeVariables, PartialTypeSignatures,
    GADTs, MultiParamTypeClasses, OverloadedStrings, AllowAmbiguousTypes,
    TypeOperators, DefaultSignatures #-}

module Database.SparkT.Internal where

import Data.String.Conv
import Data.Aeson
import Data.Data

instance ToJSON TypeRep where
  toJSON = String . toS . show
