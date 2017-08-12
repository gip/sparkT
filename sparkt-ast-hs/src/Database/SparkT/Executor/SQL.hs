module Database.SparkT.Executor.SQL where

import Control.Monad
import Control.Monad.Except

import Data.Typeable

import Database.SparkT.AST.Database
import Database.SparkT.AST.SQL
import Database.SparkT.Executor.Context

data EType = EString | EInt | EBool | EDouble
  deriving (Show, Eq)

mapEtype :: TypeRep -> EType
mapEtype dt | dt == typeOf (1::Int) = EInt
            | dt == typeOf (1::Integer) = EInt
            | dt == typeOf (True::Bool) = EBool
            | dt == typeOf (1.0::Double) = EDouble

executeInsert :: Insert (Context m String Value EType)
              -> ExceptT (TypeCheckingError String EType) m (Context m String EType [Value])
executeInsert = undefined

executeSelect :: Select (Context m String Value EType)
              -> ExceptT (TypeCheckingError String EType) m (Frame m String Value EType)
executeSelect = undefined
