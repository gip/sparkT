module Database.SparkT.AST.Error where

data Error a =
    ImmutabilityViolationError a a
  | MissingContextError a a
  | DAGCycleError a a
  deriving (Show)
