module Database.SparkT.AST.Error where

data Error a b =
    ImmutabilityViolationError a b
  | MissingContextError a b
  | DAGCycleError a b

  | ExpressionTypeMismatchError a b
  | UnionColumnMatchError a b
  | UnreferencedColumnError a b
  | AmbiguousColumnError a b
  | UnreferencedTableError a b
  | AliasOnStarProjectionError a b

  | ExecutorNotImplementedError a b
  deriving (Show, Eq)
