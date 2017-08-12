{-# LANGUAGE OverloadedStrings #-}
module Database.SparkT.Parser.SQL where

import Prelude hiding (Ordering)

import Text.Parsec (parse, try)
import Text.Parsec.String (Parser)
import Text.Parsec.Char
import Text.Parsec.Combinator
import Text.Parsec.Expr
import Text.Parsec.Language
import qualified Text.Parsec.Token as P

import Control.Applicative ((<$>),(<*), (*>),(<*>), (<$), (<|>), many)
import Control.Monad
import Data.Functor
import Data.Char
import Data.Text (Text, pack)
import Data.Set (Set, fromList, notMember)

import Database.SparkT.AST.SQL
import Database.SparkT.Parser.Internal

-- Select
parseSelect :: Parser (Select a)
parseSelect =  Select <$> parseSelectTable
                      <*> parseOrder
                      <*> parseLimit
                      <*> parseOffset

parseSelectTable :: Parser (SelectTable a)
parseSelectTable = union
  where
    simple =
      keyword "SELECT" *> (SelectTable <$> parseProj
                                       <*> from
                                       <*> parseWhere
                                       <*> parseGrouping
                                       <*> pure Nothing) -- TODO: Having
    from = try (keyword "FROM" *> fmap Just parseFrom) <|> pure Nothing
    union = chainl1 simple $ do
      keyword "UNION"
      all <- option False (keyword "ALL" *> pure True)
      return $ UnionTables all

parseProj :: Parser (Projection a)
parseProj = ProjExprs <$> (proj `sepBy1` comma)
  where proj = (,) <$> parseExpression <*> optionMaybe name
        name = (keyword "AS" *> identifierText) <|> try identifierText

parseFrom :: Parser (From a)
parseFrom =  joins <|> try (parens parseFrom) <|> try fromTable
  where
    fromTable = FromTable <$> try parseTableSource <*> optionMaybe identifierText
    joins = try $ do
      source <- try (parens parseFrom) <|> fromTable
      joinType <- keywordList ["INNER", "OUTER", "LEFT", "RIGHT"]
      keyword "JOIN"
      source' <- try (parens parseFrom) <|> fromTable
      on <- optionMaybe (keyword "ON" *> parseExpression)
      case joinType of
        "INNER" -> return $ InnerJoin source source' on
        "OUTER" -> return $ OuterJoin source source' on
        "LEFT" -> return $ LeftJoin source source' on
        "RIGHT" -> return $ RightJoin source source' on

parseTableSource :: Parser (TableSource a)
parseTableSource = try subSelect <|> try named
  where
    named = TableNamed <$> pure Nothing <*> identifierTableText <*> pure []
    subSelect = TableFromSubSelect <$> parens parseSelect

parseWhere :: Parser (Maybe (Expression a))
parseWhere = optionMaybe (keyword "WHERE" *> parseExpression)

parseGrouping :: Parser (Maybe (Grouping a))
parseGrouping = optionMaybe (keyword "GROUP" *> keyword "BY" *> groups)
  where groups = Grouping <$> (parseExpression `sepBy1` comma)

parseLimit :: Parser (Maybe Integer)
parseLimit = optionMaybe (keyword "LIMIT" *> integer)

parseOffset :: Parser (Maybe Integer)
parseOffset = optionMaybe (keyword "OFFSET" *> integer)

parseOrder :: Parser [Ordering a]
parseOrder = option [] (keyword "ORDER" *> keyword "BY" *> order)
  where order = (try asc <|> try desc <|> try ascDefault) `sepBy1` comma
        asc = OrderingAsc <$> parseExpression <* keyword "ASC"
        desc = OrderingDesc <$> parseExpression <* keyword "DESC"
        ascDefault = OrderingAsc <$> parseExpression

-- Expressions
parseExpression' :: Parser (Expression a)
parseExpression' = parseExpressionNonLR

parseExpressionNonLR :: Parser (Expression a)
parseExpressionNonLR =  try $ parens parseExpression'
                    <|> try parseCase
                    <|> try parseCast
                    <|> try parseWindowCall
                    <|> try parseFunctionCall
                    <|> try parseExpressionFieldName
                    <|> try parseValue
                    <|> try parseExists

-- https://stackoverflow.com/questions/4622/sql-case-statement-syntax
parseCase :: Parser (Expression a)
parseCase = try $ do
  keyword "CASE"
  caseExpr <- (parseSearchedCase <|> parseSimpleCase)
  keyword "END"
  return caseExpr
  where
    parseWhen :: Parser (Expression a, Expression a)
    parseWhen = try $ do
      keyword "WHEN"
      wExpr <- parseExpression
      keyword "THEN"
      tExpr <- parseExpression
      return (wExpr, tExpr)
    parseElse = (keyword "ELSE" *> parseExpression) <|> pure (ExpressionValue $ Value Null)
    parseSearchedCase :: Parser (Expression a)
    parseSearchedCase = ExpressionCase <$> many1 parseWhen <*> parseElse
    parseSimpleCase :: Parser (Expression a)
    parseSimpleCase = do
      cExpr <- parseExpression
      wExprs <- many1 parseWhen
      eExpr <- parseElse
      return $ ExpressionCase (map (trans cExpr) wExprs) eExpr
      where trans cExpr (wExpr, tExpr) = (ExpressionCompOp "=" Nothing cExpr wExpr, tExpr)

parseExists :: Parser (Expression a)
parseExists = keyword "EXISTS" *> parens (ExpressionExists <$> parseSelect)

parseCast :: Parser (Expression a)
parseCast = try (keyword "CAST" *> parens doit)
  where
    doit = try $ do
      expr <- parseExpression
      keyword "AS"
      target <- parseDataType
      return $ ExpressionCast expr (CastTargetDataType target)

parseDataType :: Parser DataType
parseDataType =
      try (keyword "INT" *> pure DataTypeInteger)
  <|> try (keyword "STRING" *> (pure $ DataTypeChar True Nothing))
  <|> try (keyword "DATE" *> pure DataTypeDate)

parseFunctionCall :: Parser (Expression a)
parseFunctionCall = try $ do
  fn <- (:) <$> firstChar <*> many nonFirstChar
  callList <- parens (sepBy parseExpression comma)
  return $ ExpressionFunctionCall (ExpressionValue $ Value fn) callList
  where
    firstChar = letter <|> char '_'
    nonFirstChar = digit <|> firstChar

-- https://www.postgresql.org/docs/9.1/static/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS
parseWindowCall :: Parser (Expression a)
parseWindowCall = try $ do
  expr <- parseFunctionCall
  keyword "OVER"
  window <- parens parseWindowDef -- Only window definitions is supported
  return $ ExpressionWindow expr window
  where
    parseWindowDef :: Parser (Window a)
    parseWindowDef = try $ do
      keyword "PARTITION"
      keyword "BY"
      exprs <- parseExpression `sepBy1` comma
      order <- parseOrder
      frame_ <- optionMaybe frame
      return $ Window exprs order frame_
    frame = try $ do
      range <- try (keyword "RANGE" *> pure True) <|> (keyword "ROWS" *> pure False)
      (start, end) <- try frameBetween <|> ((,) <$> framePos <*> pure Nothing)
      return $ WindowFrame range start end
    frameBetween = do
      keyword "BETWEEN"
      start <- framePos
      keyword "AND"
      end <- framePos
      return (start, Just end)
    framePos =  try (keyword "UNBOUNDED" *> keyword "PRECEDING" *> pure WindowFrameUnboundedPreceding)
            <|> try ((WindowFramePreceding <$> integer) <* keyword "PRECEDING")
            <|> try (keyword "CURRENT" *> keyword "ROW" *> pure WindowFrameCurrentRow)
            <|> try (keyword "UNBOUNDED" *> keyword "FOLLOWING" *> pure WindowFrameUnboundedFollowing)
            <|> try ((WindowFrameFollowing <$> integer) <* keyword "FOLLOWING")

qualifiedName :: Parser FieldName
qualifiedName = lexeme doit
  where
    q = char '`'
    doit =   (char '*' *> pure (UnqualifiedField "*"))
         <|> qualified False
         <|> (between q q (qualified True))
         <|> (UnqualifiedField <$> identifierText)
    qualified quoted = try $ do
      scope <- (:) <$> firstChar <*> many nonFirstChar
      char '.'
      ident <- string "*" <|> (:) <$> firstChar <*> many nonFirstChar
      guard $ (quoted || notMember (map toUpper scope) reserved)
      guard $ (quoted || notMember (map toUpper ident) reserved)
      return $ QualifiedField (pack scope) (pack ident)
    firstChar = letter <|> char '_'
    nonFirstChar = digit <|> firstChar

parseExpressionFieldName :: Parser (Expression a)
parseExpressionFieldName = ExpressionFieldName <$> qualifiedName

-- Values
parseExpressionInt = ExpressionValue <$> (Value <$> integer)
parseExpressionDouble = ExpressionValue <$> (Value <$> float)
parseExpressionString = ExpressionValue <$> quoted
  where quoted = lexeme (q *> (Value <$> manyTill anyChar q))
        q = char '\''
parseExpressionNull = keyword "NULL" *> (pure $ ExpressionValue (Value Null))

parseValue =   try parseExpressionDouble
           <|> try parseExpressionInt
           <|> try parseExpressionString
           <|> try parseExpressionNull

parseExpression = buildExpressionParser table expressionOpTerm
expressionOpTerm = parens parseExpression <|> parseExpression'
table   = [ [prefix "+", prefix "-"]
          , [binary "*" AssocLeft, binary "/" AssocLeft, binary "%" AssocLeft]
          , [binary "+" AssocLeft, binary "-" AssocLeft ]
          , [postfix ["IS", "NOT", "NULL"] ExpressionIsNotNull, postfix ["IS", "NULL"] ExpressionIsNull,
             binaryL "LIKE" AssocLeft]
          ++ map (\c -> binaryComp c AssocLeft) [">=", "<=", "<>", "!=", "!>", "!<", "=", ">", "<"]
          , [postBetween]
          , [prefixL "NOT"]
          , [binaryL "AND" AssocLeft]
          , [binaryL "OR" AssocLeft]
          , [postCast]
          ]


binaryL name assoc = Infix (do { keyword name; return $ ExpressionBinOp (pack name) }) assoc
binary name assoc = Infix (do { try $ lexeme (ciString name); return $ ExpressionBinOp (pack name) }) assoc
binaryComp name assoc = Infix (do { try $ lexeme (string name); return $ ExpressionCompOp (pack name) Nothing }) assoc
prefix name = Prefix (do { try $ lexeme (ciString name); return $ ExpressionUnOp (pack name) })
prefixL name = Prefix (do { keyword name; notFollowedBy letter; return $ ExpressionUnOp (pack name) })
postfix names fun = Postfix (do { try $ mapM keyword names; return fun })
postCast = Postfix (do { try $ lexeme (string "::"); dt <- try parseDataType ;return $ \e -> ExpressionCast e (CastTargetDataType dt)})
postBetween = Postfix $ try $ do
  keyword "BETWEEN"
  eFrom <- try parseValue -- TODO: that should be parseExpression according to the SQL standard
  keyword "AND"           --         but that creates issue in parsing then (capturing the end of the expression)
  eTo <- try parseValue
  return $ \expr -> ExpressionBetween expr eFrom eTo
