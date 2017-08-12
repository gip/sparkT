module Database.SparkT.Parser.Internal where

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

-- Postgres reserved words
reserved = fromList ["ALL", "ANALYSE", "ANALYZE", "AND", "ANY",
            "ARRAY", "AS", "ASC", "ASYMMETRIC", "BETWEEN",
            "BINARY", "BOTH", "BY", "CASE", "CAST", "CHECK",
            "COLLATE", "COLUMN", "CONSTRAINT", "CREATE",
            "CROSS", "CURRENT_DATE", "CURRENT_ROLE",
            "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
            "DEFAULT", "DEFERRABLE", "DESC", "DISTINCT", "DO",
            "ELSE", "END", "EXCEPT", "EXISTS", "FALSE", "FOR", "FOREIGN", "FREEZE",
            "FROM", "FULL", "GRANT", "GROUP", "HAVING", "ILIKE",
            "IN", "INITIALLY", "INNER", "INTERSECT", "INTO",
            "IS", "ISNULL", "JOIN", "LEADING", "LEFT", "LIKE",
            "LIMIT", "LOCALTIME", "LOCALTIMESTAMP", "NATURAL",
            "NEW", "NOT", "NOTNULL", "NULL", "OFF", "OFFSET",
            "OLD", "ON", "ONLY", "OR", "ORDER", "OUTER", "OVERLAPS",
            "PLACING", "PRIMARY", "REFERENCES", "RIGHT", "SELECT",
            "SESSION_USER", "SIMILAR", "SOME", "SYMMETRIC",
            "TABLE", "THEN", "TO", "TRAILING", "TRUE", "UNION", "UNIQUE",
            "USER", "USING", "VERBOSE", "WHEN", "WHERE"]

keyword :: String -> Parser ()
keyword = void . p
  where p k = try $ do
          i <- lexeme (many letter)
          guard (map toUpper i == map toUpper k)
          return k

keywordList :: [String] -> Parser String
keywordList l = try $ do
          w <- lexeme (many letter)
          let word = map toUpper w
          guard (elem word l)
          return word

-- TODO: move the whitespace parser at the tokenization level otherwise error
--       messages are not helpful
lexeme :: Parser a -> Parser a
lexeme p = p <* whitespace

identifier :: Parser String
identifier = try quoted <|> try direct
  where
    q = char '`'
    direct = do
      ident <- lexeme ((:) <$> firstChar <*> many nonFirstChar)
      guard $ notMember (map toUpper ident) reserved
      return ident
    quoted = lexeme (between q q ((:) <$> firstChar <*> many nonFirstChar))
    firstChar = letter <|> char '_'
    nonFirstChar = digit <|> firstChar

identifierText :: Parser Text
identifierText = fmap pack identifier

identifierTableText :: Parser Text
identifierTableText = fmap pack (try (lexeme qualified) <|> lexeme simple)
  where
    simple = (:) <$> firstChar <*> many nonFirstChar
    qualified = do
      qual <- simple
      char '.'
      name <- simple
      return $ concat [qual, ".", name]
    firstChar = letter <|> char '_'
    nonFirstChar = digit <|> firstChar


whitespace :: Parser ()
whitespace =
  choice [simpleWhitespace *> whitespace
         ,lineComment *> whitespace
         ,blockComment *> whitespace
         ,return ()]
  where
    lineComment = try (string "--") *> manyTill anyChar (void (char '\n') <|> eof)
    blockComment = try (string "/*") *> manyTill anyChar (try $ string "*/")
    simpleWhitespace = void $ many1 (oneOf " \t\n")

integer :: Parser Integer
integer = do
  n <- lexeme $ many1 digit
  return $ read n

float :: Parser Double
float = lexeme $ P.float (P.makeTokenParser emptyDef)

comma :: Parser Char
comma = lexeme $ char ','

parens :: Parser a -> Parser a
parens = between (lexeme $ char '(') (lexeme $ char ')')

quotedString :: Parser String
quotedString =
  lexeme $ between (char '\'') (char '\'') (many anyChar)

-- Like string but match is case insensitive
ciString s = try (mapM ciChar s)
  where
    ciChar :: Char -> Parser Char
    ciChar c = char (toLower c) <|> char (toUpper c)
