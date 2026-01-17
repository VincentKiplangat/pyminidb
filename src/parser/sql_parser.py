"""
SQL Parser - Converts SQL tokens into executable commands.
"""
from typing import List, Dict, Any
from src.storage.page import PageType

# from pyminidb.src.catalog.schema import ColumnConstraint, DataType
from ..catalog.schema import ColumnConstraint, DataType

from .lexer import SQLLexer, TokenType, Token
from .ast_nodes import *

class SQLParser:
    """Parses SQL tokens into abstract syntax tree (AST)."""

    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.position = 0
        self.current_token = self.tokens[0] if tokens else None

    def parse(self) -> ASTNode:
        """Parse tokens into an AST node."""
        if not self.tokens:
            raise SyntaxError("No tokens to parse")

        if self.current_token.type == TokenType.CREATE:
            return self.parse_create()
        elif self.current_token.type == TokenType.INSERT:
            return self.parse_insert()
        elif self.current_token.type == TokenType.SELECT:
            return self.parse_select()
        elif self.current_token.type == TokenType.DELETE:
            return self.parse_delete()
        elif self.current_token.type == TokenType.UPDATE:
            return self.parse_update()
        else:
            raise SyntaxError(f"Unsupported statement type: {self.current_token.type}")

    # ---------------- CREATE TABLE ----------------
    def parse_create(self) -> CreateTableStatement:
        self.advance()  # Skip CREATE
        self.expect(TokenType.TABLE, "Expected TABLE after CREATE")
        table_name = self.expect(TokenType.IDENTIFIER, "Expected table name").value
        self.expect(TokenType.LPAREN, "Expected '(' after table name")

        columns = []
        while self.current_token.type != TokenType.RPAREN:
            col_def = self.parse_column_definition()
            columns.append(col_def)

            if self.current_token.type == TokenType.COMMA:
                self.advance()
            elif self.current_token.type != TokenType.RPAREN:
                raise SyntaxError("Expected ',' or ')' in column definitions")

        self.advance()  # Skip ')'
        self.expect(TokenType.SEMICOLON, "Expected ';' after CREATE TABLE statement")

        return CreateTableStatement(table_name, columns)

    def parse_column_definition(self) -> ColumnDefinition:
        """Parse a single column definition."""
        col_name = self.expect(TokenType.IDENTIFIER, "Expected column name").value

        # Accept data type tokens
        if self.current_token.type in [TokenType.INTEGER, TokenType.VARCHAR, TokenType.TEXT, TokenType.FLOAT]:
            data_type_token = self.current_token
            self.advance()
            data_type = DataType[data_type_token.value.upper()]
        else:
            raise SyntaxError(f"Expected data type. Found: {self.current_token.type}")

        # Optional length for VARCHAR
        length = None
        if data_type == DataType.VARCHAR:
            if self.current_token.type == TokenType.LPAREN:
                self.advance()
                num_token = self.expect(TokenType.NUMBER, "Expected length for VARCHAR")
                length = int(num_token.value)
                self.expect(TokenType.RPAREN, "Expected ')' after VARCHAR length")

        # Column constraints
        constraints = []
        while self.current_token.type in [TokenType.PRIMARY, TokenType.UNIQUE, TokenType.NOT, TokenType.NULL]:
            if self.current_token.type == TokenType.PRIMARY:
                self.advance()
                self.expect(TokenType.KEY, "Expected KEY after PRIMARY")
                constraints.append(ColumnConstraint.PRIMARY_KEY)
            elif self.current_token.type == TokenType.UNIQUE:
                self.advance()
                constraints.append(ColumnConstraint.UNIQUE)
            elif self.current_token.type == TokenType.NOT:
                self.advance()
                self.expect(TokenType.NULL, "Expected NULL after NOT")
                constraints.append(ColumnConstraint.NOT_NULL)
            else:
                self.advance()  # NULL without NOT

            if self.current_token.type == TokenType.COMMA:
                break  # Next column

        return ColumnDefinition(col_name, data_type, constraints, length)

    # ---------------- INSERT ----------------
    def parse_insert(self) -> InsertStatement:
        self.advance()  # Skip INSERT
        self.expect(TokenType.INTO, "Expected INTO after INSERT")
        table_name = self.expect(TokenType.IDENTIFIER, "Expected table name").value

        # Optional column list
        columns = []
        if self.current_token.type == TokenType.LPAREN:
            self.advance()
            while self.current_token.type != TokenType.RPAREN:
                col_name = self.expect(TokenType.IDENTIFIER, "Expected column name").value
                columns.append(col_name)
                if self.current_token.type == TokenType.COMMA:
                    self.advance()
            self.advance()  # Skip ')'

        self.expect(TokenType.VALUES, "Expected VALUES")
        self.expect(TokenType.LPAREN, "Expected '(' after VALUES")

        values = []
        while self.current_token.type != TokenType.RPAREN:
            if self.current_token.type == TokenType.NUMBER:
                values.append(int(self.current_token.value))
            elif self.current_token.type == TokenType.STRING:
                values.append(self.current_token.value)
            elif self.current_token.type == TokenType.NULL:
                values.append(None)
            else:
                raise SyntaxError(f"Unexpected value type: {self.current_token.type}")

            self.advance()
            if self.current_token.type == TokenType.COMMA:
                self.advance()

        self.advance()  # Skip ')'
        self.expect(TokenType.SEMICOLON, "Expected ';' after INSERT statement")

        return InsertStatement(table_name, columns, values)

    # ---------------- SELECT ----------------
    def parse_select(self) -> SelectStatement:
        self.advance()  # Skip SELECT

        columns = []
        if self.current_token.type == TokenType.STAR:
            columns = ["*"]
            self.advance()
        else:
            while self.current_token.type not in [TokenType.FROM, TokenType.EOF]:
                if self.current_token.type == TokenType.IDENTIFIER:
                    columns.append(self.current_token.value)
                    self.advance()
                    if self.current_token.type == TokenType.COMMA:
                        self.advance()
                    elif self.current_token.type != TokenType.FROM:
                        raise SyntaxError("Expected ',' or FROM in SELECT")
                else:
                    raise SyntaxError(f"Unexpected token in SELECT column list: {self.current_token.type}")

        self.expect(TokenType.FROM, "Expected FROM")
        table_name = self.expect(TokenType.IDENTIFIER, "Expected table name in SELECT")

        where = None
        if self.current_token.type == TokenType.WHERE:
            self.advance()
            where = self.parse_where_clause()

        limit = None
        if self.current_token.type == TokenType.LIMIT:
            self.advance()
            limit_token = self.expect(TokenType.NUMBER, "Expected limit number")
            limit = int(limit_token.value)

        self.expect(TokenType.SEMICOLON, "Expected ';' after SELECT statement")
        return SelectStatement(table_name, columns, where, limit)

    def parse_where_clause(self) -> Dict[str, Any]:
        """Parse simple WHERE clause (single condition)."""
        left = self.expect(TokenType.IDENTIFIER, "Expected column name").value

        op_token = self.current_token
        if op_token.type not in [
            TokenType.EQUALS, TokenType.NOT_EQUALS,
            TokenType.LESS_THAN, TokenType.GREATER_THAN,
            TokenType.LESS_EQUALS, TokenType.GREATER_EQUALS
        ]:
            raise SyntaxError(f"Expected comparison operator, got {op_token.type}")
        self.advance()

        if self.current_token.type == TokenType.NUMBER:
            right = int(self.current_token.value)
        elif self.current_token.type == TokenType.STRING:
            right = self.current_token.value
        elif self.current_token.type == TokenType.NULL:
            right = None
        else:
            raise SyntaxError(f"Unexpected value in WHERE clause: {self.current_token.type}")
        self.advance()

        return {"column": left, "operator": op_token.type.value, "value": right}

    # ---------------- Helpers ----------------
    def advance(self):
        """Move to the next token."""
        self.position += 1
        if self.position < len(self.tokens):
            self.current_token = self.tokens[self.position]
        else:
            self.current_token = Token(TokenType.EOF, "")

    def expect(self, token_type: TokenType, message: str) -> Token:
        """Expect a specific token type and advance, or raise error."""
        if self.current_token.type == token_type:
            token = self.current_token
            self.advance()
            return token
        raise SyntaxError(f"{message}. Found: {self.current_token.type}")
