"""
SQL Lexer - converts SQL strings into tokens.
"""
from enum import Enum
from typing import List, Tuple, Optional
import re

class TokenType(Enum):
    """Types of tokens in SQL."""
    # Keywords
    SELECT = "SELECT"
    FROM = "FROM"
    WHERE = "WHERE"
    INSERT = "INSERT"
    INTO = "INTO"
    VALUES = "VALUES"
    UPDATE = "UPDATE"
    SET = "SET"
    DELETE = "DELETE"
    CREATE = "CREATE"
    TABLE = "TABLE"
    DROP = "DROP"
    INDEX = "INDEX"
    PRIMARY = "PRIMARY"
    KEY = "KEY"
    UNIQUE = "UNIQUE"
    NOT = "NOT"
    NULL = "NULL"
    AND = "AND"
    OR = "OR"
    ORDER = "ORDER"
    BY = "BY"
    ASC = "ASC"
    DESC = "DESC"
    LIMIT = "LIMIT"
    JOIN = "JOIN"
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    ON = "ON"
    BEGIN = "BEGIN"
    COMMIT = "COMMIT"
    ROLLBACK = "ROLLBACK"
    TRANSACTION = "TRANSACTION"
    
    # Data types
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    VARCHAR = "VARCHAR"
    TEXT = "TEXT"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    
    # Operators
    EQUALS = "="
    NOT_EQUALS = "!="
    LESS_THAN = "<"
    GREATER_THAN = ">"
    LESS_EQUALS = "<="
    GREATER_EQUALS = ">="
    PLUS = "+"
    MINUS = "-"
    MULTIPLY = "*"
    DIVIDE = "/"
    
    # Punctuation
    COMMA = ","
    DOT = "."
    SEMICOLON = ";"
    LPAREN = "("
    RPAREN = ")"
    STAR = "*"  # For SELECT *
    
    # Literals
    IDENTIFIER = "IDENTIFIER"
    STRING = "STRING"
    NUMBER = "NUMBER"
    
    # Special
    EOF = "EOF"

class Token:
    """A single token with type and value."""
    
    def __init__(self, token_type: TokenType, value: str = "", 
                 line: int = 0, column: int = 0):
        self.type = token_type
        self.value = value
        self.line = line
        self.column = column
    
    def __repr__(self) -> str:
        if self.value:
            return f"Token({self.type.value}, '{self.value}', line={self.line})"
        return f"Token({self.type.value}, line={self.line})"
    
    def __eq__(self, other) -> bool:
        if isinstance(other, Token):
            return self.type == other.type and self.value == other.value
        elif isinstance(other, TokenType):
            return self.type == other
        return False

class SQLLexer:
    """Lexical analyzer for SQL statements."""
    
    # Regex patterns for tokens
    PATTERNS = [
        # Keywords (case-insensitive)
        (r'(?i)SELECT', TokenType.SELECT),
        (r'(?i)FROM', TokenType.FROM),
        (r'(?i)WHERE', TokenType.WHERE),
        (r'(?i)INSERT', TokenType.INSERT),
        (r'(?i)INTO', TokenType.INTO),
        (r'(?i)VALUES', TokenType.VALUES),
        (r'(?i)UPDATE', TokenType.UPDATE),
        (r'(?i)SET', TokenType.SET),
        (r'(?i)DELETE', TokenType.DELETE),
        (r'(?i)CREATE', TokenType.CREATE),
        (r'(?i)TABLE', TokenType.TABLE),
        (r'(?i)DROP', TokenType.DROP),
        (r'(?i)INDEX', TokenType.INDEX),
        (r'(?i)PRIMARY', TokenType.PRIMARY),
        (r'(?i)KEY', TokenType.KEY),
        (r'(?i)UNIQUE', TokenType.UNIQUE),
        (r'(?i)NOT', TokenType.NOT),
        (r'(?i)NULL', TokenType.NULL),
        (r'(?i)AND', TokenType.AND),
        (r'(?i)OR', TokenType.OR),
        (r'(?i)ORDER', TokenType.ORDER),
        (r'(?i)BY', TokenType.BY),
        (r'(?i)ASC', TokenType.ASC),
        (r'(?i)DESC', TokenType.DESC),
        (r'(?i)LIMIT', TokenType.LIMIT),
        (r'(?i)JOIN', TokenType.JOIN),
        (r'(?i)INNER', TokenType.INNER),
        (r'(?i)LEFT', TokenType.LEFT),
        (r'(?i)RIGHT', TokenType.RIGHT),
        (r'(?i)ON', TokenType.ON),
        (r'(?i)BEGIN', TokenType.BEGIN),
        (r'(?i)COMMIT', TokenType.COMMIT),
        (r'(?i)ROLLBACK', TokenType.ROLLBACK),
        (r'(?i)TRANSACTION', TokenType.TRANSACTION),
        
        # Data types
        (r'(?i)INTEGER', TokenType.INTEGER),
        (r'(?i)BIGINT', TokenType.BIGINT),
        (r'(?i)VARCHAR', TokenType.VARCHAR),
        (r'(?i)TEXT', TokenType.TEXT),
        (r'(?i)FLOAT', TokenType.FLOAT),
        (r'(?i)DOUBLE', TokenType.DOUBLE),
        (r'(?i)BOOLEAN', TokenType.BOOLEAN),
        (r'(?i)DATE', TokenType.DATE),
        (r'(?i)TIMESTAMP', TokenType.TIMESTAMP),
        
        # Operators
        (r'=', TokenType.EQUALS),
        (r'!=', TokenType.NOT_EQUALS),
        (r'<', TokenType.LESS_THAN),
        (r'>', TokenType.GREATER_THAN),
        (r'<=', TokenType.LESS_EQUALS),
        (r'>=', TokenType.GREATER_EQUALS),
        (r'\+', TokenType.PLUS),
        (r'-', TokenType.MINUS),
        (r'\*', TokenType.MULTIPLY),
        (r'/', TokenType.DIVIDE),
        
        # Punctuation
        (r',', TokenType.COMMA),
        (r'\.', TokenType.DOT),
        (r';', TokenType.SEMICOLON),
        (r'\(', TokenType.LPAREN),
        (r'\)', TokenType.RPAREN),
        
        # Literals
        (r'"[^"]*"', TokenType.STRING),  # Double quoted strings
        (r"'[^']*'", TokenType.STRING),  # Single quoted strings
        (r'\d+\.\d+', TokenType.NUMBER),  # Floating point
        (r'\d+', TokenType.NUMBER),       # Integers
        
        # Identifiers
        (r'[a-zA-Z_][a-zA-Z0-9_]*', TokenType.IDENTIFIER),
        
        # Whitespace (ignored)
        (r'\s+', None),
    ]
    
    def __init__(self, sql: str):
        self.sql = sql
        self.position = 0
        self.line = 1
        self.column = 1
        self.tokens: List[Token] = []
    
    def tokenize(self) -> List[Token]:
        """Convert SQL string to tokens."""
        while self.position < len(self.sql):
            token = self._next_token()
            if token:
                self.tokens.append(token)
        
        # Add EOF token
        self.tokens.append(Token(TokenType.EOF, "", self.line, self.column))
        return self.tokens
    
    def _next_token(self) -> Optional[Token]:
        """Get the next token from input."""
        remaining = self.sql[self.position:]
        
        for pattern, token_type in self.PATTERNS:
            match = re.match(pattern, remaining)
            if match:
                value = match.group(0)
                start = self.position
                
                # Update position
                self.position += len(value)
                
                # Update line and column
                lines = value.count('\n')
                if lines > 0:
                    self.line += lines
                    last_newline = value.rfind('\n')
                    self.column = len(value) - last_newline
                else:
                    self.column += len(value)
                
                # Skip whitespace tokens
                if token_type is None:
                    return None
                
                # Handle string literals (remove quotes)
                if token_type == TokenType.STRING:
                    value = value[1:-1]  # Remove surrounding quotes
                
                # Handle identifiers vs keywords
                if token_type == TokenType.IDENTIFIER:
                    # Check if it's actually a keyword
                    upper_value = value.upper()
                    for kw_pattern, kw_type in self.PATTERNS:
                        if kw_type and kw_type != TokenType.IDENTIFIER:
                            kw_name = kw_pattern.replace(r'(?i)', '').replace('\\', '')
                            if kw_name.upper() == upper_value:
                                token_type = kw_type
                                break
                
                return Token(token_type, value, self.line - lines, self.column - len(value))
        
        # No pattern matched - syntax error
        raise SyntaxError(f"Unexpected character '{remaining[0]}' at line {self.line}, column {self.column}")