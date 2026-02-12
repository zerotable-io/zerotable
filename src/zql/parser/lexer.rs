// Code loosely based on ToyDB project.

use std::fmt;
use std::iter::Peekable;
use std::str::Chars;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Keyword {
    And,
    Or,
    Not,
    In,
    Between,
    Like,
    Regex,
    Is,
    Exists,
    Contains,
    ContainsAny,
    ContainsAll,
    Where,
    Order,
    Skip,
    Limit,
    Returning,
    Asc,
    Desc,
    True,
    False,
    Null,
}

/// Maps a lowercase string to a Keyword, if it is one.
/// This is how the lexer decides "is this identifier actually a reserved word?"
impl TryFrom<&str> for Keyword {
    type Error = ();

    fn try_from(s: &str) -> Result<Self, ()> {
        match s {
            "and" => Ok(Self::And),
            "or" => Ok(Self::Or),
            "not" => Ok(Self::Not),
            "in" => Ok(Self::In),
            "between" => Ok(Self::Between),
            "like" => Ok(Self::Like),
            "regex" => Ok(Self::Regex),
            "is" => Ok(Self::Is),
            "exists" => Ok(Self::Exists),
            "contains" => Ok(Self::Contains),
            "containsAny" => Ok(Self::ContainsAny),
            "containsAll" => Ok(Self::ContainsAll),
            "where" => Ok(Self::Where),
            "order" => Ok(Self::Order),
            "skip" => Ok(Self::Skip),
            "limit" => Ok(Self::Limit),
            "returning" => Ok(Self::Returning),
            "asc" => Ok(Self::Asc),
            "desc" => Ok(Self::Desc),
            "true" => Ok(Self::True),
            "false" => Ok(Self::False),
            "null" => Ok(Self::Null),
            _ => Err(()),
        }
    }
}

impl fmt::Display for Keyword {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::And => "and",
            Self::Or => "or",
            Self::Not => "not",
            Self::In => "in",
            Self::Between => "between",
            Self::Like => "like",
            Self::Regex => "regex",
            Self::Is => "is",
            Self::Exists => "exists",
            Self::Contains => "contains",
            Self::ContainsAny => "containsAny",
            Self::ContainsAll => "containsAll",
            Self::Where => "where",
            Self::Order => "order",
            Self::Skip => "skip",
            Self::Limit => "limit",
            Self::Returning => "returning",
            Self::Asc => "asc",
            Self::Desc => "desc",
            Self::True => "true",
            Self::False => "false",
            Self::Null => "null",
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Number(String),
    String(String),
    Ident(String),
    Keyword(Keyword),
    Variable(String),
    ParentRef(String),
    GrandparentRef(String),
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Plus,
    Minus,
    Asterisk,
    Slash,
    Percent,
    ColonColon,
    Colon,
    OpenParen,
    CloseParen,
    OpenBrace,
    CloseBrace,
    OpenBracket,
    CloseBracket,
    Comma,
    Dot,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Number(s) => write!(f, "{s}"),
            Self::String(s) => write!(f, "\"{s}\""),
            Self::Ident(s) => write!(f, "{s}"),
            Self::Keyword(k) => write!(f, "{k}"),
            Self::Variable(s) => write!(f, "${s}"),
            Self::ParentRef(s) => write!(f, "^{s}"),
            Self::GrandparentRef(s) => write!(f, "^^{s}"),
            Self::Equal => f.write_str("="),
            Self::NotEqual => f.write_str("!="),
            Self::GreaterThan => f.write_str(">"),
            Self::GreaterThanOrEqual => f.write_str(">="),
            Self::LessThan => f.write_str("<"),
            Self::LessThanOrEqual => f.write_str("<="),
            Self::Plus => f.write_str("+"),
            Self::Minus => f.write_str("-"),
            Self::Asterisk => f.write_str("*"),
            Self::Slash => f.write_str("/"),
            Self::Percent => f.write_str("%"),
            Self::ColonColon => f.write_str("::"),
            Self::Colon => f.write_str(":"),
            Self::OpenParen => f.write_str("("),
            Self::CloseParen => f.write_str(")"),
            Self::OpenBrace => f.write_str("{"),
            Self::CloseBrace => f.write_str("}"),
            Self::OpenBracket => f.write_str("["),
            Self::CloseBracket => f.write_str("]"),
            Self::Comma => f.write_str(","),
            Self::Dot => f.write_str("."),
        }
    }
}

/// A simple lexer error. We keep it as a message string for now.
/// Span-based error reporting can be layered on later.
#[derive(Debug, Clone, PartialEq)]
pub struct LexError(pub String);

impl fmt::Display for LexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for LexError {}

pub struct Lexer<'a> {
    // We need an iterator that is peekable! This way we can look ahead
    // characters without consuming them.
    iter: Peekable<Chars<'a>>,

    // Just for counting the position of characters inside the input.
    // Later line and col will be used in a Span type.
    line: usize,
    col: usize,
}

impl<'a> Lexer<'a> {
    // The lexer lives as long as the input lives!
    pub fn new(input: &'a str) -> Self {
        Self {
            iter: input.chars().peekable(),
            line: 1,
            col: 1,
        }
    }

    /// Look at the next character without consuming it.
    fn peek(&mut self) -> Option<&char> {
        self.iter.peek()
    }

    /// Consume the next character and advance position tracking `line`, `col`.
    ///
    /// The position tracking is newline \n sensitive.
    fn next_char(&mut self) -> Option<char> {
        let c = self.iter.next()?;
        if c == '\n' {
            self.line += 1;
            self.col = 1;
        } else {
            self.col += 1;
        }
        Some(c)
    }

    /// Consume the next character only if it passes a test `predicate`.
    /// Otherwise returns `None` and nothing is consumed.
    fn next_if(&mut self, predicate: impl Fn(&char) -> bool) -> Option<char> {
        let c = self.iter.next_if(predicate)?;
        if c == '\n' {
            self.line += 1;
            self.col = 1;
        } else {
            self.col += 1;
        }
        Some(c)
    }

    /// Consume characters while they satisfy the predicate, collecting them
    /// into a string.
    fn take_while(&mut self, predicate: impl Fn(&char) -> bool) -> String {
        let mut s = String::new();
        while let Some(c) = self.next_if(&predicate) {
            s.push(c);
        }
        s
    }

    /// Eat whitespace and `--` line comments.
    ///
    /// We skip those, but the position tracking registers everything.
    fn skip_whitespace(&mut self) {
        loop {
            // Skip whitespace characters.
            while self.next_if(|c| c.is_whitespace()).is_some() {}

            // Skip -- line comments.
            if self.peek() == Some(&'-') {
                // We need to clone because we can look ahead only one char!
                // For line comments -- there are two chars.
                let mut ahead = self.iter.clone();
                ahead.next();
                if ahead.peek() == Some(&'-') {
                    self.next_char(); // consume first -
                    self.next_char(); // consume second -

                    // Now we know that we are inside a comment. We skip all
                    // the chars on the same line. When we encounter a newline,
                    // we repeat the loop.
                    while self.next_if(|c| *c != '\n').is_some() {}
                    continue;
                }
            }

            break;
        }
    }

    /// The main dispatch method. Looks at the next character and calls the
    /// appropriate scanner.
    fn scan(&mut self) -> Result<Option<Token>, LexError> {
        self.skip_whitespace();

        let Some(&c) = self.peek() else {
            return Ok(None); // end of input
        };

        match c {
            '"' => self.scan_string(),
            '$' => self.scan_variable(),
            '^' => self.scan_parent_ref(),
            '`' => self.scan_quoted_ident(),
            '0'..='9' => self.scan_number(),
            c if c.is_alphabetic() || c == '_' => self.scan_ident_or_keyword(),
            _ => self.scan_symbol(),
        }
    }

    /// Scans operators and punctuation. Handles both single-char tokens like
    /// `(` and multi-char tokens like `::`, `>=`, `!=`.
    fn scan_symbol(&mut self) -> Result<Option<Token>, LexError> {
        // We know peek() returned Some from the scan() function, so next_char() won't be None. 
        // But we check anyway, defensive in depth.
        let Some(c) = self.next_char() else {
            return Ok(None);
        };

        let mut token = match c {
            ':' => Token::Colon,
            '(' => Token::OpenParen,
            ')' => Token::CloseParen,
            '{' => Token::OpenBrace,
            '}' => Token::CloseBrace,
            '[' => Token::OpenBracket,
            ']' => Token::CloseBracket,
            ',' => Token::Comma,
            '.' => Token::Dot,
            '=' => Token::Equal,
            '>' => Token::GreaterThan,
            '<' => Token::LessThan,
            '!' => {
                // `!` alone is not valid in ZQL, only `!=`
                if self.peek() == Some(&'=') {
                    self.next_char();
                    return Ok(Some(Token::NotEqual));
                }
                return Err(LexError(format!(
                    "unexpected character '!' at line {}, col {} (did you mean '!='?)",
                    self.line,
                    self.col - 1
                )));
            }
            '+' => Token::Plus,
            '*' => Token::Asterisk,
            '/' => Token::Slash,
            '%' => Token::Percent,
            '-' => {
                // Check if this starts a negative number.
                if self.peek().is_some_and(|c| c.is_ascii_digit()) {
                    return self.scan_negative_number();
                }
                Token::Minus
            }
            _ => {
                return Err(LexError(format!(
                    "unexpected character '{c}' at line {}, col {}",
                    self.line,
                    self.col - 1
                )));
            }
        };

        // Check if this single-char token extends to a two-char token.
        token = match token {
            Token::Colon if self.peek() == Some(&':') => {
                self.next_char();
                Token::ColonColon
            }
            Token::GreaterThan if self.peek() == Some(&'=') => {
                self.next_char();
                Token::GreaterThanOrEqual
            }
            Token::LessThan if self.peek() == Some(&'=') => {
                self.next_char();
                Token::LessThanOrEqual
            }
            other => other,
        };

        Ok(Some(token))
    }

    /// Scans a double-quoted string literal with escape support.
    /// Supports: `\"`, `\\`, `\n`, `\t`, `\r`.
    fn scan_string(&mut self) -> Result<Option<Token>, LexError> {
        self.next_char(); // consume opening "

        let mut s = String::new();
        loop {
            match self.next_char() {
                None => {
                    return Err(LexError("unterminated string literal".into()));
                }
                Some('"') => {
                    return Ok(Some(Token::String(s)));
                }
                Some('\\') => match self.next_char() {
                    None => return Err(LexError("unterminated string escape".into())),

                    // It is fine to have an empty string.
                    Some('"') => s.push('"'),
                    Some('\\') => s.push('\\'),
                    Some('n') => s.push('\n'),
                    Some('t') => s.push('\t'),
                    Some('r') => s.push('\r'),
                    Some(c) => {
                        return Err(LexError(format!("invalid string escape: \\{c}")));
                    }
                },
                Some(c) => s.push(c),
            }
        }
    }

    /// Scans a numeric literal (positive). Digits, optional decimal point.
    fn scan_number(&mut self) -> Result<Option<Token>, LexError> {
        let mut s = self.take_while(|c| c.is_ascii_digit());

        // If followed by `.` and then a digit, include the fractional part.
        // We need to look two chars ahead: the dot and the digit after it.
        // This avoids consuming the dot in `10.field`.
        if self.peek() == Some(&'.') {
            let mut ahead = self.iter.clone();
            ahead.next(); // skip the .
            if ahead.peek().is_some_and(|c| c.is_ascii_digit()) {
                self.next_char(); // consume .
                s.push('.');
                s.push_str(&self.take_while(|c| c.is_ascii_digit()));
            }
        }

        Ok(Some(Token::Number(s)))
    }

    /// Scans a negative numeric literal. The `-` has already been consumed
    /// by scan_symbol. So this function needs to be used paired with scan_symbol
    /// that checks if we have a simple minus token or a negative number.
    fn scan_negative_number(&mut self) -> Result<Option<Token>, LexError> {
        let mut s = "-".to_string();
        s.push_str(&self.take_while(|c| c.is_ascii_digit()));

        if self.peek() == Some(&'.') {
            let mut ahead = self.iter.clone();
            ahead.next();
            if ahead.peek().is_some_and(|c| c.is_ascii_digit()) {
                self.next_char();
                s.push('.');
                s.push_str(&self.take_while(|c| c.is_ascii_digit()));
            }
        }

        Ok(Some(Token::Number(s)))
    }

    /// Scans a variable reference: `$` followed by an identifier name.
    fn scan_variable(&mut self) -> Result<Option<Token>, LexError> {
        self.next_char(); // consume $
        let name = self.take_while(|c| c.is_alphanumeric() || *c == '_');
        if name.is_empty() {
            return Err(LexError("expected variable name after $".into()));
        }
        Ok(Some(Token::Variable(name)))
    }

    /// Scans a parent reference: `^field` or grandparent `^^field`.
    fn scan_parent_ref(&mut self) -> Result<Option<Token>, LexError> {
        self.next_char(); // consume first ^

        // Check for grandparent ^^
        let is_grandparent = self.peek() == Some(&'^');
        if is_grandparent {
            self.next_char(); // consume second ^
        }

        let name = self.take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '.');
        if name.is_empty() {
            return Err(LexError("expected field name after ^".into()));
        }

        if is_grandparent {
            Ok(Some(Token::GrandparentRef(name)))
        } else {
            Ok(Some(Token::ParentRef(name)))
        }
    }

    /// Scans a backtick-quoted identifier: `\`order\``.
    /// This is the escape hatch for using reserved words (Keywords) as field names.
    fn scan_quoted_ident(&mut self) -> Result<Option<Token>, LexError> {
        self.next_char(); // consume opening `

        let mut name = String::new();
        loop {
            match self.next_char() {
                None => return Err(LexError("unterminated quoted identifier".into())),
                Some('`') => {
                    if name.is_empty() {
                        return Err(LexError("empty quoted identifier".into()));
                    }
                    return Ok(Some(Token::Ident(name)));
                }
                Some(c) => name.push(c),
            }
        }
    }

    /// Scans an identifier or keyword. Collects letters, digits, and
    /// underscores, then checks if the result is a reserved keyword.
    fn scan_ident_or_keyword(&mut self) -> Result<Option<Token>, LexError> {
        let name = self.take_while(|c| c.is_alphanumeric() || *c == '_');

        // Check if this identifier is actually a keyword.
        if let Ok(keyword) = Keyword::try_from(name.as_str()) {
            Ok(Some(Token::Keyword(keyword)))
        } else {
            Ok(Some(Token::Ident(name)))
        }
    }
}

impl Iterator for Lexer<'_> {
    // The scanning can fail, so the item is a Result.
    type Item = Result<Token, LexError>;

    // We are making the lexer an iterator that yields tokens!
    fn next(&mut self) -> Option<Self::Item> {
        match self.scan() {
            Ok(Some(token)) => Some(Ok(token)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
