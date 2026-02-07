use std::fmt;
use std::iter::Peekable;
use std::str::Chars;

/// A ZQL token produced by the lexer.
///
/// The scope of the lexer is pretty limited, almost everything is an
/// identifier (Ident). The parser then decides what each identifier means
/// based on position in the query. This means no word is reserved!
///
/// Literal types are pretty limited. We support string literals
/// only double-quoted. Numbers are stored as raw strings.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// An identifier: field names, operators, collection names,
    /// clauses, and so on.

    /// e.g. `find`, `where`, `eq`, `name`, `users`
    Ident(String),

    /// A double-quoted string literal. e.g. `"hello world"`
    String(String),
    /// A numeric literal, stored as raw text. e.g. `"42"`, `"3.14"`, `"-10"`
    Number(String),
    /// `true`
    True,
    /// `false`
    False,
    /// `null`
    Null,

    /// A variable reference. e.g. `$userId`
    Variable(String),

    /// A parent reference. Useful in nested reads. e.g. `^authorId`
    ParentRef(String),

    /// `::` — separates operation from collection name.
    ColonColon,
    /// `:` — labels a clause or a document field.
    Colon,
    /// `(`
    OpenParen,
    /// `)`
    CloseParen,
    /// `{`
    OpenBrace,
    /// `}`
    CloseBrace,
    /// `[`
    OpenBracket,
    /// `]`
    CloseBracket,
    /// `,`
    Comma,
    /// `.` — field path separator (e.g. `address.city`).
    Dot,
    /// `*` — wildcard in shape (all fields).
    Asterisk,
    /// `-` — field exclusion in shape (e.g. `-password`).
    Minus,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ident(s) => write!(f, "{s}"),
            Self::String(s) => write!(f, "\"{s}\""),
            Self::Number(s) => write!(f, "{s}"),
            Self::True => f.write_str("true"),
            Self::False => f.write_str("false"),
            Self::Null => f.write_str("null"),
            Self::Variable(s) => write!(f, "${s}"),
            Self::ParentRef(s) => write!(f, "^{s}"),
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
            Self::Asterisk => f.write_str("*"),
            Self::Minus => f.write_str("-"),
        }
    }
}

/// Tokenizes a ZQL input string into a stream of tokens.
///
/// The lexer is an iterator that lazily produces tokens as the parser pulls
/// them. It handles whitespace skipping, character escaping, `//` line comments, and all ZQL
/// lexical elements.
///
/// # Example
///
/// ```
/// use zerotable::zql::parser::lexer::{Lexer, Token};
///
/// let tokens: Vec<Token> = Lexer::new("find::users")
///     .collect::<Result<Vec<_>, _>>()
///     .unwrap();
///
/// assert_eq!(tokens, vec![
///     Token::Ident("find".into()),
///     Token::ColonColon,
///     Token::Ident("users".into()),
/// ]);
/// ```
pub struct Lexer<'a> {
    iter: Peekable<Chars<'a>>,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            iter: input.chars().peekable(),
        }
    }

    /// Looks at the next character without consuming it.
    fn peek(&mut self) -> Option<&char> {
        self.iter.peek()
    }

    /// Consumes and returns the next character.
    fn next_char(&mut self) -> Option<char> {
        self.iter.next()
    }

    /// Consumes the next character only if it satisfies the predicate.
    fn next_if(&mut self, f: impl Fn(&char) -> bool) -> Option<char> {
        self.iter.next_if(f)
    }

    /// Consumes characters while they satisfy the predicate, collecting them
    /// into a string.
    fn take_while(&mut self, f: impl Fn(&char) -> bool) -> String {
        let mut s = String::new();
        while let Some(c) = self.iter.next_if(&f) {
            s.push(c);
        }
        s
    }

    /// Skips whitespace and `//` line comments.
    fn skip_whitespace(&mut self) {
        loop {
            // Skip whitespace characters.
            while self.next_if(|c| c.is_whitespace()).is_some() {}

            // Skip // line comments. Clone the iterator to peek two characters
            // ahead without consuming.
            if self.peek() == Some(&'/') {
                let mut ahead = self.iter.clone();
                ahead.next(); // skip the first /
                if ahead.peek() == Some(&'/') {
                    self.next_char(); // consume first /
                    self.next_char(); // consume second /
                    while self.next_if(|c| *c != '\n').is_some() {}
                    continue;
                }
            }

            break;
        }
    }

    /// Scans the next token from the input, or returns `None` at end of input.
    fn scan(&mut self) -> Result<Option<Token>, String> {
        self.skip_whitespace();

        let Some(&c) = self.peek() else {
            return Ok(None);
        };

        match c {
            // String literal: "..."
            '"' => self.scan_string(),

            // Variable: $name
            '$' => self.scan_variable(),

            // Parent reference: ^field
            '^' => self.scan_parent_ref(),

            // Number: 42, 3.14
            c if c.is_ascii_digit() => self.scan_number(),

            // Identifier: name, find, true, eq
            c if c.is_alphabetic() || c == '_' => self.scan_ident(),

            // Negative number or minus: -10, -password
            '-' => {
                self.next_char();
                if self.peek().is_some_and(|c| c.is_ascii_digit()) {
                    self.scan_negative_number()
                } else {
                    Ok(Some(Token::Minus))
                }
            }

            // Double colon or single colon: ::, :
            ':' => {
                self.next_char();
                if self.next_if(|c| *c == ':').is_some() {
                    Ok(Some(Token::ColonColon))
                } else {
                    Ok(Some(Token::Colon))
                }
            }

            // Single-character tokens.
            '(' => {
                self.next_char();
                Ok(Some(Token::OpenParen))
            }
            ')' => {
                self.next_char();
                Ok(Some(Token::CloseParen))
            }
            '{' => {
                self.next_char();
                Ok(Some(Token::OpenBrace))
            }
            '}' => {
                self.next_char();
                Ok(Some(Token::CloseBrace))
            }
            '[' => {
                self.next_char();
                Ok(Some(Token::OpenBracket))
            }
            ']' => {
                self.next_char();
                Ok(Some(Token::CloseBracket))
            }
            ',' => {
                self.next_char();
                Ok(Some(Token::Comma))
            }
            '.' => {
                self.next_char();
                Ok(Some(Token::Dot))
            }
            '*' => {
                self.next_char();
                Ok(Some(Token::Asterisk))
            }

            c => Err(format!("unexpected character: '{c}'")),
        }
    }

    /// Scans a double-quoted string literal with escape support.
    /// Supports: `\"`, `\\`, `\n`, `\t`, `\r`.
    // NOTE: We need to add support for single-quoted strings? If we send the string inside a json we need to double escape!
    fn scan_string(&mut self) -> Result<Option<Token>, String> {
        self.next_char(); // consume opening "
        let mut s = String::new();
        loop {
            match self.next_char() {
                None => return Err("unterminated string literal".into()),
                Some('"') => return Ok(Some(Token::String(s))),
                Some('\\') => match self.next_char() {
                    None => return Err("unterminated string escape".into()),
                    Some('"') => s.push('"'),
                    Some('\\') => s.push('\\'),
                    Some('n') => s.push('\n'),
                    Some('t') => s.push('\t'),
                    Some('r') => s.push('\r'),
                    Some(c) => return Err(format!("invalid string escape: \\{c}")),
                },
                Some(c) => s.push(c),
            }
        }
    }

    /// Scans a variable reference: `$` followed by an identifier.
    fn scan_variable(&mut self) -> Result<Option<Token>, String> {
        self.next_char(); // consume $
        let name = self.take_while(|c| c.is_alphanumeric() || *c == '_');
        if name.is_empty() {
            return Err("expected variable name after $".into());
        }
        Ok(Some(Token::Variable(name)))
    }

    /// Scans a parent document reference: `^` followed by an identifier.
    fn scan_parent_ref(&mut self) -> Result<Option<Token>, String> {
        self.next_char(); // consume ^
        let name = self.take_while(|c| c.is_alphanumeric() || *c == '_');
        if name.is_empty() {
            return Err("expected field name after ^".into());
        }
        Ok(Some(Token::ParentRef(name)))
    }

    /// Scans a numeric literal (positive). Collects digits, optional decimal
    /// point and more digits, storing the raw text.
    fn scan_number(&mut self) -> Result<Option<Token>, String> {
        let mut s = self.take_while(|c| c.is_ascii_digit());

        // If followed by `.` and then a digit, include the fractional part.
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

    /// Scans a negative numeric literal. The `-` has already been consumed.
    fn scan_negative_number(&mut self) -> Result<Option<Token>, String> {
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

    /// Scans an identifier or literal keyword (`true`/`false`/`null`).
    ///
    /// Identifiers start with a letter or `_` and continue with alphanumerics
    /// or `_`. If the resulting string matches `true`/`false`/`null`, it
    /// produces the corresponding literal token. Everything else (including
    /// `find`, `where`, `limit`, `eq`) is an `Ident`.
    fn scan_ident(&mut self) -> Result<Option<Token>, String> {
        let name = self.take_while(|c| c.is_alphanumeric() || *c == '_');

        match name.as_str() {
            "true" => Ok(Some(Token::True)),
            "false" => Ok(Some(Token::False)),
            "null" => Ok(Some(Token::Null)),
            _ => Ok(Some(Token::Ident(name))),
        }
    }
}

impl Iterator for Lexer<'_> {
    type Item = Result<Token, String>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.scan() {
            Ok(Some(token)) => Some(Ok(token)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: lex input and collect all tokens, panicking on error.
    fn lex(input: &str) -> Vec<Token> {
        Lexer::new(input).collect::<Result<Vec<_>, _>>().unwrap()
    }

    /// Helper: lex input and expect an error.
    fn lex_err(input: &str) -> String {
        Lexer::new(input)
            .collect::<Result<Vec<_>, _>>()
            .unwrap_err()
    }

    #[test]
    fn empty_input() {
        assert_eq!(lex(""), vec![]);
        assert_eq!(lex("   "), vec![]);
        assert_eq!(lex(" \t\n "), vec![]);
    }

    #[test]
    fn identifiers() {
        assert_eq!(lex("name"), vec![Token::Ident("name".into())]);
        assert_eq!(lex("userId"), vec![Token::Ident("userId".into())]);
        assert_eq!(lex("_private"), vec![Token::Ident("_private".into())]);
        assert_eq!(lex("field1"), vec![Token::Ident("field1".into())]);
    }

    #[test]
    fn string_literals() {
        assert_eq!(lex(r#""hello""#), vec![Token::String("hello".into())]);
        assert_eq!(lex(r#""""#), vec![Token::String("".into())]);
        assert_eq!(
            lex(r#""hello world""#),
            vec![Token::String("hello world".into())]
        );
    }

    #[test]
    fn string_escapes() {
        assert_eq!(
            lex(r#""say \"hi\"""#),
            vec![Token::String("say \"hi\"".into())]
        );
        assert_eq!(lex(r#""a\\b""#), vec![Token::String("a\\b".into())]);
        assert_eq!(
            lex(r#""line\nbreak""#),
            vec![Token::String("line\nbreak".into())]
        );
        assert_eq!(
            lex(r#""tab\there""#),
            vec![Token::String("tab\there".into())]
        );
    }

    #[test]
    fn string_errors() {
        assert_eq!(lex_err(r#""unterminated"#), "unterminated string literal");
        assert_eq!(lex_err(r#""bad\x""#), "invalid string escape: \\x");
    }

    #[test]
    fn numbers() {
        assert_eq!(lex("0"), vec![Token::Number("0".into())]);
        assert_eq!(lex("42"), vec![Token::Number("42".into())]);
        assert_eq!(lex("1000"), vec![Token::Number("1000".into())]);
        assert_eq!(lex("3.14"), vec![Token::Number("3.14".into())]);
        assert_eq!(lex("0.5"), vec![Token::Number("0.5".into())]);
    }

    #[test]
    fn negative_numbers() {
        assert_eq!(lex("-1"), vec![Token::Number("-1".into())]);
        assert_eq!(lex("-42"), vec![Token::Number("-42".into())]);
        assert_eq!(lex("-0.5"), vec![Token::Number("-0.5".into())]);
    }

    #[test]
    fn number_followed_by_dot_not_digit() {
        // `10.` followed by identifier — the dot is NOT part of the number.
        assert_eq!(
            lex("10.field"),
            vec![
                Token::Number("10".into()),
                Token::Dot,
                Token::Ident("field".into())
            ]
        );
    }

    #[test]
    fn boolean_and_null() {
        assert_eq!(lex("true"), vec![Token::True]);
        assert_eq!(lex("false"), vec![Token::False]);
        assert_eq!(lex("null"), vec![Token::Null]);
    }

    #[test]
    fn variables() {
        assert_eq!(lex("$userId"), vec![Token::Variable("userId".into())]);
        assert_eq!(lex("$limit"), vec![Token::Variable("limit".into())]);
    }

    #[test]
    fn variable_error() {
        assert_eq!(lex_err("$ "), "expected variable name after $");
    }

    #[test]
    fn parent_refs() {
        assert_eq!(lex("^authorId"), vec![Token::ParentRef("authorId".into())]);
        assert_eq!(lex("^_id"), vec![Token::ParentRef("_id".into())]);
    }

    #[test]
    fn parent_ref_error() {
        assert_eq!(lex_err("^ "), "expected field name after ^");
    }

    #[test]
    fn punctuation() {
        assert_eq!(lex("::"), vec![Token::ColonColon]);
        assert_eq!(lex(":"), vec![Token::Colon]);
        assert_eq!(lex("("), vec![Token::OpenParen]);
        assert_eq!(lex(")"), vec![Token::CloseParen]);
        assert_eq!(lex("{"), vec![Token::OpenBrace]);
        assert_eq!(lex("}"), vec![Token::CloseBrace]);
        assert_eq!(lex("["), vec![Token::OpenBracket]);
        assert_eq!(lex("]"), vec![Token::CloseBracket]);
        assert_eq!(lex(","), vec![Token::Comma]);
        assert_eq!(lex("."), vec![Token::Dot]);
        assert_eq!(lex("*"), vec![Token::Asterisk]);
    }

    #[test]
    fn minus_vs_negative() {
        // Minus before a letter = exclude operator.
        assert_eq!(
            lex("-password"),
            vec![Token::Minus, Token::Ident("password".into())]
        );
        // Minus before a digit = negative number.
        assert_eq!(lex("-10"), vec![Token::Number("-10".into())]);
    }

    #[test]
    fn comments() {
        assert_eq!(lex("// this is a comment"), vec![]);
        assert_eq!(
            lex("name // comment\nemail"),
            vec![Token::Ident("name".into()), Token::Ident("email".into())]
        );
        assert_eq!(
            lex("// line 1\n// line 2\nname"),
            vec![Token::Ident("name".into())]
        );
    }

    #[test]
    fn unexpected_character() {
        assert_eq!(lex_err("@"), "unexpected character: '@'");
        assert_eq!(lex_err("#"), "unexpected character: '#'");
    }

    // -- Full query tokenization tests --

    #[test]
    fn find_query() {
        let tokens = lex(r#"find::users (where: (eq status "active") limit: 10) { name, email }"#);
        assert_eq!(
            tokens,
            vec![
                Token::Ident("find".into()),
                Token::ColonColon,
                Token::Ident("users".into()),
                Token::OpenParen,
                Token::Ident("where".into()),
                Token::Colon,
                Token::OpenParen,
                Token::Ident("eq".into()),
                Token::Ident("status".into()),
                Token::String("active".into()),
                Token::CloseParen,
                Token::Ident("limit".into()),
                Token::Colon,
                Token::Number("10".into()),
                Token::CloseParen,
                Token::OpenBrace,
                Token::Ident("name".into()),
                Token::Comma,
                Token::Ident("email".into()),
                Token::CloseBrace,
            ]
        );
    }

    #[test]
    fn get_by_variable() {
        let tokens = lex("get::users ($userId) { name, email }");
        assert_eq!(
            tokens,
            vec![
                Token::Ident("get".into()),
                Token::ColonColon,
                Token::Ident("users".into()),
                Token::OpenParen,
                Token::Variable("userId".into()),
                Token::CloseParen,
                Token::OpenBrace,
                Token::Ident("name".into()),
                Token::Comma,
                Token::Ident("email".into()),
                Token::CloseBrace,
            ]
        );
    }

    #[test]
    fn create_with_returning() {
        let tokens = lex(r#"create::users { name: "Alice", email: "a@b.com" } returning { _id }"#);
        assert_eq!(
            tokens,
            vec![
                Token::Ident("create".into()),
                Token::ColonColon,
                Token::Ident("users".into()),
                Token::OpenBrace,
                Token::Ident("name".into()),
                Token::Colon,
                Token::String("Alice".into()),
                Token::Comma,
                Token::Ident("email".into()),
                Token::Colon,
                Token::String("a@b.com".into()),
                Token::CloseBrace,
                Token::Ident("returning".into()),
                Token::OpenBrace,
                Token::Ident("_id".into()),
                Token::CloseBrace,
            ]
        );
    }

    #[test]
    fn delete_with_where() {
        let tokens = lex("delete::sessions (where: (lt expiresAt $now))");
        assert_eq!(
            tokens,
            vec![
                Token::Ident("delete".into()),
                Token::ColonColon,
                Token::Ident("sessions".into()),
                Token::OpenParen,
                Token::Ident("where".into()),
                Token::Colon,
                Token::OpenParen,
                Token::Ident("lt".into()),
                Token::Ident("expiresAt".into()),
                Token::Variable("now".into()),
                Token::CloseParen,
                Token::CloseParen,
            ]
        );
    }

    #[test]
    fn nested_expressions() {
        let tokens = lex("(and (gte age 21) (eq active true))");
        assert_eq!(
            tokens,
            vec![
                Token::OpenParen,
                Token::Ident("and".into()),
                Token::OpenParen,
                Token::Ident("gte".into()),
                Token::Ident("age".into()),
                Token::Number("21".into()),
                Token::CloseParen,
                Token::OpenParen,
                Token::Ident("eq".into()),
                Token::Ident("active".into()),
                Token::True,
                Token::CloseParen,
                Token::CloseParen,
            ]
        );
    }

    #[test]
    fn shape_with_exclude_and_wildcard() {
        let tokens = lex("{ * -password -secret }");
        assert_eq!(
            tokens,
            vec![
                Token::OpenBrace,
                Token::Asterisk,
                Token::Minus,
                Token::Ident("password".into()),
                Token::Minus,
                Token::Ident("secret".into()),
                Token::CloseBrace,
            ]
        );
    }

    #[test]
    fn field_path() {
        let tokens = lex("address.city");
        assert_eq!(
            tokens,
            vec![
                Token::Ident("address".into()),
                Token::Dot,
                Token::Ident("city".into()),
            ]
        );
    }

    #[test]
    fn order_clause() {
        let tokens = lex("order: (desc createdAt)");
        assert_eq!(
            tokens,
            vec![
                Token::Ident("order".into()),
                Token::Colon,
                Token::OpenParen,
                Token::Ident("desc".into()),
                Token::Ident("createdAt".into()),
                Token::CloseParen,
            ]
        );
    }

    #[test]
    fn array_literal() {
        let tokens = lex(r#"[1, "two", true, null]"#);
        assert_eq!(
            tokens,
            vec![
                Token::OpenBracket,
                Token::Number("1".into()),
                Token::Comma,
                Token::String("two".into()),
                Token::Comma,
                Token::True,
                Token::Comma,
                Token::Null,
                Token::CloseBracket,
            ]
        );
    }

    #[test]
    fn parent_ref_in_nested_read() {
        let tokens = lex("(eq authorId ^_id)");
        assert_eq!(
            tokens,
            vec![
                Token::OpenParen,
                Token::Ident("eq".into()),
                Token::Ident("authorId".into()),
                Token::ParentRef("_id".into()),
                Token::CloseParen,
            ]
        );
    }

    #[test]
    fn document_with_reserved_looking_fields() {
        // Fields named "find", "where", "limit" are all just Ident — no clash with documents with those word.
        let tokens = lex(r#"{ find: "x", where: "y", limit: 10 }"#);
        assert_eq!(
            tokens,
            vec![
                Token::OpenBrace,
                Token::Ident("find".into()),
                Token::Colon,
                Token::String("x".into()),
                Token::Comma,
                Token::Ident("where".into()),
                Token::Colon,
                Token::String("y".into()),
                Token::Comma,
                Token::Ident("limit".into()),
                Token::Colon,
                Token::Number("10".into()),
                Token::CloseBrace,
            ]
        );
    }
}
