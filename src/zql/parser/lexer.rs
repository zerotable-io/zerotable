// code loosely based on ToyDB project.

use std::{iter::Peekable, str::Chars};

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

pub enum Token {
    Number(String),
    String(String),
    Ident(String),
    Keyword(Keyword),
    ParentRef(String),
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

pub struct Lexer<'a> {
    // We need an iterator that is peekable! This way we can look ahead characters without consuming them.
    iter: Peekable<Chars<'a>>,

    // just for counting the position of characters inside the input
    // later line and col will be used in a Span type.
    line: usize,
    col: usize,
}

impl<'a> Lexer<'a> {
    // the lexer lives as long as the input lives!
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
    /// The position tracking is new line \n sensitive.
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

    /// Consume the next character only if it passes a test `predicate`. Otherwise returns `None`.
    ///
    /// eg. consume the next char if it is a digit. If `predicate` fails, nothing is consumed.
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

    /// Eat whitespace and `--` line comments. 
    /// 
    /// We skip those, but the position tracking registers everything
    fn skip_whitespace(&mut self) {
        loop {
            // Skip whitespace characters
            while self.next_if(|c| c.is_whitespace()).is_some() {}

            // Skip -- line comments
            if self.peek() == Some(&'-') {

                // we need to clone because we can look ahead only one char! And for line comments -- there are two chars.
                let mut ahead = self.iter.clone();
                ahead.next();
                if ahead.peek() == Some(&'-') {
                    self.next_char(); // consume first -
                    self.next_char(); // consume second -

                    // now we know that we are inside a comment. We skip all the chars on the same line.
                    // when we encounter a newline, we repeat the loop.
                    while self.next_if(|c| *c != '\n').is_some() {}
                    continue;
                }
            }

            break;
        }
    }
}

impl Iterator for Lexer<'_> {
    // what the iterator yields.The scanning can fail so the item is Result.
    type Item = Result<Token, LexError>;

    // basically we are making the lexer an iterator that yields tokens!
    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
