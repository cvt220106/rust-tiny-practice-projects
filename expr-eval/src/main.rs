use std::{fmt::Display, iter::Peekable, str::Chars};

// type alias reduce Result complexity
type Result<T> = std::result::Result<T, ExprError>;

#[derive(Debug)]
enum ExprError {
    Parse(String),
}

impl std::error::Error for ExprError {}

impl Display for ExprError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parse(s) => write!(f, "{}", s),
        }
    }
}

// Token enum to sign number, operator, ( )
#[derive(Debug, Clone, Copy)]
enum Token {
    Number(i32),
    Plus,       // +
    Minus,      // -
    Multiply,   // *
    Divide,     // /
    Power,      // ^
    LeftParen,  // (
    RightParen, // )
}

// left association
const ASSOC_LEFT: i32 = 0;
// right association
const ASSOC_RIGHT: i32 = 1;

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Number(n) => n.to_string(),
                Self::Plus => "+".to_string(),
                Self::Minus => "-".to_string(),
                Self::Multiply => "*".to_string(),
                Self::Divide => "/".to_string(),
                Self::Power => "^".to_string(),
                Self::LeftParen => "(".to_string(),
                Self::RightParen => ")".to_string(),
            }
        )
    }
}

impl Token {
    fn is_operator(&self) -> bool {
        match self {
            Token::Plus | Token::Minus | Token::Multiply | Token::Divide | Token::Power => true,
            _ => false,
        }
    }

    fn precedence(&self) -> i32 {
        match self {
            Token::Plus | Token::Minus => 1,
            Token::Multiply | Token::Divide => 2,
            Token::Power => 3,
            _ => 0,
        }
    }

    // get the precedence of operator
    fn assoc(&self) -> i32 {
        match self {
            Token::Power => ASSOC_RIGHT,
            _ => ASSOC_LEFT,
        }
    }

    // compute based on opearator
    fn compute(&self, l: i32, r: i32) -> Option<i32> {
        match self {
            Token::Plus => Some(l + r),
            Token::Minus => Some(l - r),
            Token::Multiply => Some(l * r),
            Token::Divide => Some(l / r),
            Token::Power => Some(l.pow(r as u32)),
            _ => None,
        }
    }
}

// pares string to token sequnce
struct Tokenizer<'a> {
    tokens: Peekable<Chars<'a>>,
}

impl<'a> Tokenizer<'a> {
    fn new(expr: &'a str) -> Self {
        Self {
            tokens: expr.chars().peekable(),
        }
    }

    fn consume_whitespace(&mut self) {
        while let Some(&c) = self.tokens.peek() {
            if c.is_whitespace() {
                self.tokens.next();
            } else {
                break;
            }
        }
    }

    fn scan_number(&mut self) -> Option<Token> {
        let mut num = String::new();
        while let Some(&c) = self.tokens.peek() {
            if c.is_numeric() {
                num.push(c);
                self.tokens.next();
            } else {
                break;
            }
        }
        match num.parse() {
            Ok(n) => Some(Token::Number(n)),
            Err(_) => None,
        }
    }

    fn scan_operator(&mut self) -> Option<Token> {
        match self.tokens.next() {
            Some('+') => Some(Token::Plus),
            Some('-') => Some(Token::Minus),
            Some('*') => Some(Token::Multiply),
            Some('/') => Some(Token::Divide),
            Some('^') => Some(Token::Power),
            Some('(') => Some(Token::LeftParen),
            Some(')') => Some(Token::RightParen),
            _ => None,
        }
    }
}

// achieve iterator trait to tokenizer
impl<'a> Iterator for Tokenizer<'a> {
    type Item = Token;

    fn next(&mut self) -> Option<Self::Item> {
        // consume whitespace
        self.consume_whitespace();
        match self.tokens.peek() {
            Some(c) if c.is_numeric() => self.scan_number(),
            Some(_) => self.scan_operator(),
            None => return None,
        }
    }
}

struct Expr<'a> {
    iter: Peekable<Tokenizer<'a>>,
}

impl<'a> Expr<'a> {
    pub fn new(src: &'a str) -> Self {
        Self {
            iter: Tokenizer::new(src).peekable(),
        }
    }

    pub fn eval(&mut self) -> Result<i32> {
        let result = self.compute_expr(1)?;
        if self.iter.peek().is_some() {
            return Err(ExprError::Parse("Unexcepted end of expr".into()));
        }
        Ok(result)
    }

    // compute single token or sub-expr
    fn compute_atom(&mut self) -> Result<i32> {
        match self.iter.peek() {
            // number, direct return
            Some(Token::Number(n)) => {
                let val = *n;
                self.iter.next();
                return Ok(val);
            }
            // (, recursively compute val in ()
            Some(Token::LeftParen) => {
                self.iter.next();
                let result = self.compute_expr(1)?;
                match self.iter.next() {
                    Some(Token::RightParen) => (),
                    _ => return Err(ExprError::Parse("Unexcepted character".into())),
                }
                return Ok(result);
            }
            _ => {
                return Err(ExprError::Parse(
                    "Expecting a number or left parenthesis".into(),
                ))
            }
        }
    }

    fn compute_expr(&mut self, min_prec: i32) -> Result<i32> {
        let mut atom_lhs = self.compute_atom()?;

        loop {
            let cur_token = self.iter.peek();
            if cur_token.is_none() {
                break;
            }
            let token = *cur_token.unwrap();
            if !token.is_operator() || token.precedence() < min_prec {
                break;
            }

            let mut next_prec = token.precedence();
            if token.assoc() == ASSOC_LEFT {
                next_prec += 1;
            }

            self.iter.next();

            let atom_rhs = self.compute_expr(next_prec)?;

            match token.compute(atom_lhs, atom_rhs) {
                Some(res) => atom_lhs = res,
                None => return Err(ExprError::Parse("Unexcepted expr".into())),
            }
        }
        Ok(atom_lhs)
    }
}

fn main() {
    let src = "92 + 5 + 5 * 27 - (92 - 12) / 4 + 26";
    let mut expr = Expr::new(src);
    let result = expr.eval();
    println!("result = {:?}", result);
}
