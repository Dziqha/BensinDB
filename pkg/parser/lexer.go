package parser

import (
	"strings"
	"unicode"
)

type Lexer struct {
	input string
	pos   int
	char  rune
}

var keywords = map[string]TokenType{
		"BUAT":        TOKEN_BUAT,
		"TANGKI":      TOKEN_TANGKI,
		"ISI":         TOKEN_ISI,
		"KE":          TOKEN_KE,
		"NILAI":       TOKEN_NILAI,
		"PILIH":       TOKEN_PILIH,
		"DARI":        TOKEN_DARI,
		"DIMANA":      TOKEN_DIMANA,
		"ATUR":        TOKEN_ATUR,
		"SET":         TOKEN_SET,
		"BAKAR":       TOKEN_BAKAR,
		"GABUNG":      TOKEN_GABUNG,
		"DAN":         TOKEN_DAN,
		"MENJADI":     TOKEN_MENJADI,
		"CAMPUR":      TOKEN_CAMPUR,
		"SATUKAN":     TOKEN_SATUKAN,
		"URUTKAN":     TOKEN_URUTKAN,
		"BERDASARKAN": TOKEN_BERDASARKAN,
		"GRUPKAN":     TOKEN_GRUPKAN,
		"MENAIK":      TOKEN_MENAIK,
		"MENURUN":     TOKEN_MENURUN,
		"INT":         TOKEN_INT,
		"FLOAT":       TOKEN_FLOAT,
		"TEKS":        TOKEN_TEKS,
		"SUM":         TOKEN_SUM,
		"AVG":         TOKEN_AVG,
		"COUNT":       TOKEN_COUNT,
		"MAX":         TOKEN_MAX,
		"MIN":         TOKEN_MIN,
	}

func NewLexer(input string) *Lexer {
	l := &Lexer{input: input, pos: 0}
	if len(input) > 0 {
		l.char = rune(input[0])
	}
	return l
}

func (l *Lexer) NextToken() Token {
	l.skipWhitespace()
	
	if l.pos >= len(l.input) {
		return Token{Type: TOKEN_EOF, Value: "", Pos: l.pos}
	}
	
	switch l.char {
	case '(':
		token := Token{Type: TOKEN_LPAREN, Value: "(", Pos: l.pos}
		l.advance()
		return token
	case ')':
		token := Token{Type: TOKEN_RPAREN, Value: ")", Pos: l.pos}
		l.advance()
		return token
	case ',':
		token := Token{Type: TOKEN_COMMA, Value: ",", Pos: l.pos}
		l.advance()
		return token
	case '.':
		token := Token{Type: TOKEN_DOT, Value: ".", Pos: l.pos}
		l.advance()
		return token
	case '*':
		token := Token{Type: TOKEN_ASTERISK, Value: "*", Pos: l.pos}
		l.advance()
		return token
	case '=':
		token := Token{Type: TOKEN_EQUALS, Value: "=", Pos: l.pos}
		l.advance()
		return token
	case '>':
		pos := l.pos
		l.advance()
		if l.char == '=' {
			l.advance()
			return Token{Type: TOKEN_GTE, Value: ">=", Pos: pos}
		}
		return Token{Type: TOKEN_GT, Value: ">", Pos: pos}
	case '<':
		pos := l.pos
		l.advance()
		if l.char == '=' {
			l.advance()
			return Token{Type: TOKEN_LTE, Value: "<=", Pos: pos}
		}
		return Token{Type: TOKEN_LT, Value: "<", Pos: pos}
	case '!':
		pos := l.pos
		l.advance()
		if l.char == '=' {
			l.advance()
			return Token{Type: TOKEN_NEQ, Value: "!=", Pos: pos}
		}
	case '+':
		token := Token{Type: TOKEN_PLUS, Value: "+", Pos: l.pos}
		l.advance()
		return token
	case '-':
		token := Token{Type: TOKEN_MINUS, Value: "-", Pos: l.pos}
		l.advance()
		return token
	case '/':
		token := Token{Type: TOKEN_DIVIDE, Value: "/", Pos: l.pos}
		l.advance()
		return token
	case '\'', '"':
		return l.readString()
	}
	
	if unicode.IsDigit(l.char) {
		return l.readNumber()
	}
	
	if unicode.IsLetter(l.char) || l.char == '_' {
		return l.readIdentifier()
	}
	
	token := Token{Type: TOKEN_UNKNOWN, Value: string(l.char), Pos: l.pos}
	l.advance()
	return token
}

func (l *Lexer) GetAllTokens() []Token {
	tokens := []Token{}
	for {
		token := l.NextToken()
		tokens = append(tokens, token)
		if token.Type == TOKEN_EOF {
			break
		}
	}
	return tokens
}

func (l *Lexer) advance() {
	l.pos++
	if l.pos >= len(l.input) {
		l.char = 0
	} else {
		l.char = rune(l.input[l.pos])
	}
}

func (l *Lexer) skipWhitespace() {
	for l.char == ' ' || l.char == '\t' || l.char == '\n' || l.char == '\r' {
		l.advance()
	}
}

func (l *Lexer) readString() Token {
	quote := l.char
	pos := l.pos
	l.advance() // skip opening quote
	
	start := l.pos
	for l.char != quote && l.pos < len(l.input) {
		l.advance()
	}
	
	value := l.input[start:l.pos]
	l.advance() // skip closing quote
	
	return Token{Type: TOKEN_STRING, Value: value, Pos: pos}
}

func (l *Lexer) readNumber() Token {
	pos := l.pos
	start := l.pos
	
	for unicode.IsDigit(l.char) || l.char == '.' {
		l.advance()
	}
	
	value := l.input[start:l.pos]
	return Token{Type: TOKEN_NUMBER, Value: value, Pos: pos}
}

func (l *Lexer) readIdentifier() Token {
	pos := l.pos
	start := l.pos
	
	for unicode.IsLetter(l.char) || unicode.IsDigit(l.char) || l.char == '_' {
		l.advance()
	}
	
	value := l.input[start:l.pos]
	tokenType := l.lookupKeyword(value)
	
	return Token{Type: tokenType, Value: value, Pos: pos}
}

func (l *Lexer) lookupKeyword(word string) TokenType {
	upper := strings.ToUpper(word)
	if tokenType, exists := keywords[upper]; exists {
		return tokenType
	}
	
	return TOKEN_IDENTIFIER
}