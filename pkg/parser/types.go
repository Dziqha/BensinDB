package parser

// TokenType represents FQL token types
type TokenType int

const (
	// Commands
	TOKEN_BUAT TokenType = iota
	TOKEN_TANGKI
	TOKEN_ISI
	TOKEN_NILAI
	TOKEN_PILIH
	TOKEN_DARI
	TOKEN_DIMANA
	TOKEN_ATUR
	TOKEN_SET
	TOKEN_BAKAR
	TOKEN_GABUNG
	TOKEN_DAN
	TOKEN_KE
	TOKEN_MENJADI
	TOKEN_CAMPUR
	TOKEN_SATUKAN
	TOKEN_URUTKAN
	TOKEN_BERDASARKAN
	TOKEN_GRUPKAN
	TOKEN_MENAIK
	TOKEN_MENURUN
	
	// Data Types
	TOKEN_INT
	TOKEN_FLOAT
	TOKEN_TEKS
	
	// Operators
	TOKEN_EQUALS
	TOKEN_GT
	TOKEN_LT
	TOKEN_GTE
	TOKEN_LTE
	TOKEN_NEQ
	TOKEN_PLUS
	TOKEN_MINUS
	TOKEN_MULTIPLY
	TOKEN_DIVIDE
	
	// Aggregates
	TOKEN_SUM
	TOKEN_AVG
	TOKEN_COUNT
	TOKEN_MAX
	TOKEN_MIN
	
	// Literals
	TOKEN_IDENTIFIER
	TOKEN_NUMBER
	TOKEN_STRING
	TOKEN_ASTERISK
	
	// Punctuation
	TOKEN_LPAREN
	TOKEN_RPAREN
	TOKEN_COMMA
	TOKEN_DOT
	TOKEN_EOF
	TOKEN_UNKNOWN
)

// Token represents a lexical token
type Token struct {
	Type  TokenType
	Value string
	Pos   int
}

// Query represents parsed FQL query
type Query struct {
	Type      string
	Tangki    string
	Columns   []string
	Values    []interface{}
	Condition *Condition
	JoinInfo  *JoinInfo
	OrderInfo *OrderInfo
	GroupInfo *GroupInfo
	UnionInfo *UnionInfo
}

// Condition represents WHERE clause
type Condition struct {
	Column   string
	Operator string
	Value    interface{}
}

// JoinInfo represents JOIN operation
type JoinInfo struct {
	Tangki1   string
	Tangki2   string
	NewTangki string
	OnColumn1 string
	OnColumn2 string
}

// UnionInfo represents UNION operation
type UnionInfo struct {
	Tangkis   []string
	NewTangki string
}

// OrderInfo represents ORDER BY
type OrderInfo struct {
	Column    string
	Ascending bool
}

// GroupInfo represents GROUP BY with aggregate
type GroupInfo struct {
	Column        string
	AggregateFunc string
	AggregateCol  string
}