package parser

import (
	"fmt"
	"strconv"
	"strings"
)

type Parser struct {
	lexer       *Lexer
	currentPoint Token
}

func NewParser(input string) *Parser {
	lexer := NewLexer(input)
	p := &Parser{
		lexer:       lexer,
	}
	p.nextToken()
	return p
}

func (p *Parser) nextToken() {
    p.currentPoint = p.lexer.NextToken()
}

// Parse parses the FQL query
func (p *Parser) Parse() (*Query, error) {
	if p.currentPoint.Type == TOKEN_EOF {
		return nil, fmt.Errorf("query kosong")
	}

	switch p.currentPoint.Type {
	case TOKEN_BUAT:
		return p.parseCreate()
	case TOKEN_ISI:
		return p.parseInsert()
	case TOKEN_PILIH:
		return p.parseSelect()
	case TOKEN_ATUR:
		return p.parseUpdate()
	case TOKEN_BAKAR:
		return p.parseDelete()
	case TOKEN_GABUNG:
		return p.parseJoin()
	case TOKEN_CAMPUR, TOKEN_SATUKAN:
		return p.parseUnion()
	case TOKEN_URUTKAN:
		return p.parseOrder()
	case TOKEN_GRUPKAN:
		return p.parseGroup()
	default:
		return nil, fmt.Errorf("perintah tidak dikenal: %s", p.currentPoint.Value)
	}
}

// BUAT TANGKI nama (kolom1 TIPE, kolom2 TIPE, ...)
func (p *Parser) parseCreate() (*Query, error) {
	p.consume(TOKEN_BUAT)
	p.consume(TOKEN_TANGKI)
	
	tangki := p.consume(TOKEN_IDENTIFIER).Value
	p.consume(TOKEN_LPAREN)
	
	columns := []string{}
	for p.peek().Type != TOKEN_RPAREN {
		colName := p.consume(TOKEN_IDENTIFIER).Value
		colType := p.consumeAny(TOKEN_INT, TOKEN_FLOAT, TOKEN_TEKS).Value
		columns = append(columns, colName+":"+strings.ToUpper(colType))
		
		if p.peek().Type == TOKEN_COMMA {
			p.consume(TOKEN_COMMA)
		}
	}
	
	p.consume(TOKEN_RPAREN)
	
	return &Query{
		Type:    "CREATE",
		Tangki:  tangki,
		Columns: columns,
	}, nil
}

// ISI TANGKI nama NILAI (val1, val2, ...)
func (p *Parser) parseInsert() (*Query, error) {
    p.consume(TOKEN_ISI)

    for p.peek().Type == TOKEN_KE || p.peek().Type == TOKEN_TANGKI {
        p.nextToken() 
    }

    tangki := p.consume(TOKEN_IDENTIFIER).Value

    if p.peek().Type == TOKEN_NILAI {
        p.consume(TOKEN_NILAI)
    }

    p.consume(TOKEN_LPAREN)
    
    values := []interface{}{}
    for p.peek().Type != TOKEN_RPAREN {
        val := p.parseValue()
        values = append(values, val)
        
        if p.peek().Type == TOKEN_COMMA {
            p.consume(TOKEN_COMMA)
        }
    }
    p.consume(TOKEN_RPAREN)

    return &Query{
        Type:   "INSERT",
        Tangki: tangki,
        Values: values,
    }, nil
}

// PILIH kolom1, kolom2 DARI tangki [DIMANA kondisi]
func (p *Parser) parseSelect() (*Query, error) {
	p.consume(TOKEN_PILIH)
	
	columns := []string{}
	if p.peek().Type == TOKEN_ASTERISK {
		p.consume(TOKEN_ASTERISK)
		columns = append(columns, "*")
	} else {
		for {
			col := p.consume(TOKEN_IDENTIFIER).Value
			columns = append(columns, col)
			
			if p.peek().Type != TOKEN_COMMA {
				break
			}
			p.consume(TOKEN_COMMA)
		}
	}
	
	p.consume(TOKEN_DARI)
	tangki := p.consume(TOKEN_IDENTIFIER).Value
	
	var condition *Condition
	if p.peek().Type == TOKEN_DIMANA {
		p.consume(TOKEN_DIMANA)
		condition = p.parseCondition()
	}
	
	return &Query{
		Type:      "SELECT",
		Tangki:    tangki,
		Columns:   columns,
		Condition: condition,
	}, nil
}

// ATUR TANGKI nama SET kolom=nilai DIMANA kondisi
func (p *Parser) parseUpdate() (*Query, error) {
	p.consume(TOKEN_ATUR)
	p.consume(TOKEN_TANGKI)
	
	tangki := p.consume(TOKEN_IDENTIFIER).Value
	p.consume(TOKEN_SET)
	
	column := p.consume(TOKEN_IDENTIFIER).Value
	p.consume(TOKEN_EQUALS)
	
	value := p.parseExpression()
	
	p.consume(TOKEN_DIMANA)
	condition := p.parseCondition()
	
	return &Query{
		Type:      "UPDATE",
		Tangki:    tangki,
		Columns:   []string{column},
		Values:    []interface{}{value},
		Condition: condition,
	}, nil
}

// BAKAR TANGKI nama DIMANA kondisi
func (p *Parser) parseDelete() (*Query, error) {
	p.consume(TOKEN_BAKAR)
	p.consume(TOKEN_TANGKI)
	
	tangki := p.consume(TOKEN_IDENTIFIER).Value
	p.consume(TOKEN_DIMANA)
	
	condition := p.parseCondition()
	
	return &Query{
		Type:      "DELETE",
		Tangki:    tangki,
		Condition: condition,
	}, nil
}

// GABUNG tangki1 DAN tangki2 MENJADI tangki_baru DIMANA tangki1.kolom=tangki2.kolom
func (p *Parser) parseJoin() (*Query, error) {
	p.consume(TOKEN_GABUNG)
	
	tangki1 := p.consume(TOKEN_IDENTIFIER).Value
	p.consume(TOKEN_DAN)
	tangki2 := p.consume(TOKEN_IDENTIFIER).Value
	p.consume(TOKEN_MENJADI)
	newTangki := p.consume(TOKEN_IDENTIFIER).Value
	p.consume(TOKEN_DIMANA)
	
	col1Tangki := p.consume(TOKEN_IDENTIFIER).Value
	if col1Tangki != tangki1 {
		return nil, fmt.Errorf("expected tangki name '%s', got '%s'", tangki1, col1Tangki)
	}
	p.consume(TOKEN_DOT)
	col1 := p.consume(TOKEN_IDENTIFIER).Value

	p.consume(TOKEN_EQUALS)

	col2Tangki := p.consume(TOKEN_IDENTIFIER).Value
	if col2Tangki != tangki2 {
		return nil, fmt.Errorf("expected tangki name '%s', got '%s'", tangki2, col2Tangki)
	}
	p.consume(TOKEN_DOT)
	col2 := p.consume(TOKEN_IDENTIFIER).Value
	
	return &Query{
		Type: "JOIN",
		JoinInfo: &JoinInfo{
			Tangki1:   tangki1,
			Tangki2:   tangki2,
			NewTangki: newTangki,
			OnColumn1: col1,
			OnColumn2: col2,
		},
	}, nil
}

// CAMPUR TANGKI tangki1 + tangki2 MENJADI tangki_baru
// or SATUKAN tangki1, tangki2, ... MENJADI tangki_baru
func (p *Parser) parseUnion() (*Query, error) {
	isSatukan := p.peek().Type == TOKEN_SATUKAN
	p.nextToken()
	
	tangkis := []string{}
	
	if isSatukan {
		for {
			tangki := p.consume(TOKEN_IDENTIFIER).Value
			tangkis = append(tangkis, tangki)
			
			if p.peek().Type != TOKEN_COMMA {
				break
			}
			p.consume(TOKEN_COMMA)
		}
	} else {
		p.consume(TOKEN_TANGKI)
		tangki1 := p.consume(TOKEN_IDENTIFIER).Value
		tangkis = append(tangkis, tangki1)
		
		p.consume(TOKEN_PLUS)
		tangki2 := p.consume(TOKEN_IDENTIFIER).Value
		tangkis = append(tangkis, tangki2)
	}
	
	p.consume(TOKEN_MENJADI)
	newTangki := p.consume(TOKEN_IDENTIFIER).Value
	
	return &Query{
		Type: "UNION",
		UnionInfo: &UnionInfo{
			Tangkis:   tangkis,
			NewTangki: newTangki,
		},
	}, nil
}

// URUTKAN TANGKI nama BERDASARKAN kolom [MENAIK|MENURUN]
func (p *Parser) parseOrder() (*Query, error) {
	p.consume(TOKEN_URUTKAN)
	p.consume(TOKEN_TANGKI)
	
	tangki := p.consume(TOKEN_IDENTIFIER).Value
	p.consume(TOKEN_BERDASARKAN)
	column := p.consume(TOKEN_IDENTIFIER).Value
	
	ascending := true
	if p.peek().Type == TOKEN_MENURUN {
		p.consume(TOKEN_MENURUN)
		ascending = false
	} else if p.peek().Type == TOKEN_MENAIK {
		p.consume(TOKEN_MENAIK)
	}
	
	return &Query{
		Type:   "ORDER",
		Tangki: tangki,
		OrderInfo: &OrderInfo{
			Column:    column,
			Ascending: ascending,
		},
	}, nil
}

// GRUPKAN TANGKI nama BERDASARKAN kolom [SUM(kolom_target)]
func (p *Parser) parseGroup() (*Query, error) {
	p.consume(TOKEN_GRUPKAN)
	p.consume(TOKEN_TANGKI)
	
	tangki := p.consume(TOKEN_IDENTIFIER).Value
	p.consume(TOKEN_BERDASARKAN)
	column := p.consume(TOKEN_IDENTIFIER).Value
	
	aggFunc := ""
	aggCol := ""
	
	if p.peek().Type >= TOKEN_SUM && p.peek().Type <= TOKEN_MIN {
		aggFunc = strings.ToUpper(p.current().Value)
		p.nextToken()
		p.consume(TOKEN_LPAREN)
		aggCol = p.consume(TOKEN_IDENTIFIER).Value
		p.consume(TOKEN_RPAREN)
	}
	
	return &Query{
		Type:   "GROUP",
		Tangki: tangki,
		GroupInfo: &GroupInfo{
			Column:        column,
			AggregateFunc: aggFunc,
			AggregateCol:  aggCol,
		},
	}, nil
}


func (p *Parser) parseCondition() *Condition {
	column := p.consume(TOKEN_IDENTIFIER).Value
	
	operator := ""
	switch p.peek().Type {
	case TOKEN_EQUALS:
		operator = "="
	case TOKEN_GT:
		operator = ">"
	case TOKEN_LT:
		operator = "<"
	case TOKEN_GTE:
		operator = ">="
	case TOKEN_LTE:
		operator = "<="
	case TOKEN_NEQ:
		operator = "!="
	default:
		operator = "="
	}
	p.nextToken()
	
	value := p.parseValue()
	
	return &Condition{
		Column:   column,
		Operator: operator,
		Value:    value,
	}
}

func (p *Parser) parseExpression() interface{} {	
	if p.peek().Type == TOKEN_IDENTIFIER {
		firstToken := p.consume(TOKEN_IDENTIFIER).Value
		
		if p.peek().Type >= TOKEN_PLUS && p.peek().Type <= TOKEN_DIVIDE {
			op := p.current().Value
			p.nextToken()
			val := p.parseValue()
			
			return map[string]interface{}{
				"type":     "expression",
				"column":   firstToken,
				"operator": op,
				"value":    val,
			}
		}
		
		return firstToken
	}
	
	return p.parseValue()
}

func (p *Parser) parseValue() interface{} {
	token := p.current()
	p.nextToken()
	
	switch token.Type {
	case TOKEN_NUMBER:
		if strings.Contains(token.Value, ".") {
			f, _ := strconv.ParseFloat(token.Value, 64)
			return f
		}
		i, _ := strconv.Atoi(token.Value)
		return i
	case TOKEN_STRING:
		return token.Value
	case TOKEN_IDENTIFIER:
		return token.Value
	default:
		return token.Value
	}
}

func (p *Parser) current() Token {
	return p.currentPoint
}

func (p *Parser) peek() Token {
	return p.current()
}

func (p *Parser) consume(expected TokenType) Token {
	token := p.current()
	if token.Type != expected {
		panic(fmt.Sprintf("expected %d, got %d (%s)", expected, token.Type, token.Value))
	}
	p.nextToken()
	return token
}

func (p *Parser) consumeAny(types ...TokenType) Token {
	token := p.current()
	for _, t := range types {
		if token.Type == t {
			p.nextToken()
			return token
		}
	}
	panic(fmt.Sprintf("unexpected token: %s", token.Value))
}