package engine

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/Dziqha/BensinDB/pkg/parser"
	"github.com/Dziqha/BensinDB/pkg/query"
	"github.com/Dziqha/BensinDB/pkg/tangki"
)

type Engine struct {
	tangkis map[string]*tangki.Tangki
	mu      sync.RWMutex
	file    string 
	dirty   bool  
}

func OpenTangki(filepath string) (*Engine, error) {
	eng := &Engine{
		tangkis: make(map[string]*tangki.Tangki),
		file:    filepath,
		dirty:   false,
	}

	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		return eng, nil
	}

	if err := Load(eng, filepath); err != nil {
		return nil, err
	}

	eng.dirty = false

	return eng, nil
}

func (e *Engine) Jalankan(fql string) error {
	p := parser.NewParser(fql)
	q, err := p.Parse()
	if err != nil {
		return fmt.Errorf("parse error: %v", err)
	}
	
	e.mu.Lock()
	defer e.mu.Unlock()
	
	switch q.Type {
	case "CREATE":
		err = e.createTangki(q)
	case "INSERT":
		err = e.insertData(q)
	case "UPDATE":
		err = e.updateData(q)
	case "DELETE":
		err = e.deleteData(q)
	case "JOIN":
		err = e.joinTangki(q)
	case "UNION":
		err = e.unionTangki(q)
	default:
		return fmt.Errorf("perintah tidak didukung untuk Jalankan: %s", q.Type)
	}
	
	if err == nil {
		e.dirty = true
	}
	
	return err
}

func (e *Engine) Query(fql string) ([]tangki.Row, error) {
	p := parser.NewParser(fql)
	q, err := p.Parse()
	if err != nil {
		return nil, fmt.Errorf("parse error: %v", err)
	}
	
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	switch q.Type {
	case "SELECT":
		return e.selectData(q)
	case "ORDER":
		return e.orderData(q)
	case "GROUP":
		return e.groupData(q)
	default:
		return nil, fmt.Errorf("perintah tidak didukung untuk Query: %s", q.Type)
	}
}

func (e *Engine) GetTangki(name string) (*tangki.Tangki, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	tangki, exists := e.tangkis[name]
	return tangki, exists
}

func (e *Engine) getTangkiNoLock(name string) (*tangki.Tangki, bool) {
	tangki, exists := e.tangkis[name]
	return tangki, exists
}

func (e *Engine) ListTangki() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	return e.listTangkiNoLock()
}

func (e *Engine) listTangkiNoLock() []string {
	names := make([]string, 0, len(e.tangkis))
	for name := range e.tangkis {
		names = append(names, name)
	}
	return names
}

func (e *Engine) DropTangki(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	if _, exists := e.tangkis[name]; !exists {
		return fmt.Errorf("tangki '%s' tidak ditemukan", name)
	}
	
	delete(e.tangkis, name)
	e.dirty = true
	return nil
}


func (e *Engine) createTangki(q *parser.Query) error {
    if _, exists := e.tangkis[q.Tangki]; exists {
        return fmt.Errorf("tangki '%s' sudah ada", q.Tangki)
    }
    
    columns := make([]tangki.Column, len(q.Columns))
    for i, colDef := range q.Columns {
        parts := strings.Split(colDef, ":")
        if len(parts) != 2 {
            return fmt.Errorf("format kolom salah: %s", colDef)
        }
        columns[i] = tangki.Column{
            Name: strings.TrimSpace(parts[0]), 
            Type: strings.TrimSpace(parts[1]),
        }
    }
    
    e.tangkis[q.Tangki] = tangki.NewTangki(q.Tangki, columns)
    return nil
}

func (e *Engine) insertData(q *parser.Query) error {
	tangki, exists := e.tangkis[q.Tangki]
	if !exists {
		return fmt.Errorf("tangki '%s' tidak ditemukan", q.Tangki)
	}
	
	return tangki.AddRow(q.Values...)
}

func (e *Engine) selectData(q *parser.Query) ([]tangki.Row, error) {
	tangki, exists := e.tangkis[q.Tangki]
	if !exists {
		return nil, fmt.Errorf("tangki '%s' tidak ditemukan", q.Tangki)
	}

	condition := e.buildConditionFunc(tangki, q.Condition)
	return tangki.SelectRows(q.Columns, condition)
}

func (e *Engine) updateData(q *parser.Query) error {
    tangki, exists := e.tangkis[q.Tangki]
    if !exists {
        return fmt.Errorf("tangki '%s' tidak ditemukan", q.Tangki)
    }
    
    column := q.Columns[0] // Nama kolom (string)
    value := q.Values[0]   // Nilai baru
    
    if expr, ok := value.(map[string]interface{}); ok {
        if expr["type"] == "expression" {
            return e.updateWithExpression(tangki, column, expr, q.Condition)
        }
    }
    
    condition := e.buildConditionFunc(tangki, q.Condition)
    return tangki.UpdateRows(column, value, condition)
}

func (e *Engine) updateWithExpression(tangki *tangki.Tangki, column string, expr map[string]interface{}, cond *parser.Condition) error {
    targetIndex := tangki.GetColumnIndex(column)
    sourceColName := expr["column"].(string)
    sourceIndex := tangki.GetColumnIndex(sourceColName)

    if targetIndex == -1 {
        return fmt.Errorf("kolom target '%s' tidak ditemukan", column)
    }
    if sourceIndex == -1 {
        return fmt.Errorf("kolom sumber '%s' tidak ditemukan", sourceColName)
    }

    operator := expr["operator"].(string)
    value := expr["value"]
    
    conditionFunc := e.buildConditionFunc(tangki, cond)
    
    for i := range tangki.Rows {
        if conditionFunc(tangki.Rows[i]) {
            currentVal := toFloat(tangki.Rows[i][sourceIndex])
            
            var newVal float64
            valFloat := toFloat(value)

            switch operator {
            case "+": newVal = currentVal + valFloat
            case "-": newVal = currentVal - valFloat
            case "*": newVal = currentVal * valFloat
            case "/": newVal = currentVal / valFloat
            default: return fmt.Errorf("operator tidak didukung: %s", operator)
            }
            
            tangki.Rows[i][targetIndex] = newVal
        }
    }
    return nil
}
func (e *Engine) deleteData(q *parser.Query) error {
	tangki, exists := e.tangkis[q.Tangki]
	if !exists {
		return fmt.Errorf("tangki '%s' tidak ditemukan", q.Tangki)
	}
	
	condition := e.buildConditionFunc(tangki, q.Condition)
	return tangki.DeleteRows(condition)
}

func (e *Engine) joinTangki(q *parser.Query) error {
	tangki1, exists1 := e.tangkis[q.JoinInfo.Tangki1]
	tangki2, exists2 := e.tangkis[q.JoinInfo.Tangki2]
	
	if !exists1 {
		return fmt.Errorf("tangki '%s' tidak ditemukan", q.JoinInfo.Tangki1)
	}
	if !exists2 {
		return fmt.Errorf("tangki '%s' tidak ditemukan", q.JoinInfo.Tangki2)
	}
	
	result := query.Join(tangki1, tangki2, q.JoinInfo.OnColumn1, q.JoinInfo.OnColumn2)
	result.Name = q.JoinInfo.NewTangki
	e.tangkis[q.JoinInfo.NewTangki] = result
	
	return nil
}

func (e *Engine) unionTangki(q *parser.Query) error {
	tangkis := make([]*tangki.Tangki, len(q.UnionInfo.Tangkis))
	
	for i, name := range q.UnionInfo.Tangkis {
		t, exists := e.tangkis[name]
		if !exists {
			return fmt.Errorf("tangki '%s' tidak ditemukan", name)
		}
		tangkis[i] = t
	}
	
	result := query.Union(tangkis...)
	result.Name = q.UnionInfo.NewTangki
	e.tangkis[q.UnionInfo.NewTangki] = result
	
	return nil
}

func (e *Engine) orderData(q *parser.Query) ([]tangki.Row, error) {
	tangki, exists := e.tangkis[q.Tangki]
	if !exists {
		return nil, fmt.Errorf("tangki '%s' tidak ditemukan", q.Tangki)
	}
	
	return query.OrderBy(tangki, q.OrderInfo.Column, q.OrderInfo.Ascending), nil
}

func (e *Engine) groupData(q *parser.Query) ([]tangki.Row, error) {
	tangki, exists := e.tangkis[q.Tangki]
	if !exists {
		return nil, fmt.Errorf("tangki '%s' tidak ditemukan", q.Tangki)
	}
	
	return query.GroupBy(tangki, q.GroupInfo.Column, q.GroupInfo.AggregateFunc, q.GroupInfo.AggregateCol)
}

func (e *Engine) buildConditionFunc(t *tangki.Tangki, cond *parser.Condition) func(tangki.Row) bool {
    if cond == nil {
        return func(row tangki.Row) bool { return true }
    }

    colIndex := -1
    for i, col := range t.Columns {
        if col.Name == cond.Column {
            colIndex = i
            break
        }
    }

    if colIndex == -1 {
        return func(row tangki.Row) bool { return false }
    }

    return func(row tangki.Row) bool {
        val := row[colIndex] 
        return compareValues(val, cond.Operator, cond.Value)
    }
}

func (e *Engine) registerTangki(t *tangki.Tangki) {
	e.tangkis[t.Name] = t
}


func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.dirty {
		return nil
	}

	return e.saveNoLock()
}

func compareValues(a interface{}, op string, b interface{}) bool {
    switch va := a.(type) {
    case int64:
        if vb, ok := b.(int64); ok {
            return evalInt64(va, op, vb)
        }
    case float64:
        if vb, ok := b.(float64); ok {
            return evalFloat64(va, op, vb)
        }
    case string:
        if vb, ok := b.(string); ok {
            return evalString(va, op, vb)
        }
    }

    return evalFloat64(toFloat(a), op, toFloat(b))
}

func evalInt64(a int64, op string, b int64) bool {
    switch op {
    case "=":  return a == b
    case "!=": return a != b
    case ">":  return a > b
    case "<":  return a < b
    case ">=": return a >= b
    case "<=": return a <= b
    }
    return false
}

func evalFloat64(a float64, op string, b float64) bool {
    switch op {
    case "=":  return a == b
    case "!=": return a != b
    case ">":  return a > b
    case "<":  return a < b
    case ">=": return a >= b
    case "<=": return a <= b
    }
    return false
}

func evalString(a string, op string, b string) bool {
    switch op {
    case "=":  return a == b
    case "!=": return a != b
    default:   return false 
    }
}

func toFloat(val interface{}) float64 {
    switch v := val.(type) {
    case float64: return v
    case int64:   return float64(v)
    case int:     return float64(v)
    case string:
        f, _ := strconv.ParseFloat(v, 64)
        return f
    default:
        return 0
    }
}