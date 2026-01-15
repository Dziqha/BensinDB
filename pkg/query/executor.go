package query

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/Dziqha/BensinDB/pkg/tangki"
)

func Join(t1, t2 *tangki.Tangki, col1, col2 string) *tangki.Tangki {
    idx1 := t1.GetColumnIndex(col1)
    idx2 := t2.GetColumnIndex(col2)

    if idx1 == -1 || idx2 == -1 { return nil }

    allColumns := append(t1.Columns, t2.Columns...)
    result := tangki.NewTangki("joined", allColumns)

    for _, row1 := range t1.Rows {
        for _, row2 := range t2.Rows {
            if compareValues(row1[idx1], "=", row2[idx2]) {
                
                joinedRow := make(tangki.Row, len(row1)+len(row2))
                copy(joinedRow, row1)
                copy(joinedRow[len(row1):], row2)
                
                result.Rows = append(result.Rows, joinedRow)
            }
        }
    }
    return result
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

    return evalFloat64(toFloatAJAX(a), op, toFloatAJAX(b))
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

func toFloatAJAX(val interface{}) float64 {
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



func Union(tangkis ...*tangki.Tangki) *tangki.Tangki {
	if len(tangkis) == 0 {
		return tangki.NewTangki("union", []tangki.Column{})
	}
	
	result := tangkis[0].Clone("union")
	
	for i := 1; i < len(tangkis); i++ {
		for _, row := range tangkis[i].Rows {
			if columnsMatch(result.Columns, tangkis[i].Columns) {
				result.Rows = append(result.Rows, row.Clone())
			}
		}
	}
	
	return result
}

func OrderBy(t *tangki.Tangki, colName string, asc bool) []tangki.Row {
    idx := t.GetColumnIndex(colName)
    if idx == -1 {
        return t.Rows
    }

    sortedRows := make([]tangki.Row, len(t.Rows))
    copy(sortedRows, t.Rows)

    sort.Slice(sortedRows, func(i, j int) bool {
        val1 := toFloatAJAX(sortedRows[i][idx])
        val2 := toFloatAJAX(sortedRows[j][idx])

        if asc {
            return val1 < val2
        }
        return val1 > val2
    })

    return sortedRows
}

func GroupBy(t *tangki.Tangki, groupCol, aggFunc, aggCol string) ([]tangki.Row, error) {
    groupIdx := t.GetColumnIndex(groupCol)
    aggIdx := -1
    if aggCol != "" {
        aggIdx = t.GetColumnIndex(aggCol)
    }

    if groupIdx == -1 {
        return nil, fmt.Errorf("kolom group '%s' tidak ditemukan", groupCol)
    }

    groups := make(map[string][]tangki.Row)
    for _, row := range t.Rows {
        key := fmt.Sprintf("%v", row[groupIdx]) 
        groups[key] = append(groups[key], row)
    }

    results := make([]tangki.Row, 0, len(groups))

    for key, rows := range groups {
        result := make(tangki.Row, 2) 
        result[0] = key 

        if aggFunc != "" && aggCol != "" {
            aggValue, err := aggregateByIndex(rows, aggFunc, aggIdx)
            if err != nil {
                return nil, err
            }
            result[1] = aggValue
        } else {
            result[1] = len(rows) 
        }
        results = append(results, result)
    }

    return results, nil
}
func aggregateByIndex(rows []tangki.Row, funcName string, colIdx int) (float64, error) {
    if len(rows) == 0 {
        return 0, nil
    }

    var result float64
    
    switch funcName {
    case "SUM", "AVG":
        for _, row := range rows {
            result += toFloatAJAX(row[colIdx])
        }
        if funcName == "AVG" {
            result = result / float64(len(rows))
        }
    case "MAX":
        result = toFloatAJAX(rows[0][colIdx])
        for _, row := range rows {
            val := toFloatAJAX(row[colIdx])
            if val > result {
                result = val
            }
        }
    case "MIN":
        result = toFloatAJAX(rows[0][colIdx])
        for _, row := range rows {
            val := toFloatAJAX(row[colIdx])
            if val < result {
                result = val
            }
        }
    case "COUNT":
        return float64(len(rows)), nil
    default:
        return 0, fmt.Errorf("fungsi agregasi tidak dikenal: %s", funcName)
    }

    return result, nil
}

func columnsMatch(cols1, cols2 []tangki.Column) bool {
	if len(cols1) != len(cols2) {
		return false
	}
	
	for i := range cols1 {
		if cols1[i].Name != cols2[i].Name || cols1[i].Type != cols2[i].Type {
			return false
		}
	}
	
	return true
}