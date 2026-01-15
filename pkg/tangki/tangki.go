package tangki

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type Column struct {
	Name string
	Type string // "INT", "FLOAT", "TEKS"
}

type Tangki struct {
	Name    string
	Columns []Column
	Rows    []Row
	pool    []interface{} 
}

func NewTangki(name string, columns []Column) *Tangki {
	return &Tangki{
		Name:    name,
		Columns: columns,
		Rows:    make([]Row, 0),
	}
}


func (t *Tangki) AddRow(values ...interface{}) error {
	numCols := len(t.Columns)
	if len(values) != numCols {
		return errors.New("jumlah nilai tidak sesuai")
	}

	if t.pool == nil || len(t.pool)+numCols > cap(t.pool) {
		newCap := 1000 * numCols
		if t.pool != nil {
			newCap = cap(t.pool) * 2
		}
		t.pool = make([]interface{}, 0, newCap)
	}

	start := len(t.pool)

	for i := 0; i < numCols; i++ {
		val := values[i]
		cType := t.Columns[i].Type

		
		if (cType == "INT" && isInt(val)) || 
		   (cType == "FLOAT" && isFloat(val)) || 
		   (cType == "TEKS" && isString(val)) {
			t.pool = append(t.pool, val)
		} else {
			converted, err := t.validateAndConvert(cType, val)
			if err != nil { return err }
			t.pool = append(t.pool, converted)
		}
	}

	t.Rows = append(t.Rows, Row(t.pool[start:start+numCols]))
	return nil
}

func isInt(v interface{}) bool    { _, ok := v.(int); return ok }
func isFloat(v interface{}) bool  { _, ok := v.(float64); return ok }
func isString(v interface{}) bool { _, ok := v.(string); return ok }


func (t *Tangki) UpdateRows(columnName string, value interface{}, condition func(Row) bool) error {
    colIndex := t.GetColumnIndex(columnName)
    if colIndex == -1 {
        return fmt.Errorf("kolom '%s' tidak ditemukan", columnName)
    }

    colType := t.Columns[colIndex].Type
    val, err := t.validateAndConvert(colType, value)
    if err != nil {
        return err
    }
    
    updated := 0
    for i := range t.Rows {
        if condition(t.Rows[i]) {
            t.Rows[i][colIndex] = val
            updated++
        }
    }
    
    if updated == 0 {
        return fmt.Errorf("tidak ada baris yang di-update")
    }
    
    return nil
}

func (t *Tangki) DeleteRows(condition func(Row) bool) error {
	newRows := make([]Row, 0)
	deleted := 0
	
	for _, row := range t.Rows {
		if !condition(row) {
			newRows = append(newRows, row)
		} else {
			deleted++
		}
	}
	
	if deleted == 0 {
		return fmt.Errorf("tidak ada baris yang dihapus")
	}
	
	t.Rows = newRows
	return nil
}

func (t *Tangki) SelectRows(columnNames []string, condition func(Row) bool) ([]Row, error) {
    isSelectAll := len(columnNames) == 1 && columnNames[0] == "*"
    var colIndices []int
    if !isSelectAll {
        colIndices = make([]int, len(columnNames))
        for i, name := range columnNames {
            idx := t.GetColumnIndex(name)
            if idx == -1 {
                return nil, fmt.Errorf("kolom '%s' tidak ditemukan", name)
            }
            colIndices[i] = idx
        }
    }

    count := 0
    for _, row := range t.Rows {
        if condition == nil || condition(row) {
            count++
        }
    }

    results := make([]Row, 0, count)

    for _, row := range t.Rows {
        if condition == nil || condition(row) {
            if isSelectAll {
                results = append(results, row)
            } else {
                selectedRow := make(Row, len(colIndices))
                for i, actualIdx := range colIndices {
                    selectedRow[i] = row[actualIdx]
                }
                results = append(results, selectedRow)
            }
        }
    }

    return results, nil
}


func (t *Tangki) GetAllRows() []Row {
	return t.Rows
}

func (t *Tangki) Clone(newName string) *Tangki {
	newTangki := &Tangki{
		Name:    newName,
		Columns: make([]Column, len(t.Columns)),
		Rows:    make([]Row, len(t.Rows)),
	}
	
	copy(newTangki.Columns, t.Columns)
	for i, row := range t.Rows {
		newTangki.Rows[i] = row.Clone()
	}
	
	return newTangki
}


func (t *Tangki) validateAndConvert(colType string, value interface{}) (interface{}, error) {
    switch colType {
    case "INT":
        switch v := value.(type) {
        case int:
            return v, nil
        case int64:
            return int(v), nil
        case float64:
            return int(v), nil
        case string:
            i, err := strconv.Atoi(v)
            if err != nil {
                return nil, err
            }
            return i, nil
        default:
            return nil, errors.New("tipe data tidak sesuai untuk INT")
        }
        
    case "FLOAT":
        switch v := value.(type) {
        case float64:
            return v, nil
        case int:
            return float64(v), nil
        case string:
            f, err := strconv.ParseFloat(v, 64)
            if err != nil {
                return nil, err
            }
            return f, nil
        default:
            return nil, errors.New("tipe data tidak sesuai untuk FLOAT")
        }
        
    case "TEKS":
        if s, ok := value.(string); ok {
            return s, nil
        }
        return fmt.Sprint(value), nil
        
    default:
        return nil, errors.New("tipe kolom tidak dikenal")
    }
}


func (t *Tangki) GetColumnIndex(name string) int {
    for i, col := range t.Columns {
        if strings.EqualFold(col.Name, name) {
            return i
        }
    }
    return -1
}