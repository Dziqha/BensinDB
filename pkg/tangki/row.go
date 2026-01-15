package tangki

import (
	"fmt"
	"strconv"
)

type Row []interface{}


func (r Row) Get(column string) (interface{}, error) {
	for i, col := range r {
		if col == column {
			return r[i], nil
		}
	}
	return nil, fmt.Errorf("kolom '%s' tidak ditemukan", column)
}

func (r Row) Set(column string, value interface{}) {
	for i, col := range r {
		if col == column {
			r[i] = value
			return
		}
	}
}

func (r Row) GetInt(column string) (int, error) {
	val, err := r.Get(column)
	if err != nil {
		return 0, err
	}
	
	switch v := val.(type) {
	case int:
		return v, nil
	case float64:
		return int(v), nil
	case string:
		return strconv.Atoi(v)
	default:
		return 0, fmt.Errorf("tidak bisa convert '%v' ke INT", v)
	}
}

func (r Row) GetFloat(column string) (float64, error) {
	val, err := r.Get(column)
	if err != nil {
		return 0, err
	}
	
	switch v := val.(type) {
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("tidak bisa convert '%v' ke FLOAT", v)
	}
}

func (r Row) GetString(column string) (string, error) {
	val, err := r.Get(column)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%v", val), nil
}

func (r Row) Clone() Row {
	newRow := make(Row, len(r))
	copy(newRow, r)
	return newRow
}

func (r Row) Merge(other Row) Row {
	result := r.Clone()
	copy(result, other)
	return result
}