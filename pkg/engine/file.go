package engine

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"github.com/Dziqha/BensinDB/pkg/tangki"
)

const (
	TypeInt   = 0
	TypeFloat = 1
	TypeTeks  = 2
)

// saveNoLock adalah versi internal Save yang dipanggil dari Close()
// Asumsi: lock sudah diambil oleh caller
func (e *Engine) saveNoLock() error {
	file, err := os.Create(e.file)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	binary.Write(writer, binary.LittleEndian, uint16(1)) // major
	binary.Write(writer, binary.LittleEndian, uint16(0)) // minor

	tangkiNames := e.listTangkiNoLock()
	binary.Write(writer, binary.LittleEndian, uint16(len(tangkiNames)))

	for _, name := range tangkiNames {
		t, ok := e.getTangkiNoLock(name)
		if !ok {
			continue
		}
		
		writeString(writer, t.Name)

		binary.Write(writer, binary.LittleEndian, uint16(len(t.Columns)))
		for _, col := range t.Columns {
			writeString(writer, col.Name)
			var tbyte byte
			switch col.Type {
			case "INT":
				tbyte = TypeInt
			case "FLOAT":
				tbyte = TypeFloat
			case "TEKS":
				tbyte = TypeTeks
			}
			writer.WriteByte(tbyte)
		}

		binary.Write(writer, binary.LittleEndian, uint32(len(t.Rows)))
		for _, row := range t.Rows {
			for j, col := range t.Columns {
				val := row[j]
				switch col.Type {
				case "INT":
					binary.Write(writer, binary.LittleEndian, toInt64(val))
				case "FLOAT":
					f := val.(float64)
					binary.Write(writer, binary.LittleEndian, math.Float64bits(f))
				case "TEKS":
					writeString(writer, val.(string))
				}
			}
		}
	}
	return nil
}

// Save adalah fungsi publik yang bisa dipanggil dari luar
// Fungsi ini mengambil lock sendiri
func Save(eng *Engine, filepath string) error {
	eng.mu.RLock()
	defer eng.mu.RUnlock()
	
	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	binary.Write(writer, binary.LittleEndian, uint16(1)) // major
	binary.Write(writer, binary.LittleEndian, uint16(0)) // minor

	tangkiNames := eng.listTangkiNoLock()
	binary.Write(writer, binary.LittleEndian, uint16(len(tangkiNames)))

	for _, name := range tangkiNames {
		t, ok := eng.getTangkiNoLock(name)
		if !ok {
			continue
		}
		
		writeString(writer, t.Name)

		binary.Write(writer, binary.LittleEndian, uint16(len(t.Columns)))
		for _, col := range t.Columns {
			writeString(writer, col.Name)
			var tbyte byte
			switch col.Type {
			case "INT":
				tbyte = TypeInt
			case "FLOAT":
				tbyte = TypeFloat
			case "TEKS":
				tbyte = TypeTeks
			}
			writer.WriteByte(tbyte)
		}

		binary.Write(writer, binary.LittleEndian, uint32(len(t.Rows)))
		for _, row := range t.Rows {
			for j, col := range t.Columns {
				val := row[j]
				switch col.Type {
				case "INT":
					binary.Write(writer, binary.LittleEndian, toInt64(val))
				case "FLOAT":
					f := val.(float64)
					binary.Write(writer, binary.LittleEndian, math.Float64bits(f))
				case "TEKS":
					writeString(writer, val.(string))
				}
			}
		}
	}
	return nil
}

func Load(eng *Engine, filepath string) error {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return err
	}

	buf := data
	pos := 0

	readUint16 := func() uint16 {
		if pos+2 > len(buf) { return 0 }
		v := binary.LittleEndian.Uint16(buf[pos : pos+2])
		pos += 2
		return v
	}

	readString := func() string {
		lenUint := readUint16()
		l := int(lenUint)
		if pos+l > len(buf) {
			return "" 
		}
		s := string(buf[pos : pos+l])
		pos += l
		return s
	}
	readUint32 := func() uint32 {
		v := binary.LittleEndian.Uint32(buf[pos : pos+4])
		pos += 4
		return v
	}
	readByte := func() byte {
		b := buf[pos]
		pos++
		return b
	}

	_ = readUint16() // verMajor
	_ = readUint16() // verMinor

	numTangki := int(readUint16())

	for i := 0; i < numTangki; i++ {
		tName := readString()
		numCols := int(readUint16())
		cols := make([]tangki.Column, numCols)

		for j := 0; j < numCols; j++ {
			cname := readString()
			ctype := readByte()

			tstr := ""
			switch ctype {
			case TypeInt:
				tstr = "INT"
			case TypeFloat:
				tstr = "FLOAT"
			case TypeTeks:
				tstr = "TEKS"
			default:
				return fmt.Errorf("unknown column type: %d", ctype)
			}

			cols[j] = tangki.Column{Name: cname, Type: tstr}
		}

		tk := tangki.NewTangki(tName, cols)
		eng.registerTangki(tk)

		numRows := int(readUint32())

		for r := 0; r < numRows; r++ {
			row := make(tangki.Row, len(cols))
			for j, col := range cols {
            switch col.Type {
            case "INT":
                row[j] = int64(binary.LittleEndian.Uint64(buf[pos : pos+8]))
                pos += 8
            case "FLOAT":
                bits := binary.LittleEndian.Uint64(buf[pos : pos+8])
                row[j] = math.Float64frombits(bits)
                pos += 8
            case "TEKS":
                row[j] = readString()
            }
        }
        tk.Rows = append(tk.Rows, row)
		}
	}

	return nil
}

func writeString(w *bufio.Writer, s string) {
	binary.Write(w, binary.LittleEndian, uint16(len(s)))
	w.WriteString(s)
}


type TangkiData struct {
	Name    string
	Columns []tangki.Column
}

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int64:
		return val
	case float64: 
		return int64(val)
	default:
		return 0
	}
}