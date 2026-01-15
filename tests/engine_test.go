package tests

import (
	"fmt"
	"testing"

	"github.com/Dziqha/BensinDB/pkg/engine"
	"github.com/Dziqha/BensinDB/pkg/parser"
)

func TestCreateTangki(t *testing.T) {
	db, err := engine.OpenTangki("")
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer db.Close()

	err = db.Jalankan("BUAT TANGKI users (id INT, nama TEKS, umur INT)")
	if err != nil {
		t.Fatalf("Failed to create tangki: %v", err)
	}

	tangki, exists := db.GetTangki("users")
	if !exists {
		t.Fatal("Tangki 'users' tidak ditemukan")
	}

	if len(tangki.Columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(tangki.Columns))
	}
}

func TestInsertData(t *testing.T) {
	db, err := engine.OpenTangki("")
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer db.Close()

	db.Jalankan("BUAT TANGKI users (id INT, nama TEKS)")
	err = db.Jalankan("ISI TANGKI users NILAI (1, 'Andi')")

	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tangki, _ := db.GetTangki("users")
	if len(tangki.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(tangki.Rows))
	}
}

func TestSelectData(t *testing.T) {
	db, err := engine.OpenTangki("")
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer db.Close()

	db.Jalankan("BUAT TANGKI users (id INT, nama TEKS, umur INT)")
	db.Jalankan("ISI TANGKI users NILAI (1, 'Andi', 25)")
	db.Jalankan("ISI TANGKI users NILAI (2, 'Budi', 30)")

	results, err := db.Query("PILIH * DARI users")
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	results, err = db.Query("PILIH * DARI users DIMANA umur > 25")
	if err != nil {
		t.Fatalf("Select with WHERE failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	if results[0][1] != "Budi" {
		t.Fatalf("Expected 'Budi', got %v", results[0][1])
	}
}

func TestUpdateData(t *testing.T) {
    db, _ := engine.OpenTangki("")
    defer db.Close()

    db.Jalankan("BUAT TANGKI users (id INT, nama TEKS, score INT)")
    db.Jalankan("ISI TANGKI users NILAI (1, 'Andi', 100)")

    err := db.Jalankan("ATUR TANGKI users SET score = score + 50 DIMANA id = 1")
    if err != nil { t.Fatalf("Update 1 failed: %v", err) }

    results, _ := db.Query("PILIH * DARI users")
    if fmt.Sprintf("%v", results[0][2]) != "150" {
        t.Fatalf("Expected score 150, got %v", results[0][2])
    }

    err = db.Jalankan("ATUR TANGKI users SET score = score + 50 DIMANA id = 1")
    if err != nil { t.Fatalf("Update 2 failed: %v", err) }

    results, _ = db.Query("PILIH * DARI users")
    score := results[0][2].(float64) 
    if score != 200 {
        t.Fatalf("Expected score 200, got %v", score)
    }
}
func TestDeleteData(t *testing.T) {
	db, err := engine.OpenTangki("")
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer db.Close()

	db.Jalankan("BUAT TANGKI users (id INT, nama TEKS)")
	db.Jalankan("ISI TANGKI users NILAI (1, 'Andi')")
	db.Jalankan("ISI TANGKI users NILAI (2, 'Budi')")

	err = db.Jalankan("BAKAR TANGKI users DIMANA id = 1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	results, _ := db.Query("PILIH * DARI users")
	if len(results) != 1 {
		t.Fatalf("Expected 1 row after delete, got %d", len(results))
	}

	if results[0][1] != "Budi" {
		t.Fatalf("Expected 'Budi', got %v", results[0][1])
	}
}

func TestJoin(t *testing.T) {
	db, err := engine.OpenTangki("")
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer db.Close()

	db.Jalankan("BUAT TANGKI users (id INT, nama TEKS)")
	db.Jalankan("BUAT TANGKI orders (id INT, user_id INT, total FLOAT)")

	db.Jalankan("ISI TANGKI users NILAI (1, 'Andi')")
	db.Jalankan("ISI TANGKI users NILAI (2, 'Budi')")

	db.Jalankan("ISI TANGKI orders NILAI (1, 1, 100000)")
	db.Jalankan("ISI TANGKI orders NILAI (2, 1, 200000)")
	db.Jalankan("ISI TANGKI orders NILAI (3, 2, 150000)")

	err = db.Jalankan("GABUNG users DAN orders MENJADI user_orders DIMANA users.id = orders.user_id")
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}

	results, _ := db.Query("PILIH * DARI user_orders")
	if len(results) != 3 {
		t.Fatalf("Expected 3 joined rows, got %d", len(results))
	}
}

func TestOrderBy(t *testing.T) {
	db, err := engine.OpenTangki("")
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer db.Close()

	db.Jalankan("BUAT TANGKI users (id INT, nama TEKS, score INT)")
	db.Jalankan("ISI TANGKI users NILAI (1, 'Andi', 100)")
	db.Jalankan("ISI TANGKI users NILAI (2, 'Budi', 200)")
	db.Jalankan("ISI TANGKI users NILAI (3, 'Citra', 150)")

	results, err := db.Query("URUTKAN TANGKI users BERDASARKAN score MENURUN")
	if err != nil {
		t.Fatalf("Order failed: %v", err)
	}

	if results[0][1] != "Budi" {
		t.Fatalf("Expected 'Budi' first, got %v", results[0][1])
	}
}

func TestGroupBy(t *testing.T) {
	db, err := engine.OpenTangki("")
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer db.Close()

	db.Jalankan("BUAT TANGKI sales (id INT, product TEKS, amount FLOAT)")
	db.Jalankan("ISI TANGKI sales NILAI (1, 'Laptop', 5000000)")
	db.Jalankan("ISI TANGKI sales NILAI (2, 'Laptop', 6000000)")
	db.Jalankan("ISI TANGKI sales NILAI (3, 'Phone', 3000000)")

	results, err := db.Query("GRUPKAN TANGKI sales BERDASARKAN product SUM(amount)")
	if err != nil {
		t.Fatalf("Group by failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 groups, got %d", len(results))
	}

	results, err = db.Query("GRUPKAN TANGKI sales BERDASARKAN product COUNT(id)")
	if err != nil {
		t.Fatalf("Group by count failed: %v", err)
	}

	for _, row := range results {
		if row[0] == "Laptop" {
			count := int(row[1].(float64)) 
			if count != 2 {
				t.Fatalf("Expected count 2 for Laptop, got %d", count)
			}
		}
	}
}

func TestFQLParser(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "Test Buat Tangki (CREATE)",
			input:   "BUAT TANGKI pengguna (id INT, nama TEKS)",
			wantErr: false,
		},
		{
			name:    "Test Isi Data (INSERT)",
			input:   "ISI KE pengguna (1, 'Bensin Murni')",
			wantErr: false,
		},
		{
			name:    "Test Pilih Data (SELECT)",
			input:   "PILIH * DARI pengguna DIMANA id = 1",
			wantErr: false,
		},
		{
			name:    "Test Atur Data (UPDATE)",
			input:   "ATUR TANGKI pengguna SET nama = 'Oktan Tinggi' DIMANA id = 1",
			wantErr: false,
		},
		{
			name:    "Test Bakar Data (DELETE)",
			input:   "BAKAR TANGKI pengguna DIMANA id = 1",
			wantErr: false,
		},
		{
			name:    "Test Campur Data (UNION)",
			input:   "PILIH * DARI tangki_a CAMPUR PILIH * DARI tangki_b",
			wantErr: false,
		},
        {
			name:    "Test Gabung Tangki (JOIN)",
			input:   "PILIH * DARI tangki_a GABUNG tangki_b PADA tangki_a.id = tangki_b.id",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := parser.NewParser(tt.input)
			_, err := p.Parse() 
			if (err != nil) != tt.wantErr {
				t.Errorf("Parser.Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func BenchmarkInsert(b *testing.B) {
	db, _ := engine.OpenTangki("")
	defer db.Close()
	
	db.Jalankan("BUAT TANGKI users (id INT, nama TEKS)")
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Jalankan("ISI TANGKI users NILAI (1, 'User')")
	}
}

func BenchmarkSelect(b *testing.B) {
	db, _ := engine.OpenTangki("")
	defer db.Close()
	
	db.Jalankan("BUAT TANGKI users (id INT, nama TEKS)")
	
	for i := 0; i < 1000; i++ {
		db.Jalankan("ISI TANGKI users NILAI (1, 'User')")
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Query("PILIH * DARI users")
	}
}