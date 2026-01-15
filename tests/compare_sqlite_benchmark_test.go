package tests

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/Dziqha/BensinDB/pkg/engine"
)

func BenchmarkBensinDB_Pure(b *testing.B) {
    dbPath := "test_bensin.bensin"
    os.RemoveAll(dbPath)
    eng, _ := engine.OpenTangki(dbPath)
    eng.Jalankan("BUAT TANGKI pegawai (id INT, nama TEKS, gaji FLOAT, divisi TEKS)")
    t, _ := eng.GetTangki("pegawai") 

    b.ResetTimer()
    for n := 0; n < b.N; n++ {
        err := t.AddRow(int64(n), "Nama"+fmt.Sprint(n), 5000.0, "IT")
        if err != nil {
            b.Fatal(err)
        }
    }
}
func BenchmarkSQLite(b *testing.B) {
	for n := 0; n < b.N; n++ {
		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			b.Fatal(err)
		}

		_, err = db.Exec(`CREATE TABLE pegawai (id INTEGER, nama TEXT, gaji REAL, divisi TEXT)`)
		if err != nil {
			b.Fatal(err)
		}

		tx, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}
		
		stmt, err := tx.Prepare("INSERT INTO pegawai(id, nama, gaji, divisi) VALUES (?, ?, ?, ?)")
		if err != nil {
			b.Fatal(err)
		}
		
		for i := 1; i <= 1000; i++ {
			_, err = stmt.Exec(i, "Nama"+fmt.Sprint(i), float64(5000+i), "IT")
			if err != nil {
				b.Fatal(err)
			}
		}
		stmt.Close()
		tx.Commit()

		rows, err := db.Query("SELECT * FROM pegawai")
		if err != nil {
			b.Fatal(err)
		}
		
		for rows.Next() {
			var id int
			var nama, divisi string
			var gaji float64
			err = rows.Scan(&id, &nama, &gaji, &divisi)
			if err != nil {
				b.Fatal(err)
			}
		}
		rows.Close()
		db.Close()
	}
}

func BenchmarkBensinDB_Insert(b *testing.B) {
	eng, _ := engine.OpenTangki("")
	defer eng.Close()
	
	eng.Jalankan("BUAT TANGKI pegawai (id INT, nama TEKS, gaji FLOAT, divisi TEKS)")
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eng.Jalankan(fmt.Sprintf("ISI TANGKI pegawai NILAI (%d, 'Nama%d', %f)", i, i, float64(5000)))
	}
}

func BenchmarkSQLite_Insert(b *testing.B) {
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()
	
	db.Exec("CREATE TABLE pegawai (id INTEGER, nama TEXT, gaji REAL)")
	stmt, _ := db.Prepare("INSERT INTO pegawai(id, nama, gaji) VALUES (?, ?, ?)")
	defer stmt.Close()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt.Exec(i, fmt.Sprintf("Nama%d", i), float64(5000))
	}
}

func BenchmarkBensinDB_Select(b *testing.B) {
	eng, _ := engine.OpenTangki("")
	defer eng.Close()
	
	eng.Jalankan("BUAT TANGKI pegawai (id INT, nama TEKS, gaji FLOAT)")
	
	for i := 1; i <= 1000; i++ {
		eng.Jalankan(fmt.Sprintf("ISI TANGKI pegawai NILAI (%d, 'Nama%d', %f)", i, i, float64(5000)))
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eng.Query("PILIH * DARI pegawai")
	}
}

func BenchmarkSQLite_Select(b *testing.B) {
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()
	
	db.Exec("CREATE TABLE pegawai (id INTEGER, nama TEXT, gaji REAL)")
	
	stmt, _ := db.Prepare("INSERT INTO pegawai(id, nama, gaji) VALUES (?, ?, ?)")
	for i := 1; i <= 1000; i++ {
		stmt.Exec(i, fmt.Sprintf("Nama%d", i), float64(5000))
	}
	stmt.Close()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query("SELECT * FROM pegawai")
		for rows.Next() {
			var id int
			var nama string
			var gaji float64
			rows.Scan(&id, &nama, &gaji)
		}
		rows.Close()
	}
}