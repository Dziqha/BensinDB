package main

import (
	"fmt"
	"log"

	"github.com/Dziqha/BensinDB/pkg/engine"
	"github.com/Dziqha/BensinDB/pkg/tangki"
)

func main() {
	// === OPEN DATABASE ===
	db, err := engine.OpenTangki("pertamax.bensin")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close() // AUTO SAVE

	fmt.Println(`=== DEMO BENSINDB - BAHASA QUERY INDONESIA ===`)

	// ========== BASIC OPERATIONS ==========
	fmt.Println("\n📦 1. CREATE TABLE")
	err = db.Jalankan("BUAT TANGKI pegawai (id INT, nama TEKS, gaji FLOAT, divisi TEKS)")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("\n📦 2. INSERT DATA")
	db.Jalankan("ISI TANGKI pegawai NILAI (1, 'Andi', 5000000, 'IT')")
	db.Jalankan("ISI TANGKI pegawai NILAI (2, 'Budi', 6000000, 'IT')")
	db.Jalankan("ISI TANGKI pegawai NILAI (3, 'Citra', 7000000, 'HR')")
	db.Jalankan("ISI TANGKI pegawai NILAI (4, 'Dedi', 5500000, 'HR')")

	fmt.Println("\n📊 3. SELECT ALL")
	results, _ := db.Query("PILIH * DARI pegawai")
	printResults(results)

	fmt.Println("\n📊 4. SELECT WITH WHERE")
	results, _ = db.Query("PILIH * DARI pegawai DIMANA divisi = IT")
	printResults(results)

	fmt.Println("\n✏️ 5. UPDATE")
	db.Jalankan("ATUR TANGKI pegawai SET gaji = 5500000 DIMANA id = 1")

	fmt.Println("\n✏️ 6. UPDATE WITH EXPRESSION")
	db.Jalankan("ATUR TANGKI pegawai SET gaji = gaji + 500000 DIMANA divisi = IT")

	fmt.Println("\n📊 7. SELECT AFTER UPDATE")
	results, _ = db.Query("PILIH nama, gaji, divisi DARI pegawai")
	printResults(results)

	fmt.Println("\n📊 8. ORDER BY")
	results, _ = db.Query("URUTKAN TANGKI pegawai BERDASARKAN gaji MENURUN")
	printResults(results)

	fmt.Println("\n📊 9. GROUP BY")
	results, _ = db.Query("GRUPKAN TANGKI pegawai BERDASARKAN divisi AVG(gaji)")
	printResults(results)

	fmt.Println("\n🔥 10. DELETE")
	db.Jalankan("BAKAR TANGKI pegawai DIMANA id = 4")

	fmt.Println("\n📊 11. SELECT AFTER DELETE")
	results, _ = db.Query("PILIH * DARI pegawai")
	printResults(results)

	// ========== JOIN ==========
	fmt.Println("\n=== JOIN OPERATIONS ===")

	db.Jalankan("BUAT TANGKI divisi (id INT, nama_divisi TEKS, lokasi TEKS)")
	db.Jalankan("ISI TANGKI divisi NILAI (101, 'IT', 'Jakarta')")
	db.Jalankan("ISI TANGKI divisi NILAI (102, 'HR', 'Bandung')")

	db.Jalankan("BUAT TANGKI pegawai_detail (id INT, nama TEKS, divisi_id INT)")
	db.Jalankan("ISI TANGKI pegawai_detail NILAI (1, 'Andi', 101)")
	db.Jalankan("ISI TANGKI pegawai_detail NILAI (2, 'Budi', 101)")
	db.Jalankan("ISI TANGKI pegawai_detail NILAI (3, 'Citra', 102)")

	db.Jalankan("GABUNG pegawai_detail DAN divisi MENJADI detail_lengkap DIMANA pegawai_detail.divisi_id = divisi.id")

	results, _ = db.Query("PILIH * DARI detail_lengkap")
	printResults(results)

	// ========== UNION ==========
	fmt.Println("\n=== UNION OPERATIONS ===")

	db.Jalankan("BUAT TANGKI pegawai_jakarta (id INT, nama TEKS, kota TEKS)")
	db.Jalankan("BUAT TANGKI pegawai_bandung (id INT, nama TEKS, kota TEKS)")
	db.Jalankan("BUAT TANGKI pegawai_surabaya (id INT, nama TEKS, kota TEKS)")

	db.Jalankan("ISI TANGKI pegawai_jakarta NILAI (1, 'Eko', 'Jakarta')")
	db.Jalankan("ISI TANGKI pegawai_bandung NILAI (2, 'Fitri', 'Bandung')")
	db.Jalankan("ISI TANGKI pegawai_surabaya NILAI (3, 'Irfan', 'Surabaya')")

	db.Jalankan("SATUKAN pegawai_jakarta, pegawai_bandung, pegawai_surabaya MENJADI pegawai_seluruh")

	results, _ = db.Query("PILIH * DARI pegawai_seluruh")
	printResults(results)

	fmt.Println("\n🎉 === DEMO SELESAI (AUTO SAVED) === 🎉")
}

func printResults(results []tangki.Row) {
	if len(results) == 0 {
		fmt.Println("  (tidak ada data)")
		return
	}
	for _, row := range results {
		fmt.Printf("  %v\n", row)
	}
}
