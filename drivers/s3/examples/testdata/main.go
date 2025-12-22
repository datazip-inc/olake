package main

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/parquet-go/parquet-go"
)

// Record types for different entities
type User struct {
	UserID    int64     `json:"user_id" parquet:"user_id"`
	Username  string    `json:"username" parquet:"username"`
	Email     string    `json:"email" parquet:"email"`
	FirstName string    `json:"first_name" parquet:"first_name"`
	LastName  string    `json:"last_name" parquet:"last_name"`
	Phone     string    `json:"phone" parquet:"phone"`
	Address   string    `json:"address" parquet:"address"`
	CreatedAt time.Time `json:"created_at" parquet:"created_at"`
	UpdatedAt time.Time `json:"updated_at" parquet:"updated_at"`
	Status    string    `json:"status" parquet:"status"`
	Age       int       `json:"age" parquet:"age"`
	Balance   float64   `json:"balance" parquet:"balance"`
}

type Order struct {
	OrderID       int64     `json:"order_id" parquet:"order_id"`
	UserID        int64     `json:"user_id" parquet:"user_id"`
	ProductID     int64     `json:"product_id" parquet:"product_id"`
	Amount        float64   `json:"amount" parquet:"amount"`
	Quantity      int       `json:"quantity" parquet:"quantity"`
	Tax           float64   `json:"tax" parquet:"tax"`
	ShippingCost  float64   `json:"shipping_cost" parquet:"shipping_cost"`
	Discount      float64   `json:"discount" parquet:"discount"`
	OrderDate     time.Time `json:"order_date" parquet:"order_date"`
	UpdatedAt     time.Time `json:"updated_at" parquet:"updated_at"`
	Status        string    `json:"status" parquet:"status"`
	PaymentMethod string    `json:"payment_method" parquet:"payment_method"`
}

type Product struct {
	ProductID   int64     `json:"product_id" parquet:"product_id"`
	ProductName string    `json:"product_name" parquet:"product_name"`
	SKU         string    `json:"sku" parquet:"sku"`
	Category    string    `json:"category" parquet:"category"`
	Subcategory string    `json:"subcategory" parquet:"subcategory"`
	Price       float64   `json:"price" parquet:"price"`
	Cost        float64   `json:"cost" parquet:"cost"`
	Stock       int       `json:"stock" parquet:"stock"`
	Description string    `json:"description" parquet:"description"`
	CreatedAt   time.Time `json:"created_at" parquet:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" parquet:"updated_at"`
	IsActive    bool      `json:"is_active" parquet:"is_active"`
}

var (
	statuses        = []string{"active", "inactive", "pending", "suspended"}
	orderStatuses   = []string{"pending", "completed", "cancelled", "shipped", "delivered"}
	paymentMethods  = []string{"credit_card", "paypal", "bank_transfer", "cash"}
	categories      = []string{"Electronics", "Clothing", "Books", "Home & Garden", "Toys", "Sports"}
	subcategories   = []string{"Subcat 1", "Subcat 2", "Subcat 3", "Subcat 4", "Subcat 5"}
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Generate functions
func generateUsers(baseDate time.Time, count int) []User {
	users := make([]User, count)
	for i := 0; i < count; i++ {
		users[i] = User{
			UserID:    int64(i + 1),
			Username:  fmt.Sprintf("user_%d", i+1),
			Email:     fmt.Sprintf("user%d@example.com", i+1),
			FirstName: fmt.Sprintf("First%d", i+1),
			LastName:  fmt.Sprintf("Last%d", i+1),
			Phone:     fmt.Sprintf("+1-%03d-%03d-%04d", rand.Intn(900)+100, rand.Intn(900)+100, rand.Intn(9000)+1000),
			Address:   fmt.Sprintf("%d Main St, City %d", rand.Intn(9999)+1, i%100),
			CreatedAt: baseDate.Add(time.Duration(i%8760) * time.Hour),
			UpdatedAt: baseDate.Add(time.Duration(i%8760)*time.Hour + 30*time.Minute),
			Status:    statuses[rand.Intn(len(statuses))],
			Age:       rand.Intn(63) + 18,
			Balance:   float64(rand.Intn(100000)) / 10.0,
		}
	}
	return users
}

func generateOrders(baseDate time.Time, count int) []Order {
	orders := make([]Order, count)
	for i := 0; i < count; i++ {
		orders[i] = Order{
			OrderID:       int64(i + 1),
			UserID:        int64(rand.Intn(count/10) + 1),
			ProductID:     int64(rand.Intn(1000) + 1),
			Amount:        float64(rand.Intn(99000)+1000) / 100.0,
			Quantity:      rand.Intn(10) + 1,
			Tax:           float64(rand.Intn(5000)+50) / 100.0,
			ShippingCost:  float64(rand.Intn(2000)+500) / 100.0,
			Discount:      float64(rand.Intn(10000)) / 100.0,
			OrderDate:     baseDate.Add(time.Duration(i%8760) * time.Hour),
			UpdatedAt:     baseDate.Add(time.Duration(i%8760)*time.Hour + 15*time.Minute),
			Status:        orderStatuses[rand.Intn(len(orderStatuses))],
			PaymentMethod: paymentMethods[rand.Intn(len(paymentMethods))],
		}
	}
	return orders
}

func generateProducts(baseDate time.Time, count int) []Product {
	products := make([]Product, count)
	for i := 0; i < count; i++ {
		products[i] = Product{
			ProductID:   int64(i + 1),
			ProductName: fmt.Sprintf("Product %d", i+1),
			SKU:         fmt.Sprintf("SKU-%08d", i+1),
			Category:    categories[rand.Intn(len(categories))],
			Subcategory: subcategories[rand.Intn(len(subcategories))],
			Price:       float64(rand.Intn(49500)+500) / 100.0,
			Cost:        float64(rand.Intn(24800)+200) / 100.0,
			Stock:       rand.Intn(500),
			Description: fmt.Sprintf("This is product %d with various features and benefits", i+1),
			CreatedAt:   baseDate,
			UpdatedAt:   baseDate.Add(time.Duration(i%8760) * time.Hour),
			IsActive:    rand.Intn(2) == 1,
		}
	}
	return products
}

// Save functions
func saveParquet(filename string, data interface{}) error {
	os.MkdirAll(filepath.Dir(filename), 0755)
	
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Write data based on type
	switch v := data.(type) {
	case []User:
		writer := parquet.NewGenericWriter[User](file, parquet.Compression(&parquet.Snappy))
		defer writer.Close()
		for _, row := range v {
			if _, err := writer.Write([]User{row}); err != nil {
				return fmt.Errorf("failed to write row: %w", err)
			}
		}
	case []Order:
		writer := parquet.NewGenericWriter[Order](file, parquet.Compression(&parquet.Snappy))
		defer writer.Close()
		for _, row := range v {
			if _, err := writer.Write([]Order{row}); err != nil {
				return fmt.Errorf("failed to write row: %w", err)
			}
		}
	case []Product:
		writer := parquet.NewGenericWriter[Product](file, parquet.Compression(&parquet.Snappy))
		defer writer.Close()
		for _, row := range v {
			if _, err := writer.Write([]Product{row}); err != nil {
				return fmt.Errorf("failed to write row: %w", err)
			}
		}
	}

	return nil
}

func saveCSV(filename string, data interface{}, compress bool) error {
	os.MkdirAll(filepath.Dir(filename), 0755)

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	var writer *csv.Writer
	if compress {
		gzWriter := gzip.NewWriter(file)
		defer gzWriter.Close()
		writer = csv.NewWriter(gzWriter)
	} else {
		writer = csv.NewWriter(file)
	}
	defer writer.Flush()

	// Write based on type
	switch v := data.(type) {
	case []User:
		// Header
		writer.Write([]string{"user_id", "username", "email", "first_name", "last_name", "phone", "address", "created_at", "updated_at", "status", "age", "balance"})
		for _, row := range v {
			writer.Write([]string{
				fmt.Sprintf("%d", row.UserID),
				row.Username,
				row.Email,
				row.FirstName,
				row.LastName,
				row.Phone,
				row.Address,
				row.CreatedAt.Format("2006-01-02 15:04:05"),
				row.UpdatedAt.Format("2006-01-02 15:04:05"),
				row.Status,
				fmt.Sprintf("%d", row.Age),
				fmt.Sprintf("%.2f", row.Balance),
			})
		}
	case []Order:
		writer.Write([]string{"order_id", "user_id", "product_id", "amount", "quantity", "tax", "shipping_cost", "discount", "order_date", "updated_at", "status", "payment_method"})
		for _, row := range v {
			writer.Write([]string{
				fmt.Sprintf("%d", row.OrderID),
				fmt.Sprintf("%d", row.UserID),
				fmt.Sprintf("%d", row.ProductID),
				fmt.Sprintf("%.2f", row.Amount),
				fmt.Sprintf("%d", row.Quantity),
				fmt.Sprintf("%.2f", row.Tax),
				fmt.Sprintf("%.2f", row.ShippingCost),
				fmt.Sprintf("%.2f", row.Discount),
				row.OrderDate.Format("2006-01-02 15:04:05"),
				row.UpdatedAt.Format("2006-01-02 15:04:05"),
				row.Status,
				row.PaymentMethod,
			})
		}
	case []Product:
		writer.Write([]string{"product_id", "product_name", "sku", "category", "subcategory", "price", "cost", "stock", "description", "created_at", "updated_at", "is_active"})
		for _, row := range v {
			writer.Write([]string{
				fmt.Sprintf("%d", row.ProductID),
				row.ProductName,
				row.SKU,
				row.Category,
				row.Subcategory,
				fmt.Sprintf("%.2f", row.Price),
				fmt.Sprintf("%.2f", row.Cost),
				fmt.Sprintf("%d", row.Stock),
				row.Description,
				row.CreatedAt.Format("2006-01-02 15:04:05"),
				row.UpdatedAt.Format("2006-01-02 15:04:05"),
				fmt.Sprintf("%t", row.IsActive),
			})
		}
	}

	return nil
}

func saveJSON(filename string, data interface{}, compress bool, lineDelimited bool) error {
	os.MkdirAll(filepath.Dir(filename), 0755)

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	if compress {
		gzWriter := gzip.NewWriter(file)
		defer gzWriter.Close()

		if lineDelimited {
			return writeJSONL(gzWriter, data)
		}
		encoder := json.NewEncoder(gzWriter)
		encoder.SetIndent("", "  ")
		return encoder.Encode(data)
	}

	if lineDelimited {
		return writeJSONL(file, data)
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(data)
}

func writeJSONL(writer interface{ Write([]byte) (int, error) }, data interface{}) error {
	encoder := json.NewEncoder(writer)
	switch v := data.(type) {
	case []User:
		for _, row := range v {
			if err := encoder.Encode(row); err != nil {
				return err
			}
		}
	case []Order:
		for _, row := range v {
			if err := encoder.Encode(row); err != nil {
				return err
			}
		}
	case []Product:
		for _, row := range v {
			if err := encoder.Encode(row); err != nil {
				return err
			}
		}
	}
	return nil
}

func getFileSize(filename string) int64 {
	info, err := os.Stat(filename)
	if err != nil {
		return 0
	}
	return info.Size()
}

// generateChunkTestFiles creates files specifically for testing chunk boundaries
func generateChunkTestFiles(baseDate time.Time, format string, outputDir string, lite bool) {
	// Target sizes for chunk testing:
	// LITE MODE (for local testing):
	// - Small: 1MB, 5MB, 10MB (should group together)
	// - Medium: 15MB, 20MB, 25MB (test grouping edge cases)
	// - Large: 30MB, 35MB (test larger files)
	//
	// FULL MODE (for production testing):
	// - Small: 10MB, 50MB, 100MB (should group together in 2GB chunks)
	// - Medium: 500MB, 1GB, 1.5GB (edge cases - some grouping)
	// - Large: 2.5GB, 3GB (individual chunks, >2GB)
	
	var sizes []struct {
		name      string
		sizeMB    int
		recordMod int // Approximate records for this size
	}
	
	if lite {
		sizes = []struct {
			name      string
			sizeMB    int
			recordMod int
		}{
			{"small_1mb", 1, 16700},
			{"small_5mb", 5, 83300},
			{"small_10mb", 10, 167000},
			{"medium_15mb", 15, 250000},
			{"medium_20mb", 20, 333000},
			{"medium_25mb", 25, 416000},
			{"large_30mb", 30, 500000},
			{"large_35mb", 35, 583000},
		}
	} else {
		sizes = []struct {
			name      string
			sizeMB    int
			recordMod int
		}{
			{"small_10mb", 10, 167000},
			{"small_50mb", 50, 833000},
			{"small_100mb", 100, 1666000},
			{"medium_500mb", 500, 8333000},
			{"medium_1gb", 1000, 16666000},
			{"medium_1500mb", 1500, 25000000},
			{"large_2500mb", 2500, 41666000},
			{"large_3gb", 3000, 50000000},
		}
	}

	if format == "all" || format == "parquet" {
		fmt.Println("📦 Generating Parquet files for chunk testing...")
		for _, s := range sizes {
			users := generateUsers(baseDate, s.recordMod)
			filename := filepath.Join(outputDir, "chunk_test", "parquet", s.name+".parquet")
			if err := saveParquet(filename, users); err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				actualSize := float64(getFileSize(filename)) / (1024 * 1024)
				fmt.Printf("✓ %s (target: %dMB, actual: %.2fMB, %d records)\n", 
					s.name, s.sizeMB, actualSize, len(users))
			}
		}
	}

	if format == "all" || format == "csv" {
		fmt.Println("\n📄 Generating CSV files for chunk testing...")
		for _, s := range sizes {
			orders := generateOrders(baseDate, s.recordMod)
			filename := filepath.Join(outputDir, "chunk_test", "csv", s.name+".csv.gz")
			if err := saveCSV(filename, orders, true); err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				actualSize := float64(getFileSize(filename)) / (1024 * 1024)
				fmt.Printf("✓ %s (target: %dMB, actual: %.2fMB, %d records, gzipped)\n", 
					s.name, s.sizeMB, actualSize, len(orders))
			}
		}
	}

	if format == "all" || format == "json" {
		fmt.Println("\n🔤 Generating JSON files for chunk testing...")
		for _, s := range sizes {
			products := generateProducts(baseDate, s.recordMod)
			filename := filepath.Join(outputDir, "chunk_test", "json", s.name+".jsonl.gz")
			if err := saveJSON(filename, products, true, true); err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				actualSize := float64(getFileSize(filename)) / (1024 * 1024)
				fmt.Printf("✓ %s (target: %dMB, actual: %.2fMB, %d records, JSONL, gzipped)\n", 
					s.name, s.sizeMB, actualSize, len(products))
			}
		}
	}

	fmt.Println("\n✅ Chunk test files generated!")
	fmt.Println("\nExpected chunking behavior:")
	if lite {
		fmt.Println("  • LITE MODE: Files range from 1MB to 35MB")
		fmt.Println("  • All files should group together (total < 2GB)")
		fmt.Println("  • Tests basic chunking logic without heavy resource usage")
	} else {
		fmt.Println("  • FULL MODE: Files range from 10MB to 3GB")
		fmt.Println("  • small_10mb + small_50mb + small_100mb = ~160MB → Single 2GB chunk")
		fmt.Println("  • medium_500mb + medium_1gb + medium_1500mb = ~3GB → 2 chunks (under 2GB boundary)")
		fmt.Println("  • large_2500mb = 1 chunk (>2GB)")
		fmt.Println("  • large_3gb = 1 chunk (>2GB)")
	}
	fmt.Println("\nTo test: Upload to MinIO and run sync with stream grouping")
}

func main() {
	size := flag.String("size", "small", "Data size: small, medium, large, or chunk")
	format := flag.String("format", "all", "Format: parquet, csv, json, or all")
	outputDir := flag.String("output", "test_data", "Output directory")
	targetSizeMB := flag.Int("target-mb", 0, "Target file size in MB (overrides size)")
	chunkTest := flag.Bool("chunk-test", false, "Generate files specifically for chunk boundary testing")
	liteMode := flag.Bool("lite", false, "Use lightweight mode for local testing (smaller files)")
	flag.Parse()

	fmt.Println("=== S3 Driver Test Data Generator (Go) ===")
	fmt.Printf("Size: %s, Format: %s, Output: %s\n", *size, *format, *outputDir)
	if *chunkTest {
		if *liteMode {
			fmt.Println("Mode: Chunk Boundary Testing (LITE MODE - lightweight for local testing)")
		} else {
			fmt.Println("Mode: Chunk Boundary Testing (FULL MODE - may require significant resources)")
		}
	}
	fmt.Println()

	baseDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Determine counts based on size
	var userCount, orderCount, productCount int
	
	if *targetSizeMB > 0 {
		// Calculate approximate record counts for target file size
		// User: ~60 bytes compressed, Order: ~40 bytes, Product: ~50 bytes
		targetBytes := int64(*targetSizeMB) * 1024 * 1024
		userCount = int(targetBytes / 60)
		orderCount = int(targetBytes / 40)
		productCount = int(targetBytes / 50)
		fmt.Printf("Target file size: %d MB\n", *targetSizeMB)
		fmt.Printf("Estimated records: %d users, %d orders, %d products\n\n", userCount, orderCount, productCount)
	} else if *chunkTest {
		// Generate specific files for chunk testing
		if *liteMode {
			fmt.Println("Generating LIGHTWEIGHT files for chunk boundary testing:")
			fmt.Println("  • Small files (1MB, 5MB, 10MB) - quick generation, tests grouping logic")
			fmt.Println("  • Medium files (15MB, 20MB, 25MB) - test grouping edge cases")
			fmt.Println("  • Large files (30MB, 35MB) - test larger file handling")
		} else {
			fmt.Println("Generating FULL-SIZE files for chunk boundary testing:")
			fmt.Println("  • Small files (10MB, 50MB, 100MB) - should group into 2GB chunks")
			fmt.Println("  • Medium files (500MB, 1GB, 1.5GB) - test grouping edge cases")
			fmt.Println("  • Large files (2.5GB, 3GB) - should be individual chunks")
			fmt.Println()
			fmt.Println("⚠️  WARNING: This will generate multi-GB files and may take a while!")
		}
		fmt.Println()
		generateChunkTestFiles(baseDate, *format, *outputDir, *liteMode)
		return
	} else {
		switch *size {
		case "small":
			userCount, orderCount, productCount = 10000, 50000, 5000
		case "medium":
			userCount, orderCount, productCount = 1000000, 1500000, 100000
		case "large":
			userCount, orderCount, productCount = 5000000, 10000000, 500000
		default:
			fmt.Printf("Invalid size: %s (use: small, medium, large, or chunk)\n", *size)
			os.Exit(1)
		}
		fmt.Printf("Generating %d users, %d orders, %d products\n\n", userCount, orderCount, productCount)
	}

	// Generate Parquet
	if *format == "all" || *format == "parquet" {
		fmt.Println("📦 Generating Parquet files...")
		
		users := generateUsers(baseDate, userCount)
		filename := filepath.Join(*outputDir, "parquet", "users", "data.parquet")
		if err := saveParquet(filename, users); err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Printf("✓ %s (%d records, %.2f MB)\n", filename, len(users), float64(getFileSize(filename))/(1024*1024))
		}

		orders := generateOrders(baseDate, orderCount)
		filename = filepath.Join(*outputDir, "parquet", "orders", "data.parquet")
		if err := saveParquet(filename, orders); err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Printf("✓ %s (%d records, %.2f MB)\n", filename, len(orders), float64(getFileSize(filename))/(1024*1024))
		}

		products := generateProducts(baseDate, productCount)
		filename = filepath.Join(*outputDir, "parquet", "products", "data.parquet")
		if err := saveParquet(filename, products); err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Printf("✓ %s (%d records, %.2f MB)\n", filename, len(products), float64(getFileSize(filename))/(1024*1024))
		}
	}

	// Generate CSV
	if *format == "all" || *format == "csv" {
		fmt.Println("\n📄 Generating CSV files...")
		
		users := generateUsers(baseDate, userCount)
		filename := filepath.Join(*outputDir, "csv", "users", "data.csv.gz")
		if err := saveCSV(filename, users, true); err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Printf("✓ %s (%d records, %.2f MB, gzipped)\n", filename, len(users), float64(getFileSize(filename))/(1024*1024))
		}

		orders := generateOrders(baseDate, orderCount)
		filename = filepath.Join(*outputDir, "csv", "orders", "data.csv.gz")
		if err := saveCSV(filename, orders, true); err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Printf("✓ %s (%d records, %.2f MB, gzipped)\n", filename, len(orders), float64(getFileSize(filename))/(1024*1024))
		}

		products := generateProducts(baseDate, productCount)
		filename = filepath.Join(*outputDir, "csv", "products", "data.csv")
		if err := saveCSV(filename, products, false); err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Printf("✓ %s (%d records, %.2f MB)\n", filename, len(products), float64(getFileSize(filename))/(1024*1024))
		}
	}

	// Generate JSON
	if *format == "all" || *format == "json" {
		fmt.Println("\n🔤 Generating JSON files...")
		
		users := generateUsers(baseDate, userCount)
		filename := filepath.Join(*outputDir, "json", "users", "data.jsonl.gz")
		if err := saveJSON(filename, users, true, true); err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Printf("✓ %s (%d records, %.2f MB, JSONL, gzipped)\n", filename, len(users), float64(getFileSize(filename))/(1024*1024))
		}

		orders := generateOrders(baseDate, orderCount)
		filename = filepath.Join(*outputDir, "json", "orders", "data.jsonl.gz")
		if err := saveJSON(filename, orders, true, true); err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Printf("✓ %s (%d records, %.2f MB, JSONL, gzipped)\n", filename, len(orders), float64(getFileSize(filename))/(1024*1024))
		}

		products := generateProducts(baseDate, productCount)
		filename = filepath.Join(*outputDir, "json", "products", "data.json")
		if err := saveJSON(filename, products, false, false); err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Printf("✓ %s (%d records, %.2f MB, JSON array)\n", filename, len(products), float64(getFileSize(filename))/(1024*1024))
		}
	}

	fmt.Println("\n✅ Test data generation complete!")
	fmt.Printf("\nNext steps:\n")
	fmt.Printf("  1. Upload to MinIO: cd ../examples && ./upload_to_minio.sh\n")
	fmt.Printf("  2. Run tests: ./run_tests.sh\n")
}

