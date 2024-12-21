package main

import (
	"database/sql"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	_ "github.com/mattn/go-sqlite3" // The underscore imports the package solely for its initialization side-effects
)

// Data struct for storing in the database with lesson_id, user_id, and geolocation
type Data struct {
	LessonID   int    `json:"lesson_id"`
	UserID     int    `json:"user_id"`
	Geolocation string `json:"geolocation"`
}

func main() {
	// Initialize SQLite database
	db, err := sql.Open("sqlite3", ":memory:") // Use ":memory:" for in-memory DB
	if err != nil {
		log.Fatalf("Failed to connect to SQLite: %v", err)
	}
	defer db.Close()

	// Create a table with the new structure
	createTable := `
	CREATE TABLE data (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		lesson_id INTEGER NOT NULL,
		user_id INTEGER NOT NULL,
		geolocation TEXT NOT NULL
	);`
	_, err = db.Exec(createTable)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// Initialize Gin router
	r := gin.Default()

	// POST endpoint: Add data to the database
	r.POST("/data", func(c *gin.Context) {
		var input Data
		// Parse the JSON input
		if err := c.ShouldBindJSON(&input); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid input"})
			return
		}

		// Insert data into the database
		_, err := db.Exec("INSERT INTO data (lesson_id, user_id, geolocation) VALUES (?, ?, ?)", input.LessonID, input.UserID, input.Geolocation)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to insert data"})
			return
		}

		c.JSON(http.StatusOK, gin.H{"message": "Data added successfully"})
	})

	// GET endpoint: Retrieve all data from the database
	r.GET("/data", func(c *gin.Context) {
		rows, err := db.Query("SELECT id, lesson_id, user_id, geolocation FROM data")
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve data"})
			return
		}
		defer rows.Close()

		var results []Data
		for rows.Next() {
			var d Data
			if err := rows.Scan(&d.LessonID, &d.UserID, &d.Geolocation); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse data"})
				return
			}
			results = append(results, d)
		}

		c.JSON(http.StatusOK, results)
	})

	// Start the server
	r.Run(":8085")
}
