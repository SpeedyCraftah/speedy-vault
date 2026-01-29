package handlers

import (
	"database/sql"
	"log"
	"speedyvault/src/config"

	_ "github.com/mattn/go-sqlite3"
)

var DB *sql.DB;

func (DatabaseHandler) InitDatabase() {
	var dbPath string;
	if config.DEBUG_MODE {
		dbPath = ":memory:";
	} else {
		dbPath = "database.sqlite";
	}
	
	var err error;
	DB, err = sql.Open("sqlite3", dbPath);
	if err != nil {
		log.Fatal("Could not open database connection", err);
	}

	// Disable opening more than one concurrent connection with an in-memory database as it causes a new memory instance to be created each time.
	if dbPath == ":memory:" {
		DB.SetMaxOpenConns(1);
	}
	
	// Enable foreign key enforcement.
	if _, err = DB.Exec("PRAGMA foreign_keys = ON;"); err != nil {
		log.Fatal("Could not set foreign key pragma", err);
	}
	
	// Enable WAL mode.
	if _, err = DB.Exec("PRAGMA journal_mode = WAL;"); err != nil {
		log.Fatal("Could not set WAL mode pragma", err);
	}

	// Initialize the handler tables.
	Bucket.InitDBTables();
	Object.InitDBTables();
	
	log.Println("Successfully initialized database connection and tables");
}

type DatabaseHandler struct{};
var Database = DatabaseHandler{};