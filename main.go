/*
 */
package main

import (
	"bufio"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	sqlite "github.com/mattn/go-sqlite3"
	"github.com/senzing-garage/go-sdk-abstract-factory/szfactorycreator"
)

const (
	// DatabaseURL = "sqlite3://na:na@nowhere/tmp/G2C.db" // Variation. Works
	// DatabaseURL = "sqlite3://na:na@/tmp/sqlite/G2C.db?mode=memory&cache=shared" // Variation. Works
	DatabaseURL = "sqlite3://na:na@/MYPRIVATE_DB?mode=memory&cache=shared" // Variation. Works
	SQLfile     = "testdata/sql/szcore-schema-sqlite-create.sql"
)

func main() {

	var (
		scanLine        = 0
		scanFailure     = 0
		ctx             = context.TODO()
		senzingSettings = `{"PIPELINE":{"CONFIGPATH":"/etc/opt/senzing","RESOURCEPATH":"/opt/senzing/er/resources","SUPPORTPATH":"/opt/senzing/data"},"SQL":{"CONNECTION": "` + DatabaseURL + `"}}`
	)

	// ------------------------------------------------------------------------
	// -- Install Senzing schema via Go
	// ------------------------------------------------------------------------

	// Open a connection to the SQLite database.

	parsedURL, err := url.Parse(DatabaseURL)
	onErrorPanic(err)
	connectionString := parsedURL.Path

	if len(parsedURL.RawQuery) > 0 {
		queryParameters := parsedURL.Query().Encode()
		connectionString = fmt.Sprintf("file:%s?%s", connectionString[1:], queryParameters) // Variation
	}
	fmt.Printf(">>>>> connectionString: %s\n", connectionString)
	databaseConnector := &Sqlite{
		ConnectionString: connectionString,
	}
	database := sql.OpenDB(databaseConnector)

	// Write to SQLite database from file.

	sqlFile := filepath.Clean(SQLfile)
	file, err := os.Open(sqlFile)
	onErrorPanic(err)
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		scanLine++
		sqlText := scanner.Text()
		_, err := database.ExecContext(ctx, sqlText)
		if err != nil {
			scanFailure++
			fmt.Printf(">>>>> Error: %s\n", err.Error())
		}
	}
	onErrorPanic(scanner.Err())

	// Verify database schema installed by listing tables.

	listTables(database, connectionString)

	// Test of closing and opening database. This "erases" schema.

	// err = database.Close()
	// onErrorPanic(err)
	// database = sql.OpenDB(databaseConnector)
	// listTables(database, connectionString)

	// ------------------------------------------------------------------------
	// -- Install Senzing configuration via Senzing binaries
	// ------------------------------------------------------------------------

	// Create Senzing AbstractFactory.

	fmt.Printf(">>>>> senzingSettings: %s\n", senzingSettings)
	szAbstractFactory, err := szfactorycreator.CreateCoreAbstractFactory("test", senzingSettings, 0, 0)
	onErrorPanic(err)

	// Get existing Senzing configuration. Expecting "0" for no configuration.

	szConfigManager, err := szAbstractFactory.CreateSzConfigManager(ctx)
	onErrorLog(err, "szAbstractFactory.CreateSzConfigManager")
	oldConfigID, err := szConfigManager.GetDefaultConfigID(ctx)
	onErrorLog(err, "szConfigManager.GetDefaultConfigID")
	fmt.Printf(">>>>> Old Configuration id: %d\n", oldConfigID)

	// Persist New Senzing configuration.

	szConfig, err := szAbstractFactory.CreateSzConfig(ctx)
	onErrorPanic(err)
	configHandle, err := szConfig.CreateConfig(ctx)
	onErrorLog(err, "szConfig.CreateConfig")
	configStr, err := szConfig.ExportConfig(ctx, configHandle)
	onErrorLog(err, "szConfig.ExportConfig")
	configComments := fmt.Sprintf("Created at %s", time.Now().Format(time.RFC3339Nano))
	newConfigID, err := szConfigManager.AddConfig(ctx, configStr, configComments)
	onErrorLog(err, "szConfigManager.AddConfig")
	err = szConfigManager.SetDefaultConfigID(ctx, newConfigID)
	onErrorLog(err, "szConfigManager.SetDefaultConfigID")
	fmt.Printf(">>>>> New Configuration id: %d\n", newConfigID)

	// Verify database is still available.

	listTables(database, connectionString)

}

func onErrorLog(err error, message string) {
	if err != nil {
		fmt.Printf(">>>>> Error: %s; error: %s\n", message, err.Error())
	}
}

func onErrorPanic(err error) {
	if err != nil {
		panic(err)
	}
}

func listTables(database *sql.DB, databaseName string) {
	var name = ""
	sqlRows, err := database.Query("SELECT name FROM sqlite_master WHERE type='table';")
	onErrorPanic(err)
	onErrorPanic(sqlRows.Err())
	defer sqlRows.Close()
	tables := []string{}
	for sqlRows.Next() {
		err := sqlRows.Scan(&name)
		if err != nil {
			onErrorLog(err, "sqlRows.Next()")
		}
		tables = append(tables, name)
	}
	fmt.Printf(">>>>> tables for %s: %s\n", databaseName, strings.Join(tables, ", "))
}

// ----------------------------------------------------------------------------
// Type Sqlite
// ----------------------------------------------------------------------------

type Sqlite struct {
	ConnectionString string
}

func (connector *Sqlite) Connect(_ context.Context) (driver.Conn, error) {
	return connector.Driver().Open(connector.ConnectionString)
}

func (connector *Sqlite) Driver() driver.Driver {
	return &sqlite.SQLiteDriver{}
}
