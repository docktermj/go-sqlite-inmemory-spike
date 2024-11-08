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
	"time"

	sqlite "github.com/mattn/go-sqlite3"
	"github.com/senzing-garage/go-sdk-abstract-factory/szfactorycreator"
)

const (
	// DatabaseURL = "sqlite3://na:na@/MYPRIVATE_DB?mode=memory&cache=shared"
	// DatabaseURL = "sqlite3://na:na@/tmp/sqlite/mike.db?mode=memory&cache=shared"

	DatabaseURL = "sqlite3://na:na@/tmp/sqlite/mike.db"
	SQLfile     = "/opt/senzing/er/resources/schema/szcore-schema-sqlite-create.sql"
)

func main() {

	var (
		scanLine        = 0
		scanFailure     = 0
		ctx             = context.TODO()
		senzingSettings = `{"PIPELINE":{"CONFIGPATH":"/etc/opt/senzing","RESOURCEPATH":"/opt/senzing/er/resources","SUPPORTPATH":"/opt/senzing/data"},"SQL":{"CONNECTION": "` + DatabaseURL + `"}}`
	)

	// Open a connection to the SQLite database.

	parsedURL, err := url.Parse(DatabaseURL)
	onErrorPanic(err)
	// connectionString := parsedURL.Path[1:]
	connectionString := parsedURL.Path
	if len(parsedURL.RawQuery) > 0 {
		queryParameters := parsedURL.Query().Encode()
		// escapedQueryParameters := url.QueryEscape(queryParameters)
		connectionString = fmt.Sprintf("file:%s?%s", connectionString, queryParameters)
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
		}
	}
	onErrorPanic(scanner.Err())

	// Verify database schema installed

	listTable(database)

	// Add Senzing Configuration.

	fmt.Printf(">>>>> senzingSettings: %s\n", senzingSettings)
	szAbstractFactory, err := szfactorycreator.CreateCoreAbstractFactory("test", senzingSettings, 0, 0)
	onErrorPanic(err)

	szConfigManager, err := szAbstractFactory.CreateSzConfigManager(ctx)
	onErrorLog(err, "szAbstractFactory.CreateSzConfigManager")
	oldConfigID, err := szConfigManager.GetDefaultConfigID(ctx)
	onErrorLog(err, "szConfigManager.GetDefaultConfigID")

	fmt.Printf(">>>>> Old Configuration id: %d\n", oldConfigID)

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

	listTable(database)

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

func listTable(database *sql.DB) {
	var (
		name = ""
	)
	sqlRows, err := database.Query("SELECT name FROM sqlite_master WHERE type='table';")
	onErrorPanic(err)
	defer sqlRows.Close()

	for sqlRows.Next() {
		err := sqlRows.Scan(&name)
		if err != nil {
			onErrorLog(err, "sqlRows.Next()")
		}
		fmt.Printf(">>>>> table name: %s\n", name)
	}
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
