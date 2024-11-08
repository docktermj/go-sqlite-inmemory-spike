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

	sqlite "github.com/mattn/go-sqlite3"
	"github.com/senzing-garage/go-sdk-abstract-factory/szfactorycreator"
)

const (
	DatabaseURL = "sqlite3://na:na@/MYPRIVATE_DB?mode=memory&cache=shared"
	SQLfile     = "/opt/senzing/er/resources/schema/szcore-schema-sqlite-create.sql"
)

func main() {

	var (
		scanLine    = 0
		scanFailure = 0
		ctx         = context.TODO()
		name        = ""
	)

	// Open a connection to the SQLite database.

	parsedURL, err := url.Parse(DatabaseURL)
	panicOnError(err)
	connectionString := parsedURL.Path
	if len(parsedURL.RawQuery) > 0 {
		connectionString = fmt.Sprintf("file:%s?%s", connectionString, parsedURL.Query().Encode())
	}
	fmt.Printf(">>>>> connectionString: %s\n", connectionString)
	databaseConnector := &Sqlite{
		ConnectionString: connectionString,
	}
	database := sql.OpenDB(databaseConnector)

	// Write to SQLite database from file.

	sqlFile := filepath.Clean(SQLfile)
	file, err := os.Open(sqlFile)
	panicOnError(err)
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
	panicOnError(scanner.Err())

	// Verify database schema installed

	sqlRows, err := database.Query("SELECT name FROM sqlite_master WHERE type='table';")
	panicOnError(err)
	defer sqlRows.Close()

	for sqlRows.Next() {
		err := sqlRows.Scan(&name)
		if err != nil {
			fmt.Printf(">>>>> error: %v\n", err)
		}
		fmt.Printf(">>>>> table name: %s\n", name)
	}

	// Add Senzing Configuration.

	senzingSettings := `{"PIPELINE":{"CONFIGPATH":"/etc/opt/senzing","RESOURCEPATH":"/opt/senzing/er/resources","SUPPORTPATH":"/opt/senzing/data"},"SQL":{"CONNECTION": "` + DatabaseURL + `"}}`
	fmt.Printf(">>>>> senzingSettings: %s\n", senzingSettings)
	szAbstractFactory, err := szfactorycreator.CreateCoreAbstractFactory("test", senzingSettings, 0, 0)
	panicOnError(err)

	// szConfig, err := szAbstractFactory.CreateSzConfig(ctx)
	// panicOnError(err)

	szConfigManager, err := szAbstractFactory.CreateSzConfigManager(ctx)
	if err != nil {
		fmt.Printf(">>>>> Error during CreateSzConfigManager: %s\n", err.Error())
	}
	configID, err := szConfigManager.GetDefaultConfigID(ctx)
	if err != nil {
		fmt.Printf(">>>>> Error during GetDefaultConfigID: %s\n", err.Error())
	}

	fmt.Printf(">>>>> configID: %d\n", configID)

	// Verify database is still available.

	sqlRows, err = database.Query("SELECT name FROM sqlite_master WHERE type='table';")
	panicOnError(err)
	defer sqlRows.Close()

	for sqlRows.Next() {
		err := sqlRows.Scan(&name)
		if err != nil {
			fmt.Printf(">>>>> error: %v\n", err)
		}
		fmt.Printf(">>>>> table name: %s\n", name)
	}

}

func panicOnError(err error) {
	if err != nil {
		panic(err)
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
