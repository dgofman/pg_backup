package main

import (
	"fmt"
	"os"
	"pg-backup/utils"
	"strings"

	"github.com/integrii/flaggy"
)

var (
	doExport   string
	doImport   bool
	doClean    bool
	doObscure  bool
	debugLevel bool

	// Database profile name
	dbProfile string
	// Config file name
	configFile string

	// Bulk file name
	bulkFile string
)

func init() {
	flaggy.SetName("pg-backup")
	flaggy.SetVersion("1.0.0")
	flaggy.SetDescription(`
	Pulling data directly from the database.
		Config properties:
		{
			"output-file-name": "<output_file_name>",
			"schema": "<schema_name>",
			"tables": [
				"<table_name>"
			]
			
			,"db_profiles": {
				"name": "<database_name>",
				"user": "<username>",
				"password": "<password>",
				"host": "<hostname>",
				"port": <port_number>,
				"sslmode": "disable/allow/prefer/require",
				"timeout": 10 //Optional, default 3 seconds
			}
		}`)

	flaggy.String(&doExport, "e", "export",
		"Export table data to (raw, csv or json) format, that exports the query result directly to the file.")

	flaggy.Bool(&doImport, "i", "import", "Load data into the database table(s) from a previously backed up file.")

	flaggy.Bool(&doClean, "t", "clean-tables", "Erase all data in the table(s).")

	flaggy.String(&dbProfile, "p", "profile", "Profile name as defined in the 'db_profiles' section in the config file")

	flaggy.String(&configFile, "c", "config", "The configuration file, contains the DATABASE schema and tables names.")

	flaggy.String(&bulkFile, "b", "bulkFile", "The database output file, uses the PostgreSQL copy protocol to perform bulk data insertion.")

	flaggy.Bool(&doObscure, "o", "obscure", "obscure fields")

	flaggy.Bool(&debugLevel, "d", "debug", "set the log level to DEBUG")

	flaggy.Parse()
}

func main() {

	confFile := configFile
	if confFile == "" {
		confFile = "sample.json"
	}

	helpClearTables := ""
	if doClean {
		helpClearTables = "--clean-tables"
	}

	profileName := strings.Split(dbProfile, ":")[0]

	if profileName == "" || (!doClean && !doImport && doExport == "") {
		flaggy.ShowHelpAndExit(`
Example:

Clean Table(s)
pg-backup --profile=development --config=sample.json --clean-tables

Export Data
pg-backup --profile=local --config=sample.json --export=raw
pg-backup --profile=local --config=sample.json --export=csv
pg-backup --profile=local --config=sample.json --export=json

Import Data
pg-backup --profile=development --config=sample.json --bulkFile=backup.raw --import
pg-backup --profile=development --config=sample.json --bulkFile=backup.json --import
pg-backup --profile=development --config=sample.json --bulkFile=backup-csv.zip --import --clean-tables
		`)
	} else if doExport != "" && !strings.Contains("raw|csv|json", doExport) {
		printAndExit(fmt.Sprintf(`
Required: --export=(raw, csv or json) format

Example:

pg-backup --profileName=%[1]s --config=%[2]s --export=raw
pg-backup --profileName=%[1]s --config=%[2]s --export=csv
pg-backup --profileName=%[1]s --config=%[2]s --export=json
			`, dbProfile, confFile))
	} else if doExport != "" && configFile == "" {
		printAndExit(fmt.Sprintf(`
Required: --configFile=(The configuration file)

Example:

pg-backup --profileName=%s --config=sample.json --export=%s
				`, dbProfile, doExport))
	} else if doImport {
		if bulkFile != "" && configFile == "" {
			printAndExit(fmt.Sprintf(`
Required: --config=(The configuration file)

Example:

pg-backup --profile=development --config=sample.json --bulkFile=%s --import %s
				`, bulkFile, helpClearTables))
		} else if bulkFile == "" {
			argConfigFile := ""
			if configFile == "" {
				argConfigFile = " --config=sample.json"
			}
			printAndExit(fmt.Sprintf(`
Required: --bulkFile=(The database output file)

Example:

pg-backup --profile=%[1]s%[2]s --bulkFile=backup.raw --import %[3]s
pg-backup --profile=%[1]s%[2]s --bulkFile=backup.json --import %[3]s
pg-backup --profile=%[1]s%[2]s --bulkFile=backup-csv.zip --import %[3]s
			`, dbProfile, argConfigFile, helpClearTables))
		}
	} else if doClean && configFile == "" {
		printAndExit(fmt.Sprintf(`
Required: --config=(The configuration file)

Example:

pg-backup --profileName=%s --config=sample.json --clean-tables
				`, dbProfile))
	}

	if doExport != "" {
		if err := utils.DbExport(dbProfile, configFile, doExport, doObscure); err != nil {
			panic(err)
		}
	} else if doImport {
		if err := utils.DbImport(dbProfile, configFile, bulkFile, doClean); err != nil {
			panic(err)
		}
	} else if doClean {
		if err := utils.DbClean(dbProfile, configFile); err != nil {
			panic(err)
		}
	}
}

func printAndExit(message string) {
	fmt.Println(message)
	os.Exit(3)
}
