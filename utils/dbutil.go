package utils

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/nwtgck/go-fakelish"
)

var (
	csvFileZipExtension = "-csv.zip"
)

type FileInfo struct {
	Format string  `json:"format"`
	Env    string  `json:"environment"`
	Schema string  `json:"schema"`
	Tables []Table `json:"tables"`
}

type Table struct {
	Name    string          `json:"name"`
	Columns []string        `json:"columns"`
	Rows    [][]interface{} `json:"rows"`
}

type Config struct {
	Name    string                       `json:"output-file-name"`
	Schema  string                       `json:"schema"`
	Tables  []string                     `json:"tables"`
	Obscure map[string]map[string]string `json:"obscure"`
	Profile map[string]DbProfile         `json:"db_profiles"`
}

type DbProfile struct {
	DBName     string `json:"name"`
	DBUser     string `json:"user"`
	DBPassword string `json:"password"`
	DBHost     string `json:"host"`
	DBSSLMode  string `json:"sslmode"`
	DBPort     int    `json:"port"`
	DBTimeout  *int   `json:"timeout"`
}

type ConInfo struct {
	con     *pgxpool.Pool
	context context.Context
}

func initGob() {
	// Register custom datatype for encoding/gob
	gob.Register(time.Now()) //Register time.Time
}

// Loading configuration file
func loadConfig(dbProfile string, configFilePath string) (config Config) {
	file, err := os.Open(configFilePath)
	if err != nil {
		panic(err)
	}

	data, err := ioutil.ReadAll(file)
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(data, &config)
	if err != nil {
		panic(err)
	}
	return
}

func getConInfo(dbProfile string, config Config) (conInfo ConInfo) {
	profile := config.Profile[dbProfile]
	timeout := 3
	if profile.DBTimeout != nil {
		timeout = *profile.DBTimeout
	}
	var err error
	var dsConnString = fmt.Sprintf("dbname=%v user=%v password=%v host=%v port=%v connect_timeout=%v sslmode=%v",
		profile.DBName, profile.DBUser, profile.DBPassword, profile.DBHost, profile.DBPort, timeout, profile.DBSSLMode)
	conInfo.con, err = pgxpool.Connect(context.Background(), dsConnString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	conInfo.context = context.Background()
	return
}

// Export database table records into an external file
func DbExport(dbProfile, configFilePath, exportFormat string, doObscure bool) (err error) {
	log.Println("Exporting...")

	var (
		config  = loadConfig(dbProfile, configFilePath)
		output  []byte
		outFile *os.File
	)
	conInfo := getConInfo(dbProfile, config)
	defer conInfo.con.Close()

	initGob()

	fileInfo := FileInfo{
		Format: exportFormat,
		Env:    dbProfile,
		Schema: config.Schema,
	}

	for _, tableName := range config.Tables {
		rows, err := conInfo.con.Query(conInfo.context, fmt.Sprintf("SELECT * FROM %v.%v", config.Schema, tableName))
		if err != nil {
			log.Println(fmt.Sprintf("Table: %v.%v, Error: %v", config.Schema, tableName, err))
			panic(err.Error())
		}
		defer rows.Close()

		log.Println(fmt.Sprintf("Table: %v.%v", config.Schema, tableName))
		table := Table{Name: tableName}
		for _, f := range rows.FieldDescriptions() {
			table.Columns = append(table.Columns, string(f.Name))
		}

		uniqIndexes := map[string]map[string]int{}
		if doObscure && config.Obscure[tableName] != nil {
			rows, err := conInfo.con.Query(conInfo.context, "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname='%v' AND tablename='%v'", config.Schema, tableName)
			if err == nil {
				for rows.Next() {
					row, err := rows.Values()
					query := fmt.Sprint(row[1])
					if err == nil && strings.HasPrefix(query, "CREATE UNIQUE INDEX") {
						rule := strings.Split(query, "(")[1]
						colNames := strings.Split(rule[:len(rule)-1], ", ")
						indexes := map[string]int{}
						for _, name := range colNames {
							indexes[name] = -1
						}
						for index, name := range table.Columns {
							if indexes[name] == -1 {
								indexes[name] = index
							}
						}
						uniqIndexes[fmt.Sprintf("%v", row[0])] = indexes
					}
				}
				rows.Close()
			}
		}

		valueTableMap := map[string]bool{}
		for rows.Next() {
			row, err := rows.Values()
			if err != nil {
				panic(err.Error())
			}

			retry := 0
			maxRetry := 10
		GenerateObscureValue:
			retry++
			valueRowMap := map[string]bool{}
			if doObscure && config.Obscure[tableName] != nil {
				for index, name := range table.Columns {
					pattern := config.Obscure[tableName][name]
					if pattern != "" {
						row[index] = generateNewValue(pattern)
					}
				}
			}

			null := []byte("null")
			for _, indexes := range uniqIndexes {
				val := ""
				for colName, index := range indexes {
					b, _ := json.Marshal(row[index])
					if row[index] == nil || bytes.Equal(b, null) {
						val = ""
						break //skip validation if some of the value is null
					}
					val += fmt.Sprintf("%s=%s_", colName, b)
				}
				if val != "" {
					if valueTableMap[val] && retry < maxRetry {
						log.Println(fmt.Sprintf("%v.%v (%v) duplicate value: %v", config.Schema, tableName, retry, val))
						goto GenerateObscureValue
					}
					valueRowMap[val] = true
				}
			}

			for val := range valueRowMap {
				valueTableMap[val] = true
			}

			if retry < maxRetry {
				table.Rows = append(table.Rows, row)
			} else {
				log.Printf("%v.%v skipped row: %+v", config.Schema, tableName, row)
			}
		}
		fileInfo.Tables = append(fileInfo.Tables, table)
	}

	if exportFormat == "csv" {
		outFile, err = os.Create(config.Name + csvFileZipExtension)
		if err != nil {
			panic(err)
		}
		defer outFile.Close()
		w := zip.NewWriter(outFile)
		f, err := w.Create("info.json")
		if err != nil {
			panic(err)
		}
		_, err = f.Write([]byte(fmt.Sprintf("{\"format\": \"csv\", \"environment\": \"%v\", \"schema\": \"%v\"}", fileInfo.Env, fileInfo.Schema)))
		if err != nil {
			panic(err)
		}
		for _, table := range fileInfo.Tables {
			f, err := w.Create(table.Name + ".csv")
			if err != nil {
				panic(err)
			}
			var buf bytes.Buffer
			buf.WriteString(strings.Join(table.Columns, ",") + "\n")
			for _, row := range table.Rows {
				line, err := json.Marshal(row)
				if err != nil {
					panic(err)
				}
				buf.Write(line[:len(line)-1][1:])
				buf.WriteByte('\n')
			}
			_, err = f.Write(buf.Bytes())
			if err != nil {
				panic(err)
			}
		}
		err = w.Close()
		if err != nil {
			panic(err)
		}
		return err
	} else if exportFormat == "raw" {
		b := new(bytes.Buffer)
		e := gob.NewEncoder(b)
		err := e.Encode(fileInfo)
		if err != nil {
			panic(err)
		}
		output = b.Bytes()
	} else {
		output, err = json.MarshalIndent(fileInfo, "", " ")
		if err != nil {
			panic(err)
		}
	}
	ioutil.WriteFile(config.Name+"."+exportFormat, output, 0644)

	return
}

// Import database records
func DbImport(dbProfile, configFilePath, bulkFilePath string, doClean bool) (err error) {
	log.Println("Importing...")

	var fileInfo FileInfo

	if strings.HasSuffix(bulkFilePath, csvFileZipExtension) {
		zipReader, err := zip.OpenReader(bulkFilePath)
		if err != nil {
			panic(err)
		}
		defer zipReader.Close()
		for _, zipItem := range zipReader.File {
			zipItemReader, err := zipItem.Open()
			if err != nil {
				panic(err)
			}
			defer zipItemReader.Close()

			buf, err := ioutil.ReadAll(zipItemReader)
			if err != nil {
				panic(err)
			}

			name_ext := strings.Split(zipItem.Name, ".")
			if name_ext[1] == "json" {
				json.Unmarshal(buf, &fileInfo)
			} else {
				lines := strings.Split(string(buf), "\n")
				columns := strings.Split(lines[0], ",")
				var rows [][]interface{}
				for i := 1; i < len(lines); i++ {
					var row []interface{}
					_ = json.Unmarshal([]byte("["+lines[i]+"]"), &row)
					if len(row) == len(columns) {
						rows = append(rows, row)
					}
				}
				fileInfo.Tables = append(fileInfo.Tables, Table{
					Name:    name_ext[0],
					Columns: columns,
					Rows:    rows,
				})
			}
		}
	} else {
		file, err := os.Open(bulkFilePath)
		if err != nil {
			panic(err)
		}

		data, err := ioutil.ReadAll(file)
		if err != nil {
			panic(err)
		}

		if strings.HasSuffix(bulkFilePath, ".json") {
			err = json.Unmarshal(data, &fileInfo)
		} else { //raw
			initGob()
			b := bytes.NewBuffer(data)
			d := gob.NewDecoder(b)
			err = d.Decode(&fileInfo)
		}
		if err != nil {
			panic(err)
		}
	}
	return fileInfoToDb(fileInfo, dbProfile, configFilePath, doClean)
}

func fileInfoToDb(fileInfo FileInfo, dbProfile, configFilePath string, doClean bool) (err error) {
	var (
		schema = fileInfo.Schema
	)
	config := loadConfig(dbProfile, configFilePath)
	schema = config.Schema

	conInfo := getConInfo(dbProfile, config)
	defer conInfo.con.Close()

	for _, table := range fileInfo.Tables {
		if doClean {
			conInfo.con.Exec(conInfo.context, fmt.Sprintf("DELETE FROM %v.%v", schema, table.Name))
		}

		if fileInfo.Format == "raw" {
			insertCount, err := conInfo.con.CopyFrom(conInfo.context, []string{schema, table.Name}, table.Columns, pgx.CopyFromRows(table.Rows))
			if err != nil {
				log.Printf("Table: %v.%v, Error: %v", schema, table.Name, err)
			} else {
				log.Printf("Table: %v.%v, inserted rows: %v", schema, table.Name, insertCount)
			}
		} else {
			rows := make([]interface{}, len(table.Rows))
			for r, row := range table.Rows {
				coldata := map[string]interface{}{}
				for c, name := range table.Columns {
					coldata[name] = row[c]
				}
				rows[r] = coldata
			}
			output, err := json.MarshalIndent(rows, "", " ")
			if err != nil {
				log.Printf("Table: %v.%v, Error: %v", schema, table.Name, err)
			} else {
				result, err := conInfo.con.Exec(conInfo.context,
					fmt.Sprintf("INSERT INTO %[1]s.%[2]s SELECT * FROM json_populate_recordset (NULL::%[1]s.%[2]s, $$%[3]s$$)", schema, table.Name, output))
				if err != nil {
					log.Printf("Table: %v.%v, Error: %v", schema, table.Name, err)
				} else {
					insertCount := result.RowsAffected()
					log.Printf("Table: %v.%v, inserted rows: %v", schema, table.Name, insertCount)

					var id string
					row := conInfo.con.QueryRow(conInfo.context,
						fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE column_default like '%s' AND table_schema='%s' AND table_name='%s'", "%nextval%", schema, table.Name))
					err := row.Scan(&id)
					if err != pgx.ErrNoRows {
						if err != nil {
							log.Printf("Cannot get information Table: %v.%v, Error: %v", schema, table.Name, err)
						} else {
							var seqNum *int64
							row := conInfo.con.QueryRow(conInfo.context,
								fmt.Sprintf("SELECT pg_catalog.setval(pg_get_serial_sequence('%[1]s.%[2]s', '%[3]s'), MAX(%[3]s)) FROM %[1]s.%[2]s", schema, table.Name, id))
							err := row.Scan(&seqNum)
							if err != nil && err != pgx.ErrNoRows {
								log.Printf("Cannot reset a serial sequence Table: %v.%v, Error: %v", schema, table.Name, err)
							} else if seqNum != nil {
								log.Printf("New serial sequence %v.%v, is: %v", schema, table.Name, *seqNum)
								/* Verify
								SELECT MAX(%PK_COLUMN_NAME%) FROM %SCHEMA%.%TABLE%;
								SELECT pg_get_serial_sequence('%SCHEMA%.%TABLE%', '%PK_COLUMN_NAME%');
								SELECT last_value FROM %SEQ_ID%;
								*/
							}
						}
					}
				}
			}
		}
	}

	return
}

// Clean database table(s)
func DbClean(dbProfile, configFilePath string) (err error) {
	log.Println("Cleaning...")

	config := loadConfig(dbProfile, configFilePath)
	conInfo := getConInfo(dbProfile, config)
	defer conInfo.con.Close()

	for _, tableName := range config.Tables {
		_, err := conInfo.con.Exec(conInfo.context, fmt.Sprintf("DELETE FROM %v.%v", config.Schema, tableName))
		if err != nil {
			log.Printf("Table: %v.%v, Error: %v", config.Schema, tableName, err)
			panic(err.Error())
		}
		log.Printf("Table: %v.%v", config.Schema, tableName)
	}
	return
}

func generateNewValue(pattern string) *string {
	re := regexp.MustCompile(`\[([^{}]*)\]{([^{}]*)}`)
	matches := re.FindAllStringIndex(pattern, -1)
	partObscure := ""
	lastIndex := 0
	for _, indexes := range matches {
		random := rand.New(rand.NewSource(time.Now().UnixNano()))
		_substr := string(pattern[indexes[0]:indexes[1]])
		for _, match := range re.FindAllStringSubmatch(_substr, -1) {
			_range := strings.Split(match[2], ",")
			endIndex := -1
			if len(_range) == 2 {
				endIndex, _ = strconv.Atoi(_range[1])
			}
			if begIndex, err := strconv.Atoi(_range[0]); err == nil {
				if begIndex == 0 && random.Intn(2) == 0 { //random continue or not
					if partObscure == "" {
						return nil
					}
					return &partObscure
				}
				str := ""
				if strings.ToLower(match[1]) == "a-z" { //strings
					if endIndex == -1 {
						str = fakelish.GenerateFakeWordByLength(begIndex)
					} else {
						str = fakelish.GenerateFakeWord(begIndex, endIndex)
					}
					if match[1] == "a-z" {
						str = strings.ToLower(str)
					} else if match[1] == "A-Z" {
						str = strings.ToUpper(str)
					} else {
						str = strings.Title(str)
					}
				} else if match[1] == "0-9" { //numbers
					nums := random.Perm(begIndex)
					if endIndex != -1 {
						nums = random.Perm(random.Intn(endIndex-begIndex) + begIndex)
					}
					var buf bytes.Buffer
					for i := range nums {
						if nums[i] == 0 {
							nums[i] = random.Intn(8) + 1
						}
						buf.WriteString(fmt.Sprintf("%d", nums[i]))
					}
					time.Sleep(1 * time.Nanosecond) //wait next time seq. (avoid duplicate in generation)
					str = buf.String()
				} else {
					str = match[1]
					if endIndex != -1 {
						for i := 1; i < endIndex; i++ {
							str += match[1]
						}
					}
				}
				partObscure += pattern[lastIndex:indexes[0]] + str
				lastIndex = indexes[1]
			}
		}
	}
	length := len(pattern)
	partObscure += pattern[lastIndex:length]
	partObscure = strings.TrimSpace(partObscure)
	return &partObscure
}
