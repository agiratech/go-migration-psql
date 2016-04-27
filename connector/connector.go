package connector

import(
	"database/sql"
	"log"
	"fmt"
	"io/ioutil"
	"gopkg.in/yaml.v2"
)
import _ "github.com/lib/pq"

type Config struct{
  Database Database
}

type Database struct{
  Username string
  Password string
  Database string
  Port string
}

const table_name = "migrations"
const pathConfig = "./database/config.yml"

var config Config
var format

func connect_db() *sql.DB{
  // Change to config
  setValuesConfig()
  db,err := sql.Open("postgres",getFormat())
  if(err != nil){
    log.Fatal(err)
    return nil
  }
  return db
}

func Run(){
	db := connect_db()
	_,err := db.Exec("create table IF NOT EXISTS migrations( ID SERIAL PRIMARY KEY, migration_id varchar(20) NOT NULL, status int DEFAULT 0, migration_name varchar)")
	if err != nil{
		log.Fatal(err)
	}
}
func InsertMigration(timestamp string){
	db := connect_db()
	_,err := db.Exec("INSERT INTO "+table_name+" (migration_id) VALUES('"+timestamp+"')" )
	if err != nil{
		log.Fatal(err)
	}
}
func RemoveMigration(timestamp string){
	db := connect_db()
	_,err := db.Exec("DELETE FROM "+table_name+" WHERE migration_id = '"+timestamp+"'" )
	if err != nil{
		log.Fatal(err)
	}
}

func Query(query string){
	db := connect_db()
	_,err := db.Exec(query)
	if err != nil{
		log.Fatal(err)
	}
}

func GetQuery(query string) *sql.Rows{
	db := connect_db()
	rows,err := db.Query(query)
	if err != nil{
		log.Fatal(err)
	}
	return rows
}

func Initialize(){
	connector = "postgres"
	setValuesConfig()
}

func setValuesConfig(){
  source, err := ioutil.ReadFile(pathConfig)
  if err != nil{
    log.Fatal(err)
  }
  err = yaml.Unmarshal(source, &config)
  if err != nil{
    log.Fatal(err)
  }
}

func getFormat()string{
  return fmt.Sprintf("user=%s password=%s dbname=%s port=%s", config.Database.Username, config.Database.Password, config.Database.Database, config.Database.Port)
}
