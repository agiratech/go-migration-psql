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
	Development Environment
	Production Environment
}

type Environment struct{
	Username string
	Password string
	Database string
	Port string
}

const table_name = "migrations"
const pathConfig = "./database/config.yml"

var config Config
var format,connector,env string

func SetEnv(envi string){
	env = envi
}

func connect_db() *sql.DB{
	// Change to config
	Initialize()
	db,err := sql.Open(connector,getFormat())

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
	if env == "production"{
		return fmt.Sprintf("user=%s password=%s dbname=%s port=%s", config.Production.Username, config.Production.Password, config.Production.Database, config.Production.Port)
	}
	re := fmt.Sprintf("user=%s password=%s dbname=%s port=%s", config.Development.Username, config.Development.Password, config.Development.Database, config.Development.Port)
	return re
}
