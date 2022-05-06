package main

/*
Author: Chen Jian
Blog:   https://www.o-my-chenjian.com
Date:   2018-11-20
*/

import (
	"fmt"
	"log"

	"database/sql"

	_ "github.com/lib/pq"
)

var db *sql.DB
var err error

// PGs数据库信息
const (
	pg_host     = "139.198.109.225"
	pg_port     = 55432
	pg_user     = "1ch0"
	pg_password = "zxc,./,./"
	pg_dbname   = "go-demo-pq"
)

func InsertNodeInfo() error {
	stmt, err := db.Prepare("INSERT INTO \"node_infos\"(\"node_name\", \"node_ip\", \"node_port\", \"node_username\", \"node_password\") VALUES ($1, $2, $3, $4, $5)")
	if err != nil {
		log.Fatal("PG Statements Wrong: ", err)
	}

	res, err := stmt.Exec("nicai", "10.51.42.66", "9999", "nicai", "gophdddeer")
	if err != nil {
		log.Fatal("PG Statements Exec Wrong: ", err)
	}

	id, err := res.RowsAffected()
	if err != nil {
		log.Fatal("PG Affecte Wrong: ", err)
	}

	fmt.Println(id)
	return nil
}

func DeleteNodeInfo() error {
	stmt, err := db.Prepare("DELETE FROM \"node_infos\" WHERE \"node_name\" = $1")
	if err != nil {
		log.Fatal("PG Statements Wrong: ", err)
	}

	res, err := stmt.Exec("nicai")
	if err != nil {
		log.Fatal("PG Statements Exec Wrong: ", err)
	}

	id, err := res.RowsAffected()
	if err != nil {
		log.Fatal("PG Affecte Wrong: ", err)
	}

	fmt.Println(id)
	return nil
}

func SelectAllNodeInfo() error {
	rows, err := db.Query("SELECT * FROM  \"node_infos\"")
	if err != nil {
		log.Fatal("PG Statements Wrong: ", err)
	}

	for rows.Next() {
		var nodename string
		var nodeip string
		var nodeport string
		var nodeusername string
		var nodepassword string

		if err := rows.Scan(&nodename, &nodeip, &nodeport, &nodeusername, &nodepassword); err != nil {
			log.Fatal("PG Rows Scan Failed: ", err)
		}

		fmt.Printf("node_name = \"%s\", "+
			"node_ip = \"%s\", "+
			"node_port = \"%s\", "+
			"node_username = \"%s\", "+
			"node_password = \"%s\"\n", nodename, nodeip, nodeport, nodeusername, nodepassword)
	}

	if err := rows.Err(); err != nil {
		log.Fatal("PG Query Failed: ", err)
	}

	rows.Close()
	db.Close()
	return nil
}

func UpdateNodeInfo() error {
	stmt, err := db.Prepare("UPDATE \"node_infos\" SET \"node_username\" = $1 WHERE \"node_name\" = $2")
	if err != nil {
		log.Fatal("PG Statements Wrong: ", err)
	}

	res, err := stmt.Exec("blabla", "blabla")
	if err != nil {
		log.Fatal("PG Statements Exec Wrong: ", err)
	}

	id, err := res.RowsAffected()
	if err != nil {
		log.Fatal("PG Affecte Wrong: ", err)
	}

	fmt.Println(id)
	return nil
}

func main() {
	// 链接PostgreSQL数据库
	log.Println("Connecting PostgreSQL....")

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", pg_host, pg_port, pg_user, pg_password, pg_dbname)
	db, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal("Connect PG Failed: ", err)
	}

	db.SetMaxOpenConns(2000)
	db.SetMaxIdleConns(1000)
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal("Ping GP Failed: ", err)
	}
	fmt.Println("PG Successfull Connected!")

	// 插入数据
	//err := InsertNodeInfo()
	//if err != nil {
	//	log.Fatal("Insert Data Failed: ", err)
	//}

	// 删除数据
	//err := DeleteNodeInfo()
	//if err != nil {
	//	log.Fatal("Delete Data Failed: ", err)
	//}

	//查询所有数据
	err := SelectAllNodeInfo()
	if err != nil {
		log.Fatal("Select All Data Failed: ", err)
	}

	// 更新某一数据
	//err := UpdateNodeInfo()
	//if err != nil {
	//	log.Fatal("Update Data Failed: ", err)
	//}
}
