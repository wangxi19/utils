//use in pg only
package sqlutil

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	//tips

	// _ "github.com/lib/pq"
)

type DBPool struct {
	mDBMap    map[string]*sql.DB
	mTimerMap map[string]*time.Timer
	mMutex sync.Mutex
	
	mDBInfoMap map[string]map[string]string
}

//sqltype postgres only, now

func (this *DBPool) InitDB(sqltype string, username string, password string, host string, port string, dbname string, timeoutOpts ...int) error {
	this.mMutex.Lock()
	if nil == this.mDBMap {
		this.mDBMap = make(map[string]*sql.DB)
	}

	if nil == this.mTimerMap {
		this.mTimerMap = make(map[string]*time.Timer)
	}
	
	if nil == this.mDBInfoMap {
		this.mDBInfoMap = make(map[string]map[string]string)
	}
	this.mMutex.Unlock()

	var conntimeout int
	var lifetimeout int
	if len(timeoutOpts) > 0 {
		conntimeout = timeoutOpts[0]
	} else {
		conntimeout = 30
	}

	if len(timeoutOpts) > 1 {
		lifetimeout = timeoutOpts[1]
	} else {
		lifetimeout = conntimeout
	}

	db, err := DBOpen(sqltype, username, password, host, port, dbname, strconv.Itoa(conntimeout))
	if nil != err {
		return err
	}
	
	err = db.Ping()
	if nil != err {
		return err
	}

	db.SetConnMaxLifetime(time.Duration(lifetimeout) * time.Second)
	db.SetMaxIdleConns(2)
	//unlimited conn number
	db.SetMaxOpenConns(-1)

	this.mMutex.Lock()

	this.mDBMap[dbname] = db

	this.mTimerMap[dbname] = time.AfterFunc(3 * time.Minute, func () {
		this.Close(dbname)
	})

	var dbInfo map[string]string
	dbInfo["sqltype"] = sqltype
	dbInfo["username"] = username
	dbInfo["password"] = password
	dbInfo["host"] = host
	dbInfo["port"] = port
	dbInfo["dbname"] = dbname
	dbInfo["conntimeout"] = strconv.Itoa(conntimeout)
	dbInfo["lifetimeout"] = strconv.Itoa(lifetimeout)
	dbInfo["status"] = "active"

	this.mDBInfoMap[dbname] = dbInfo

	this.mMutex.Unlock()

	return nil
}

func (this *DBPool) GetDB(dbname string) (*sql.DB, error) {
	this.mMutex.Lock()

	infoMap, ok := this.mDBInfoMap[dbname]
	if !ok {
		this.mMutex.Unlock()
		return nil, errors.New("Uninitialed database, please call InitDB firstly")
	}

	db := this.mDBMap[dbname]
	this.mMutex.Unlock()

	err := db.Ping()
	if nil != err {
		if "closed" != infoMap["status"] {
			err = db.Close()
			if nil != err {
				//to do
			}
		}

		db, err = DBOpen(infoMap["sqltype"], infoMap["username"], infoMap["password"], infoMap["host"], infoMap["port"], infoMap["dbname"], infoMap["conntimeout"])
		if nil != err {
			this.Remove(dbname)
			return nil, err
		}

		lifetimeout, _ := strconv.Atoi(infoMap["lifetimeout"])
		db.SetConnMaxLifetime(time.Duration(lifetimeout) * time.Second)
	}

	this.mMutex.Lock()
	this.mTimerMap[dbname].Reset(3 * time.Minute)
	this.mMutex.Unlock()

	return db, nil
}

func (this *DBPool) Remove(dbname string) (bool, error) {
	this.mMutex.Lock()
	delete(this.mDBInfoMap, dbname)
	db := this.mDBMap[dbname]
	delete(this.mDBMap, dbname)
	timer := this.mTimerMap[dbname]
	delete(this.mTimerMap, dbname)
	this.mMutex.Unlock()

	err := db.Close()
	ok := timer.Stop()

	return ok, err
}

func (this *DBPool) Close(dbname string) (bool, error) {
	this.mMutex.Lock()
	defer this.mMutex.Unlock()
	
	ok := this.mTimerMap[dbname].Stop()
	err := this.mDBMap[dbname].Close()
	return ok, err
}

// Note: returning *sql.DB must be close explicitly
// sqltype: pg only
func DBOpen(sqltype string, username string, password string, host string, port string, dbname string, conntimeout string) (*sql.DB, error) {
	dbInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s connect_timeout=%s sslmode=disable", host, port, username, password, dbname, conntimeout)

	return sql.Open(sqltype, dbInfo)
}

func BatchInsert(query interface{}, tbName string, fields []string, vals [][]interface{}) (sql.Result, error) {
	exeFunc := func(tx *sql.Tx) (sql.Result, error) {
		stmt, err := tx.Prepare(`insert into ` + tbName + `("` + strings.Join(fields, (`", "`)) + `") values` + (func(fields []string, cnt int) string {
			var prepareLst []string
			for i := 0; i < cnt; i++ {
				var vals []string
				for indx, _ := range fields {
					vals = append(vals, "$"+strconv.Itoa(indx+1+i*len(fields)))
				}
				prepareLst = append(prepareLst, "("+strings.Join(vals, ", ")+")")
			}

			return strings.Join(prepareLst, ", ")
		}(fields, len(vals))) + ` returning ` + fields[0])

		if nil != err {
			return nil, err
		}

		expandedVals := make([]interface{}, 0)

		for _, array := range vals {
			for _, v := range array {
				expandedVals = append(expandedVals, v)
			}
			array = nil
		}

		rst, err := stmt.Exec(expandedVals...)
		if nil != err {
			return nil, err
		}

		return rst, err
	}

	var (
		rst sql.Result
		err error
	)
	switch typeQuery := query.(type) {
	case *sql.Tx:
		rst, err = exeFunc(typeQuery)
	case *sql.DB:
		tx, err := typeQuery.Begin()
		if nil != err {
			return nil, err
		}
		rst, err = exeFunc(tx)
		if nil != err {
			tx.Rollback()
		}

		tx.Commit()
	default:
		rst = nil
		err = errors.New("Invalid query type")
	}

	return rst, err
}

func BatchInsertFaker(query interface{}, tbName string, fields []string, vals [][]interface{}) (sql.Result, error) {
	exeFunc := func(tx *sql.Tx) (sql.Result, error) {
		stmt, err := tx.Prepare(`insert into ` + tbName + `("` + strings.Join(fields, (`", "`)) + `") values(` + (func(fields []string) string {
			var vals []string
			for i, _ := range fields {
				vals = append(vals, "$"+strconv.Itoa(i+1))
			}

			return strings.Join(vals, ", ")
		}(fields)) + `) returning ` + fields[0])

		if nil != err {
			return nil, err
		}

		var rst sql.Result
		for _, val := range vals {
			rst, err = stmt.Exec(val...)
			if nil != err {
				return nil, err
			}
		}

		return rst, err
	}

	var (
		rst sql.Result
		err error
	)
	switch typeQuery := query.(type) {
	case *sql.Tx:
		rst, err = exeFunc(typeQuery)
	case *sql.DB:
		tx, err := typeQuery.Begin()
		if nil != err {
			return nil, err
		}
		rst, err = exeFunc(tx)
		if nil != err {
			tx.Rollback()
		}

		tx.Commit()
	default:
		rst = nil
		err = errors.New("Invalid query type")
	}

	return rst, err
}

func ExecSql(query interface{}, sqlStr string) (sql.Result, error) {
	type qi interface {
		Exec(query string, args ...interface{}) (sql.Result, error)
	}

	execSql := func(query qi) (sql.Result, error) {
		rst, err := query.Exec(sqlStr)
		return rst, err
	}

	var (
		rst sql.Result
		err error
	)
	switch typeQuery := query.(type) {
	case *sql.DB:
		rst, err = execSql(typeQuery)
	case *sql.Tx:
		rst, err = execSql(typeQuery)
	default:
		return nil, errors.New("Invalid query type, must be *sql.DB or *sql.Tx")
	}

	return rst, err
}

func SelectArrayMap(query interface{}, tbName string, fields interface{}, where string, order string, limit int, offset int) ([]map[string]sql.RawBytes, error) {
	rst := make([]map[string]sql.RawBytes, 0)
	var queryStr string
	switch typeField := fields.(type) {
	case []string:
		queryStr += `SELECT ` + strings.Join(typeField, ", ")
	case string:
		queryStr += `SELECT ` + typeField
	default:
		return nil, errors.New("Invalid field type, must be string or []string")
	}

	queryStr += ` FROM ` + tbName
	if "" != where {
		queryStr += ` WHERE ` + where
	}

	if "" != order {
		queryStr += ` ORDER BY ` + order
	}

	if -1 != limit {
		queryStr += ` LIMIT ` + strconv.Itoa(limit)
	}

	if -1 != offset {
		queryStr += ` OFFSET ` + strconv.Itoa(offset)
	}

	type qi interface {
		Query(query string, args ...interface{}) (*sql.Rows, error)
	}

	exeSelect := func(query qi) error {
		rows, err := query.Query(queryStr)
		if nil != err {
			return err
		}

		cols, err := rows.Columns()
		if nil != err {
			return err
		}

		for rows.Next() {
			vals := make([]interface{}, len(cols))
			rowMap := make(map[string]sql.RawBytes)
			for k, _ := range cols {
				vals[k] = new(sql.RawBytes)
			}

			err = rows.Scan(vals...)
			if nil != err {
				return err
			}

			for k, v := range cols {
				rowMap[v] = *(vals[k].(*sql.RawBytes))
			}
			rst = append(rst, rowMap)
		}
		return nil
	}

	switch typeQeury := query.(type) {
	case *sql.DB:
		err := exeSelect(typeQeury)
		if nil != err {
			return nil, err
		}
	case *sql.Tx:
		err := exeSelect(typeQeury)
		if nil != err {
			return nil, err
		}
	default:
		return nil, errors.New("Invalid query type, must be *sql.DB or *sql.Tx")
	}

	return rst, nil
}
