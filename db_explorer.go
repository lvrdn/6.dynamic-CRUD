package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type RR map[string]interface{}

type Handler struct {
	DB     *sql.DB
	Tables map[string][]Column
}

type Column struct {
	FieldName  string
	Type       string
	Collation  sql.NullString
	Null       string
	Key        string
	Default    sql.NullString
	Extra      string
	Privileges string
	Comment    string
}

func NewDbExplorer(db *sql.DB) (*Handler, error) {
	tablesForHandlerField := map[string][]Column{}

	var tablesFromDB []string

	rows, err := db.Query("show tables")
	if err != nil {
		fmt.Println("error with tables query")
		return nil, err
	}

	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		if err != nil {
			fmt.Println("error with row scan for table")
			return nil, err
		}
		tablesFromDB = append(tablesFromDB, table)
	}
	rows.Close()

	for _, item := range tablesFromDB {
		rowsCol, err := db.Query("show full columns from " + item)
		if err != nil {
			fmt.Println("error with columns query")
			return nil, err
		}
		columns := []Column{}
		for rowsCol.Next() {
			column := &Column{}
			err := rowsCol.Scan(&column.FieldName, &column.Type, &column.Collation, &column.Null, &column.Key, &column.Default, &column.Extra, &column.Privileges, &column.Comment)
			if err != nil {
				fmt.Println("error with row scan for columns")
				return nil, err
			}

			columns = append(columns, *column)
		}

		tablesForHandlerField[item] = columns

		rowsCol.Close()
	}

	return &Handler{DB: db, Tables: tablesForHandlerField}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	tables := []string{}
	for table := range h.Tables {
		tables = append(tables, table)
	}

	sort.Slice(tables, func(i, j int) bool {
		return tables[i] < tables[j]
	})

	if r.URL.Path == "/" {

		response := RR{"response": RR{"tables": tables}}

		dataResponse, err := json.Marshal(response)

		if err != nil {
			fmt.Println("error with marshal response with table list", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Write(dataResponse)
		return
	}

	for _, table := range tables {
		tableFromPath := strings.Split(r.URL.Path, "/")

		if tableFromPath[1] != table {
			continue
		}

		switch r.Method {
		case http.MethodGet:
			h.GetQuery(w, r, table)
			return

		case http.MethodPut:
			h.AddQuery(w, r, table)
			return

		case http.MethodPost:
			h.UpdateQuery(w, r, table)
			return

		case http.MethodDelete:
			h.DeleteQuery(w, r, table)
			return

		default:
		}
		return

	}

	response := RR{"error": "unknown table"}
	dataResponse, err := json.Marshal(response)

	if err != nil {
		fmt.Println(`error with marshal {"error": "unknown table"}`, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNotFound)
	w.Write(dataResponse)
}

func (h *Handler) GetQuery(w http.ResponseWriter, r *http.Request, table string) {

	query := "select * from " + table

	if strings.Contains(r.URL.RawQuery, "limit") {
		limit := r.URL.Query().Get("limit")
		if limit == "" {
			limit = "5"
		}
		if _, err := strconv.Atoi(limit); err != nil {
			limit = "5"
		}

		query += " limit " + limit

		if strings.Contains(r.URL.RawQuery, "offset") {
			offset := r.URL.Query().Get("offset")
			if offset == "" {
				offset = "0"
			}
			if _, err := strconv.Atoi(offset); err != nil {
				offset = "0"
			}

			query += " offset " + offset
		}

	} else if id := strings.TrimPrefix(r.URL.Path, "/"+table+"/"); id != r.URL.Path {

		if _, err := strconv.Atoi(id); err != nil {
			response := RR{"error": "/" + table + "/value must be number > 0"}
			dataResponse, err := json.Marshal(response)
			if err != nil {
				fmt.Println(`error (get query) with marshal {"error":"/` + table + `/value - value must be number > 0"}`)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusBadRequest)
			w.Write(dataResponse)
			return
		}
		for _, field := range h.Tables[table] {
			if field.Key == "PRI" {
				query += fmt.Sprintf(" where %s = %s", field.FieldName, id)
				break
			}
		}
	}

	rows, err := h.DB.Query(query)

	dataFromDB := make([]interface{}, len(h.Tables[table]))

	for i := 0; i < len(dataFromDB); i++ {
		var pointer interface{}
		dataFromDB[i] = &pointer
	}

	if err != nil {
		fmt.Println("error with get query to db:", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var resultRows []RR
	for rows.Next() {

		err := rows.Scan(dataFromDB...)
		if err != nil {
			fmt.Println("error with row scan GET query", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		resultRow := make(map[string]interface{})
		for i := 0; i < len(dataFromDB); i++ {

			data := *dataFromDB[i].(*interface{})
			if data == nil {
				resultRow[h.Tables[table][i].FieldName] = nil
			} else if num, err := strconv.Atoi(string(data.([]byte))); err == nil {
				resultRow[h.Tables[table][i].FieldName] = num
			} else {
				resultRow[h.Tables[table][i].FieldName] = string(data.([]byte))
			}

		}

		resultRows = append(resultRows, resultRow)

	}
	rows.Close()

	rec := RR{}

	if len(resultRows) == 0 {
		response := RR{"error": "record not found"}

		dataResponse, err := json.Marshal(response)
		if err != nil {
			fmt.Println(`error with marshal {"error" : "record not found"}`, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		w.Write(dataResponse)
		return
	}

	if len(resultRows) == 1 && !strings.Contains(r.URL.RawQuery, "limit") {
		rec["record"] = resultRows[0]
	} else {
		rec["records"] = resultRows
	}

	response := RR{"response": rec}

	dataResponse, err := json.Marshal(response)

	if err != nil {
		fmt.Println("error with marshal response of get query", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(dataResponse)
}

func (h *Handler) AddQuery(w http.ResponseWriter, r *http.Request, table string) {

	paramsFromReq := make(map[string]interface{})

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Println("error with read r.body", r.URL.Path, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	errjson := json.Unmarshal(body, &paramsFromReq)
	if errjson != nil {
		fmt.Println("error with unmarshal json from r.body", r.URL.Path, errjson)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if r.URL.Path != "/"+table+"/" {
		response := RR{"error": "not valid url for put query, try to use /" + table + "/"}
		dataResponse, err := json.Marshal(response)
		if err != nil {
			fmt.Println(`error with marshal {"error" : "not valid url for Put query, try to use /`+table+`/"}`, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		w.Write(dataResponse)
		return
	}

	query := "insert into " + table
	var fields, id string
	values := make([]interface{}, 0)
	for i := 0; i < len(h.Tables[table]); i++ {

		field := h.Tables[table][i]
		if field.Key == "PRI" {
			id = field.FieldName
			continue
		}
		var valueFromReq string

		if _, ok := paramsFromReq[field.FieldName]; !ok {
			if field.Null == "YES" {
				continue
			} else {
				valueFromReq = ""
			}

		} else {
			valueFromReq = paramsFromReq[field.FieldName].(string)
		}

		fields += field.FieldName + ","

		values = append(values, valueFromReq)
	}
	query = fmt.Sprintf("%s (%s) values (", query, strings.TrimSuffix(fields, ","))
	for i := 0; i < len(values); i++ {
		query += "?,"
	}
	query = strings.TrimSuffix(query, ",") + ")"

	result, err := h.DB.Exec(query, values...)
	if err != nil {
		fmt.Println("error with answer for put query to db", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	lastId, err := result.LastInsertId()
	if err != nil {
		fmt.Println("error with getting last id of insert row", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	response := RR{"response": RR{id: lastId}}

	dataResponse, err := json.Marshal(response)
	if err != nil {
		fmt.Println("error with marshal response of put query", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(dataResponse)
}

func (h *Handler) DeleteQuery(w http.ResponseWriter, r *http.Request, table string) {

	var fieldIdName string
	for _, field := range h.Tables[table] {
		if field.Key == "PRI" {
			fieldIdName = field.FieldName
			break
		}
	}
	id := strings.TrimPrefix(r.URL.Path, "/"+table+"/")

	if _, err := strconv.Atoi(id); err != nil {
		response := RR{"error": "/" + table + "/value must be number"}
		dataResponse, err := json.Marshal(response)
		if err != nil {
			fmt.Println(`error (delete query) with marshal {"error":"/` + table + `/value must be number"}`)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		w.Write(dataResponse)
		return
	}

	query := fmt.Sprintf("delete from %s where %s = ", table, fieldIdName)
	result, err := h.DB.Exec(query+"?", id)
	if err != nil {
		fmt.Println("error with delete query", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	affected, err := result.RowsAffected()
	if err != nil {
		fmt.Println("error with delete rows affected", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	response := RR{"response": RR{"deleted": affected}}
	dataResponse, err := json.Marshal(response)
	if err != nil {
		fmt.Println("error with marshal response of delete query", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(dataResponse)
}

func (h *Handler) UpdateQuery(w http.ResponseWriter, r *http.Request, table string) {

	paramsFromReq := make(map[string]interface{})

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Println("error with read r.body", r.URL.Path, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	errjson := json.Unmarshal(body, &paramsFromReq)
	if errjson != nil {
		fmt.Println("error with unmarshal json from r.body", r.URL.Path, errjson)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/"+table+"/")

	if _, err := strconv.Atoi(id); err != nil {
		response := RR{"error": "/" + table + "/value must be number"}
		dataResponse, err := json.Marshal(response)
		if err != nil {
			fmt.Println(`error (update query) with marshal {"error":"/` + table + `/value must be number"}`)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		w.Write(dataResponse)
		return
	}

	var fieldIdName string
	query := fmt.Sprintf("update %s set", table)
	for i := 0; i < len(h.Tables[table]); i++ {
		field := h.Tables[table][i]
		value, ok := paramsFromReq[field.FieldName]

		if field.Key == "PRI" {
			fieldIdName = field.FieldName

			if !ok {
				continue
			} else {
				response := RR{"error": "field " + field.FieldName + " have invalid type"}
				dataResponse, err := json.Marshal(response)
				if err != nil {
					fmt.Println(`error with marshal {"error":"field ` + field.FieldName + ` have invalid type"}`)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.WriteHeader(http.StatusBadRequest)
				w.Write(dataResponse)
				return
			}
		}
		if !ok {
			continue
		}
		if field.Type == "text" || field.Type == "varchar(255)" {

			if value == nil && field.Null == "NO" {
				response := RR{"error": "field " + field.FieldName + " have invalid type"}
				dataResponse, err := json.Marshal(response)
				if err != nil {
					fmt.Println(`error with marshal {"error":"field ` + field.FieldName + ` have invalid type"}`)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.WriteHeader(http.StatusBadRequest)
				w.Write(dataResponse)
				return
			}

			if fmt.Sprintf("%T", value) == "float64" {
				response := RR{"error": "field " + field.FieldName + " have invalid type"}
				dataResponse, err := json.Marshal(response)
				if err != nil {
					fmt.Println(`error with marshal {"error":"field ` + field.FieldName + ` have invalid type"}`)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.WriteHeader(http.StatusBadRequest)
				w.Write(dataResponse)
				return
			}
		}
		if value == nil && field.Null == "YES" {
			query += fmt.Sprintf("`%s` = %v,", field.FieldName, "DEFAULT")
		} else {
			query += fmt.Sprintf("`%s` = \"%s\",", field.FieldName, value)
		}

	}
	query = fmt.Sprintf("%s where %s = ", strings.TrimSuffix(query, ","), fieldIdName)
	result, err := h.DB.Exec(query+"?", id)
	if err != nil {
		fmt.Println("error with update query", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	affected, err := result.RowsAffected()
	if err != nil {
		fmt.Println("error with update rows affected", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	response := RR{"response": RR{"updated": affected}}
	dataResponse, err := json.Marshal(response)
	if err != nil {
		fmt.Println("error with marshal response of update query", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(dataResponse)
}
