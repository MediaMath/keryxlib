package pg

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	//we use pq a ton so its easier to blank import this
	_ "github.com/lib/pq"
)

const (
	nameQuery   = "select pg_namespace.nspname, pg_class.relname from pg_class join pg_namespace on pg_namespace.oid = pg_class.relnamespace where pg_relation_filenode(pg_class.oid) = $1"
	fieldsQuery = "select column_name, data_type, coalesce(character_maximum_length,numeric_precision, 0) as size from information_schema.columns where table_schema = $1 and table_name = $2 order by ordinal_position"
	relIDName   = "select coalesce(pg_relation_filenode(rel.oid), rel.relfilenode) relation_id, concat_ws('.', current_database(), ns.nspname, rel.relname) relation_name from pg_class rel join pg_namespace ns on ns.oid = rel.relnamespace"
)

//Schema is the full representation of a Table
type Schema struct {
	Database  string
	Namespace string
	Table     string
	Fields    []*SchemaField
}

func (s *Schema) String() string {
	fieldStr := ""
	sep := ""

	for _, f := range s.Fields {
		fieldStr += sep + f.Column
		sep = ", "
	}

	return fmt.Sprintf("Schema(%v.%v: [%v])", s.Namespace, s.Table, fieldStr)
}

//SchemaField represents a Column
type SchemaField struct {
	Column   string
	DataType string
	Size     uint32
}

func (sf SchemaField) String() string {
	var kind string

	if sf.Size == 0 {
		kind = sf.DataType
	} else {
		kind = fmt.Sprintf("%v(%d)", sf.DataType, sf.Size)
	}

	return kind
}

//DatabaseDetails represents a connection
type DatabaseDetails struct {
	Name string
	Conn *sql.DB
}

//SchemaReader is used to determine the schema from a database via queries.
type SchemaReader struct {
	conns          map[uint32]DatabaseDetails
	schemaCache    map[string]*Schema
	fieldSizeLimit uint32
}

//NewSchemaReader takes a list of connections, the golang db driver name and a field size limit and returns a schema reader.
func NewSchemaReader(creds []string, driverName string, fieldSizeLimit uint32) (*SchemaReader, error) {

	conns, err := resolveDatabaseConnections(creds, driverName)

	if err != nil {
		return nil, err
	}

	schemaCache := make(map[string]*Schema)
	return &SchemaReader{conns, schemaCache, fieldSizeLimit}, nil
}

func resolveDatabaseConnections(creds []string, driverName string) (map[uint32]DatabaseDetails, error) {
	var name string
	var dbOid uint32

	conns := make(map[uint32]DatabaseDetails)

	for _, connStr := range creds {
		db, err := sql.Open(driverName, connStr)
		if err != nil {
			return nil, err
		}

		err = db.QueryRow("select oid, datname from pg_database where datname = current_database() limit 1").Scan(&dbOid, &name)
		if err != nil {
			return nil, err
		}

		db.SetMaxOpenConns(1)
		conns[dbOid] = DatabaseDetails{name, db}
	}

	return conns, nil
}

//LatestReplayLocation finds the last replicated WAL entry
func (sr *SchemaReader) LatestReplayLocation() uint64 {
	for _, dbDetails := range sr.conns {
		var locStr string

		db := dbDetails.Conn

		rs, err := db.Query("select case when pg_is_in_recovery() then replace(pg_last_xlog_replay_location()::text,'/','') else 'FFFFFFFFFFFFFFFF' end")
		if err == nil {
			defer rs.Close()
			for rs.Next() {
				if err := rs.Scan(&locStr); err == nil {
					x := strings.Split(locStr, "/")
					if len(x) == 2 {
						y := fmt.Sprintf("%08v%08v", x[0], x[1])
						if s, err := strconv.ParseUint(y, 16, 64); err == nil {
							return s
						}
					}
				}
				break
			}
		}
	}

	return 0xFFFFFFFFFFFFFFFF
}

//ConvertRelNamesToIds takes a table name in the form db.ns.name and gets the postgres id for that relation.
func (sr *SchemaReader) ConvertRelNamesToIds(names []string) map[uint32]string {
	var relName string
	var relID uint32

	ids := make(map[uint32]string)

	for _, db := range sr.conns {
		rs, err := db.Conn.Query(relIDName)
		if err != nil {
			continue
		}
		defer rs.Close()

		for rs.Next() {
			if err := rs.Scan(&relID, &relName); err == nil {
				for _, name := range names {
					if name == relName {
						ids[relID] = relName
						break
					}
				}
			}
		}
	}

	return ids
}

//HaveConnectionToDb returns true if the db id in question has a connection defined.
func (sr *SchemaReader) HaveConnectionToDb(databaseID uint32) bool {
	_, ok := sr.conns[databaseID]
	return ok
}

func (sr *SchemaReader) getSchema(databaseID uint32, relationID uint32) (*Schema, error) {
	var count = 0

	key := fmt.Sprintf("%v:%v", databaseID, relationID)

	schema, ok := sr.schemaCache[key]
	if ok {
		return schema, nil
	}

	schema = &Schema{"", "", "", make([]*SchemaField, 0)}

	dbDetails, ok := sr.conns[databaseID]
	if !ok {
		return nil, nil
	}

	db := dbDetails.Conn

	rs, err := db.Query(nameQuery, relationID)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup table name: %v", err)
	}
	defer rs.Close()

	for rs.Next() {
		if err := rs.Scan(&schema.Namespace, &schema.Table); err != nil {
			return nil, fmt.Errorf("failed to read table name row: %v", err)
		}
		count++
	}

	if count < 1 {
		return nil, fmt.Errorf("error while reading table name rows: no results for %v:%v", databaseID, relationID)
	}

	count = 0

	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("error while reading table name rows: %v", err)
	}

	rs, err = db.Query(fieldsQuery, schema.Namespace, schema.Table)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup table name: %v", err)
	}
	defer rs.Close()

	for rs.Next() {
		field := new(SchemaField)
		if err := rs.Scan(&field.Column, &field.DataType, &field.Size); err != nil {
			return nil, fmt.Errorf("failed to read table fields row: %v", err)
		}
		count++
		schema.Fields = append(schema.Fields, field)

		if count < 1 {
			return nil, fmt.Errorf("error while reading table fields rows: no results for %v %v", schema.Namespace, schema.Table)
		}

		if err := rs.Err(); err != nil {
			return nil, fmt.Errorf("error while reading table fields rows: %v", err)
		}
	}

	sr.schemaCache[key] = schema

	return schema, nil
}

//GetDatabaseName takes a postgres database id and returns the name of it.
func (sr *SchemaReader) GetDatabaseName(databaseID uint32) string {
	dbDetails, ok := sr.conns[databaseID]
	if !ok {
		return ""
	}

	return dbDetails.Name
}

//GetNamespaceAndTable takes a database id and a relation id and returns the namespace and table names
func (sr *SchemaReader) GetNamespaceAndTable(databaseID uint32, relationID uint32) (string, string) {
	schema, err := sr.getSchema(databaseID, relationID)
	if err != nil || schema == nil {
		return "", ""
	}

	return schema.Namespace, schema.Table
}

//GetFieldValues takes database id, a table id, and a tuple and returns the fields for that table
func (sr *SchemaReader) GetFieldValues(databaseID uint32, relationID uint32, block uint32, offset uint16) (map[SchemaField]string, error) {
	var count = 0

	dbDetails, ok := sr.conns[databaseID]
	if !ok {
		return nil, nil
	}

	db := dbDetails.Conn

	schema, err := sr.getSchema(databaseID, relationID)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve schema: %v", err)
	} else if schema == nil {
		return nil, nil
	} else if len(schema.Fields) == 0 {
		return nil, fmt.Errorf("no access to schema for %v, %v", databaseID, relationID)
	}

	cast := fmt.Sprintf("::char varying(%v)", sr.fieldSizeLimit)

	var names []string
	var values []*string
	var valuesI []interface{}
	for _, field := range schema.Fields {
		names = append(names, fmt.Sprintf("coalesce(%v%v, '')", field.Column, cast))
		s := new(string)
		values = append(values, s)
		valuesI = append(valuesI, interface{}(s))
	}

	query := fmt.Sprintf("select %v from \"%v\".\"%v\" where ctid = '(%d,%d)'::tid", strings.Join(names, ","), schema.Namespace, schema.Table, block, offset)
	rs, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute values query: %q '%v'::(%v,%v)", err, schema.Table, block, offset)
	}
	defer rs.Close()

	for rs.Next() {
		if err := rs.Scan(valuesI...); err != nil {
			return nil, fmt.Errorf("failed to parse values row: %q '%v'::(%v,%v)", err, schema.Table, block, offset)
		}
		count++
	}

	if count < 1 {
		return nil, fmt.Errorf("failed to parse values rows: no results '%v'::(%v,%v)", schema.Table, block, offset)
	}

	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("failed to parse values row: %v", err)
	}

	out := make(map[SchemaField]string)
	for i, field := range schema.Fields {
		out[*field] = *values[i]
	}

	return out, nil
}
