package keryxlib

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/MediaMath/keryxlib/message"
)

//Config contains necessary information to start a keryx stream
type Config struct {
	DataDir          string              `json:"data_dir"`
	PGConnStrings    []string            `json:"pg_conn_strings"`
	BufferMax        int                 `json:"buffer_max"`
	ExcludeRelations map[string][]string `json:"exclude,omitempty"`
	IncludeRelations map[string][]string `json:"include,omitempty"`
	BufferDirectory  string              `json:"buffer_directory"`
	MaxMessagePerTxn uint                `json:"max_message_per_txn"`
}

//IncludedTables returns message.Tables from the config
func (config *Config) IncludedTables() []message.Table {
	var tables []message.Table
	if len(config.IncludeRelations) > 0 {
		for tableName := range config.IncludeRelations {
			table := message.TableFromFullName(tableName)
			if table != nil {
				tables = append(tables, *table)
			}

		}
	}
	return tables
}

//ExcludedTables returns message.Tables from the config
func (config *Config) ExcludedTables() []message.Table {
	var tables []message.Table
	for tableName, columns := range config.ExcludeRelations {
		table := message.TableFromFullName(tableName)
		if table != nil && (len(columns) != 0 || columns[0] != "*") {
			tables = append(tables, *table)
		}
	}
	return tables
}

//BufferDirectoryDefaultBase is the root directory to attempt to create buffers files in if
//nothing else is specified
const BufferDirectoryDefaultBase = "/var/tmp/keryx"

//GetBufferDirectoryOrTemp gets the buffer directory out of the config file. If it isnt defined it creates it in BufferDirectoryDefaultBase
func (config *Config) GetBufferDirectoryOrTemp() (bufferWorkingDirectory string, err error) {
	bufferWorkingDirectory = config.BufferDirectory
	if bufferWorkingDirectory == "" {
		err = os.MkdirAll(BufferDirectoryDefaultBase, 0700)
		if err == nil {
			bufferWorkingDirectory, err = ioutil.TempDir(BufferDirectoryDefaultBase, "buffer")
		}
	}
	return
}

//ConfigFromFile loads a config object from a json file
func ConfigFromFile(path string) (*Config, error) {
	file, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	config := new(Config)
	err = json.Unmarshal(file, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}
