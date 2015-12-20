package keryxlib

import (
	"encoding/json"
	"io/ioutil"
	"os"
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

//BufferDirectoryDefaultBase is the root directory to attempt to create buffers files in if
//nothing else is specified
const BufferDirectoryDefaultBase = "/var/tmp/keryx"

//GetBufferDirectoryOrTemp gets the buffer directory out of the config file. If it isnt defined it creates it in BufferDirectoryDefaultBase
func (c *Config) GetBufferDirectoryOrTemp() (bufferWorkingDirectory string, err error) {
	bufferWorkingDirectory = c.BufferDirectory
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
