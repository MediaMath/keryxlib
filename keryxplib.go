package keryxplib

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/MediaMath/keryxlib/filters"
	"github.com/MediaMath/keryxlib/message"
	"github.com/MediaMath/keryxlib/pg"
	"github.com/MediaMath/keryxlib/streams"
)

//TransactionChannel sets up a keryx stream and schema reader with the provided configuration and returns
//it as a channel
func TransactionChannel(serverVersion string, kc *Config) (<-chan *message.Transaction, error) {

	schemaReader, err := pg.NewSchemaReader(kc.PGConnStrings, "postgres", 255)
	if err != nil {
		return nil, err
	}

	bufferWorkingDirectory, err := kc.GetBufferDirectoryOrTemp()
	if err != nil {
		return nil, err
	}

	f := filters.Exclusive(schemaReader, kc.ExcludeRelations)
	if len(kc.IncludeRelations) > 0 {
		f = filters.Inclusive(schemaReader, kc.IncludeRelations)
	}

	stream := NewKeryxStream(schemaReader, kc.MaxMessagePerTxn)
	return stream.StartKeryxStream(serverVersion, f, kc.DataDir, bufferWorkingDirectory)
}

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

//FullStream is a facade around the full process of taking WAL entries and publishing them as txn messages.
type FullStream struct {
	walStream       *streams.WalStream
	sr              *pg.SchemaReader
	MaxMessageCount uint
}

//NewKeryxStream takes a schema reader and returns a FullStream
func NewKeryxStream(sr *pg.SchemaReader, maxMessageCount uint) *FullStream {
	return &FullStream{nil, sr, maxMessageCount}
}

//Stop will end the reading on the WAL log and subsequent streams will therefore end.
func (fs *FullStream) Stop() {
	if fs.walStream != nil {
		fs.walStream.Stop()
	}
}

//StartKeryxStream will start all the streams necessary to go from WAL entries to txn messages.
func (fs *FullStream) StartKeryxStream(serverVersion string, filters filters.MessageFilter, dataDir string, bufferWorkingDirectory string) (<-chan *message.Transaction, error) {
	walStream, err := streams.NewWalStream(dataDir)
	if err != nil {
		return nil, err
	}
	fs.walStream = walStream

	wal, err := fs.walStream.Start()
	if err != nil {
		return nil, err
	}

	txnBuffer := &streams.TxnBuffer{Filters: filters, WorkingDirectory: bufferWorkingDirectory}
	buffered, err := txnBuffer.Start(wal)
	if err != nil {
		fs.Stop()
		return nil, err
	}

	populated := &streams.PopulatedMessageStream{Filter: filters, SchemaReader: fs.sr, MaxMessageCount: fs.MaxMessageCount}
	keryx, err := populated.Start(serverVersion, buffered)
	if err != nil {
		fs.Stop()
		return nil, err
	}

	return keryx, nil
}
