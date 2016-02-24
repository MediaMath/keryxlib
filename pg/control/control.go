package control

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	controlSize      = 8192
	floatFormatValue = 1234567.0
)

// PgStateType describes the state that PostgreSQL is in.  See this link for details: https://github.com/postgres/postgres/blob/REL9_1_STABLE/src/include/catalog/pg_control.h#L69
type PgStateType uint64

// DATABASE State
const (
	DbStartup PgStateType = iota
	DbShutdowned
	DbShutdownedInRecovery
	DbShutdowning
	DbInCrashRecovery
	DbInArchiveRecovery
	DbInProduction
)

// WalLevel describes how detailed the write ahead log is.  See this link for details: https://github.com/postgres/postgres/blob/REL9_1_STABLE/src/include/access/xlog.h#L207
type WalLevel uint8

// WAL levels
const (
	WalLevelMinimal = iota
	WalLevelArchive
	WalLevelHotStandby
	WalLevelLogical
)

func (state PgStateType) String() string {
	switch state {
	case DbStartup:
		return "starting up"
	case DbShutdowned:
		return "shut down"
	case DbShutdownedInRecovery:
		return "shut down in recovery"
	case DbShutdowning:
		return "shutting down"
	case DbInCrashRecovery:
		return "in crash recovery"
	case DbInArchiveRecovery:
		return "in archive recovery"
	case DbInProduction:
		return "in production"
	}
	return "unrecognized status code"
}

func (walLevel WalLevel) String() string {
	switch walLevel {
	case WalLevelMinimal:
		return "minimal"
	case WalLevelArchive:
		return "archive"
	case WalLevelHotStandby:
		return "hot_standby"
	case WalLevelLogical:
		return "logical"
	}
	return "unrecognized wal_level"
}

// CheckPoint describes a checkpoint in the write ahead log.  See this link for descriptions of fields: https://github.com/postgres/postgres/blob/REL9_1_STABLE/src/include/catalog/pg_control.h#L31
type CheckPoint struct {
	RedoLogID        uint32
	RedoRecordOffset uint32
	ThisTimeLineID   uint32
	PrevTimeLineID   uint32
	FullPageWrites   uint8
	NextXidEpoch     uint32
	NextXid          uint32
	NextOid          uint32
	NextMulti        uint32
	NextMultiOffset  uint32
	OldestXid        uint32
	OldestXidDB      uint32
	OldestMulti      uint32
	OldestMultiDB    uint32
	Time             int64
	OldestCommitTs   uint32
	NewestCommitTs   uint32
	OldestActiveXid  uint32
}

// Control describes various runtime constants for PostgreSQL.  See this link for descriptions of fields: https://github.com/postgres/postgres/blob/REL9_1_STABLE/src/include/catalog/pg_control.h#L88
type Control struct {
	SystemIdentifier uint64
	Version          uint32
	CatalogVersionNo uint32

	State                      PgStateType
	Time                       int64
	CheckPointLogID            uint32
	CheckPointRecordOffset     uint32
	PrevCheckPointLogID        uint32
	PrevCheckPointRecordOffset uint32

	CheckPointCopy          CheckPoint
	UnloggedLSNLogID        uint32
	UnloggedLSNRecordOffset uint32

	MinRecoveryPointLogID        uint32
	MinRecoveryPointRecordOffset uint32
	MinRecoveryPointTLI          uint32
	BackupStartPointLogID        uint32
	BackupStartPointRecordOffset uint32
	BackupEndPointLogID          uint32
	BackupEndPointRecordOffset   uint32
	BackupEndRequired            uint8

	WalLevel             int32
	WalLogHints          uint8
	MaxConnections       int32
	MaxWorkerProcesses   int32
	MaxPreparedXacts     int32
	MaxLocksPerXact      int32
	TrackCommitTimestamp uint32

	MaxAlign    uint32
	FloatFormat float64

	Blcksz     uint32
	RelsegSize uint32

	XlogBlcksz  uint32
	XlogSegSize uint32

	NameDataLen  uint32
	IndexMaxKeys uint32

	ToastMaxChunkSize uint32
	Loblksize         uint32

	EnableIntTimes uint8
	Float4ByVal    uint8
	Float8ByVal    uint8

	DataChecksumVersion uint32

	Crc uint32
}

type fieldToParse struct {
	field    interface{}
	errorFmt string
}

// NewControlFromDataDir gets control from its default location relative to a data directory
func NewControlFromDataDir(dataDir string) (*Control, error) {
	return NewControlFromFile(filepath.Join(dataDir, "global", "pg_control"))
}

// NewControlFromFile gets control from a specific path
func NewControlFromFile(path string) (*Control, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	control, err := NewControl(file)
	file.Close()

	return control, err
}

// NewControl gets control from a reader
func NewControl(reader io.Reader) (*Control, error) {
	var (
		p8          uint8
		paddingByte = fieldToParse{&p8, "failed to read padding byte: %v"}
	)

	var pgData = new(Control)

	initialFields := []fieldToParse{
		fieldToParse{&pgData.SystemIdentifier, "failed to read Database system identifier: %v"},
		fieldToParse{&pgData.Version, "failed to read pg_control version number: %v"},
	}

	versionFields := map[uint32][]fieldToParse{
		903: []fieldToParse{
			fieldToParse{&pgData.CatalogVersionNo, "failed to read catalog_version_no: %v"},
			fieldToParse{&pgData.State, "failed to read Database cluster state: %v"},
			fieldToParse{&pgData.Time, "failed to read pg_control last modified time: %v"},
			fieldToParse{&pgData.CheckPointLogID, "failed to read log id of Latest checkpoint location: %v"},
			fieldToParse{&pgData.CheckPointRecordOffset, "failed to read record offset of Latest checkpoint location: %v"},
			fieldToParse{&pgData.PrevCheckPointLogID, "failed to read log id of Prior checkpoint location: %v"},
			fieldToParse{&pgData.PrevCheckPointRecordOffset, "failed to read record offset of Prior checkpoint location: %v"},
			fieldToParse{&pgData.CheckPointCopy.RedoLogID, "failed to read log id of Latest checkpoint's REDO location: %v"},
			fieldToParse{&pgData.CheckPointCopy.RedoRecordOffset, "failed to read record offset of Latest checkpoint's REDO location: %v"},
			fieldToParse{&pgData.CheckPointCopy.ThisTimeLineID, "failed to read Latest checkpoint's TimeLineID: %v"},
			fieldToParse{&pgData.CheckPointCopy.NextXid, "failed to read Latest checkpoint's NextXID: %v"},
			fieldToParse{&pgData.CheckPointCopy.NextXidEpoch, "failed to read Latest checkpoint's NextXID epoch: %v"},
			fieldToParse{&pgData.CheckPointCopy.NextOid, "failed to read Latest checkpoint's NextOID: %v"},
			fieldToParse{&pgData.CheckPointCopy.NextMulti, "failed to read Latest checkpoint's NextMultiXactId: %v"},
			fieldToParse{&pgData.CheckPointCopy.NextMultiOffset, "failed to read Latest checkpoint's NextMultiOffset: %v"},
			fieldToParse{&pgData.CheckPointCopy.OldestXid, "failed to read Latest checkpoint's oldestXID: %v"},
			fieldToParse{&pgData.CheckPointCopy.OldestXidDB, "failed to read Latest checkpoint's oldestXID's DB: %v"},
			fieldToParse{&pgData.CheckPointCopy.Time, "failed to read Time of latest checkpoint: %v"},
			fieldToParse{&pgData.CheckPointCopy.OldestActiveXid, "failed to read Latest checkpoint's oldestActiveXID: %v"},
			fieldToParse{&pgData.MinRecoveryPointLogID, "failed to read log id of Minimum recovery ending location: %v"},
			fieldToParse{&pgData.MinRecoveryPointRecordOffset, "failed to read record offset of Minimum recovery ending location: %v"},
			fieldToParse{&pgData.BackupStartPointLogID, "failed to read log id of Backup start location: %v"},
			fieldToParse{&pgData.BackupStartPointRecordOffset, "failed to read record offset of Backup start location: %v"},
			fieldToParse{&pgData.WalLevel, "failed to read Current wal_level setting: %v"},
			paddingByte,
			paddingByte,
			paddingByte,
			paddingByte,
			fieldToParse{&pgData.MaxConnections, "failed to read Current max_connections setting: %v"},
			fieldToParse{&pgData.MaxPreparedXacts, "failed to read Current max_prepared_xacts setting: %v"},
			fieldToParse{&pgData.MaxLocksPerXact, "failed to read current max_locks_per_xact setting: %v"},
			fieldToParse{&pgData.MaxAlign, "failed to read Maximum data alignment: %v"},
			fieldToParse{&pgData.TrackCommitTimestamp, "%v"},
			fieldToParse{&pgData.FloatFormat, "%v"},
			fieldToParse{&pgData.Blcksz, "failed to read database block size: %v"},
			fieldToParse{&pgData.RelsegSize, "failed to read Blocks per segment of large relation: %v"},
			fieldToParse{&pgData.XlogBlcksz, "failed to read WAL block size: %v"},
			fieldToParse{&pgData.XlogSegSize, "failed to read Bytes per WAL segment: %v"},
			fieldToParse{&pgData.NameDataLen, "failed to read Maximum length of identifiers: %v"},
			fieldToParse{&pgData.IndexMaxKeys, "failed to read Maximum columns in an index: %v"},
			fieldToParse{&pgData.ToastMaxChunkSize, "failed to read Maximum size of a TOAST chunk: %v"},
			fieldToParse{&pgData.EnableIntTimes, "%v"},
			fieldToParse{&pgData.Float4ByVal, "%v"},
			fieldToParse{&pgData.Float8ByVal, "%v"},
			paddingByte,
			fieldToParse{&pgData.DataChecksumVersion, "%v"},
			fieldToParse{&pgData.Crc, "%v"},
		},
		942: []fieldToParse{
			fieldToParse{&pgData.CatalogVersionNo, "failed to read catalog_version_no: %v"},
			fieldToParse{&pgData.State, "failed to read Database cluster state: %v"},
			fieldToParse{&pgData.Time, "failed to read pg_control last modified time: %v"},
			fieldToParse{&pgData.CheckPointRecordOffset, "failed to read record offset of Latest checkpoint location: %v"},
			fieldToParse{&pgData.CheckPointLogID, "failed to read log id of Latest checkpoint location: %v"},
			fieldToParse{&pgData.PrevCheckPointRecordOffset, "failed to read record offset of Prior checkpoint location: %v"},
			fieldToParse{&pgData.PrevCheckPointLogID, "failed to read log id of Prior checkpoint location: %v"},
			fieldToParse{&pgData.CheckPointCopy.RedoRecordOffset, "failed to read record offset of Latest checkpoint's REDO location: %v"},
			fieldToParse{&pgData.CheckPointCopy.RedoLogID, "failed to read log id of Latest checkpoint's REDO location: %v"},
			fieldToParse{&pgData.CheckPointCopy.ThisTimeLineID, "failed to read Latest checkpoint's TimeLineID: %v"},
			fieldToParse{&pgData.CheckPointCopy.PrevTimeLineID, "failed to read Latest checkpoint's prev TimeLineID: %v"},
			fieldToParse{&pgData.CheckPointCopy.FullPageWrites, "failed to read Latest checkpoint's FullPageWrites: %v"},
			paddingByte,
			paddingByte,
			paddingByte,
			fieldToParse{&pgData.CheckPointCopy.NextXidEpoch, "failed to read Latest checkpoint's NextXID epoch: %v"},
			fieldToParse{&pgData.CheckPointCopy.NextXid, "failed to read Latest checkpoint's NextXID: %v"},
			fieldToParse{&pgData.CheckPointCopy.NextOid, "failed to read Latest checkpoint's NextOID: %v"},
			fieldToParse{&pgData.CheckPointCopy.NextMulti, "failed to read Latest checkpoint's NextMultiXactId: %v"},
			fieldToParse{&pgData.CheckPointCopy.NextMultiOffset, "failed to read Latest checkpoint's NextMultiOffset: %v"},
			fieldToParse{&pgData.CheckPointCopy.OldestXid, "failed to read Latest checkpoint's oldestXID: %v"},
			fieldToParse{&pgData.CheckPointCopy.OldestXidDB, "failed to read Latest checkpoint's oldestXID's DB: %v"},
			fieldToParse{&pgData.CheckPointCopy.OldestMulti, "failed to read Latest checkpoint's OldestMulti: %v"},
			fieldToParse{&pgData.CheckPointCopy.OldestMultiDB, "failed to read Latest checkpoint's OldestMultiDB: %v"},
			fieldToParse{&pgData.CheckPointCopy.Time, "failed to read Time of latest checkpoint: %v"},
			fieldToParse{&pgData.CheckPointCopy.OldestActiveXid, "failed to read Latest checkpoint's oldestActiveXID: %v"},
			paddingByte,
			paddingByte,
			paddingByte,
			paddingByte,
			fieldToParse{&pgData.UnloggedLSNRecordOffset, "failed to read record offset of unlogged LSN: %v"},
			fieldToParse{&pgData.UnloggedLSNLogID, "failed to read log id of unlogged LSN: %v"},
			fieldToParse{&pgData.MinRecoveryPointRecordOffset, "failed to read record offset of Minimum recovery ending location: %v"},
			fieldToParse{&pgData.MinRecoveryPointLogID, "failed to read log id of Minimum recovery ending location: %v"},
			fieldToParse{&pgData.MinRecoveryPointTLI, "failed to read log id of Minimum timeline ID: %v"},
			paddingByte,
			paddingByte,
			paddingByte,
			paddingByte,
			fieldToParse{&pgData.BackupStartPointRecordOffset, "failed to read record offset of Backup start location: %v"},
			fieldToParse{&pgData.BackupStartPointLogID, "failed to read log id of Backup start location: %v"},
			fieldToParse{&pgData.BackupEndPointRecordOffset, "failed to read record offset of Backup end location: %v"},
			fieldToParse{&pgData.BackupEndPointLogID, "failed to read log id of Backup end location: %v"},
			fieldToParse{&pgData.BackupEndRequired, "failed to read Backup end required: %v"},
			paddingByte,
			paddingByte,
			paddingByte,
			fieldToParse{&pgData.WalLevel, "failed to read Current wal_level setting: %v"},
			fieldToParse{&pgData.WalLogHints, "failed to read wal log hints: %v"},
			paddingByte,
			paddingByte,
			paddingByte,
			fieldToParse{&pgData.MaxConnections, "failed to read Current max_connections setting: %v"},
			fieldToParse{&pgData.MaxWorkerProcesses, "failed to read Current max_worker_processes setting: %v"},
			fieldToParse{&pgData.MaxPreparedXacts, "failed to read Current max_prepared_xacts setting: %v"},
			fieldToParse{&pgData.MaxLocksPerXact, "failed to read current max_locks_per_xact setting: %v"},
			fieldToParse{&pgData.MaxAlign, "failed to read Maximum data alignment: %v"},
			fieldToParse{&pgData.FloatFormat, "%v"},
			fieldToParse{&pgData.Blcksz, "failed to read database block size: %v"},
			fieldToParse{&pgData.RelsegSize, "failed to read Blocks per segment of large relation: %v"},
			fieldToParse{&pgData.XlogBlcksz, "failed to read WAL block size: %v"},
			fieldToParse{&pgData.XlogSegSize, "failed to read Bytes per WAL segment: %v"},
			fieldToParse{&pgData.NameDataLen, "failed to read Maximum length of identifiers: %v"},
			fieldToParse{&pgData.IndexMaxKeys, "failed to read Maximum columns in an index: %v"},
			fieldToParse{&pgData.ToastMaxChunkSize, "failed to read Maximum size of a TOAST chunk: %v"},
			fieldToParse{&pgData.Loblksize, "failed to read chunk size in pg_largeobject: %v"},
			fieldToParse{&pgData.EnableIntTimes, "%v"},
			fieldToParse{&pgData.Float4ByVal, "%v"},
			fieldToParse{&pgData.Float8ByVal, "%v"},
			fieldToParse{&pgData.DataChecksumVersion, "%v"},
			fieldToParse{&pgData.Crc, "%v"},
		},
	}

	for _, ftp := range initialFields {
		err := binary.Read(reader, binary.LittleEndian, ftp.field)
		if err != nil {
			return nil, fmt.Errorf(ftp.errorFmt, err)
		}
	}

	if fields, ok := versionFields[pgData.Version]; ok {
		for _, ftp := range fields {
			err := binary.Read(reader, binary.LittleEndian, ftp.field)
			if err != nil {
				return nil, fmt.Errorf(ftp.errorFmt, err)
			}
		}
	} else {
		return nil, fmt.Errorf("unknown version %v", pgData.Version)
	}

	return pgData, nil
}
