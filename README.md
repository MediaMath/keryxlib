## keryxlib - parse postgres WAL logs as json events

**Deprecated** - this library is no longer actively maintained.  Move to logical decoding in postgres 9.4 or higher.

Keryxlib is a system for parsing postgres WAL logs and turning them into json events.  The basic algorithm for this is:

- Parse the WAL log and create a [wal.Entry](pg/wal/entry.go) record.  This is a basic record that contains basic WAL data.
- Entry records are buffered until either a commit or a rollback is detected.  Entries that are rolled back are discarded.
- On commit each entry is turned into a [message.Message](message/message.go) record.
- Each message is populated by querying the postgres RDBMS.
- Messages are added to a [message.Transaction](message/message.go) record for delivery.

### Example usage

```go
msgChan, err := keryxlib.TransactionChannel("<ID STRING EMBEDDED IN TRANSACTIONS", config)
```

The [config](config.go) file that is passed into this function:

```json
{
	"data_dir": "/opt/postgresql/data",
	"max_message_per_txn":1000, 
	"pg_conn_strings": [
		"user=user1 password=password1 host=/var/run/postgresql port=5432 dbname=db1 sslmode=disable"
	],
	"buffer_max": 100000,
	"buffer_directory": "/var/tmp/keryx/buffer",
	"exclude": {
		"db1.public.users":["password"],
		"db1.schema1.boo":["*"],
		"db1.schema2.moo":["*"],
		"db1.schema3.goo":["*"]
	}
}
```

#### Filters

Frequently it is useful to not include certain output in the keryx channel.  To support this keryxlib supports filtering tables prior to buffering the WAL entry.  It also supports filtering out specific columns at the population step.  The format for filtering is "dbname.schemaname.tablename":["columnname1", "columnname2"].  Filtering also supports * in the colun name array, which means all columns.

##### Inclusive vs Exclusive Filtering

The [filters](filters) package supports both inclusive and exclusive filtering.  In inclusive filtering only tables and columns that are explicitly listed will be sent into the channel.  In exclusive filtering, all tables and columns will be sent by default, but any columns that are explicitly listed in the filter will be removed from the published messages and any tables that have * columns excluded will be excluded entirely.

#### Database Connections

The postgres WAL log contains records for every database in the postgres instance.  A single connection for querying the RDBMS must be on a database by database level.  Therefore if you want to send messages for multiple databases in the message channel you must provide multiple connection strings, one for each database.  Any message for a database that does not have a connection string will be automatically filtered from the message channel.

#### Big Transactions

Transactions in some cases can become very big.  The cost of populating these very large transactions is very expensive.  In some cases this cost is not worth the effort.  If "max_message_per_txn" is set any transaction that has more messages than that value in it, will not populate the messages field and instead will have the tables that were impacted in the transaction listed as well as a count for the number of messages.


### Keryxlib misses data when... 

Keryxlib will miss data in certain known cases.

#### Deletes

By the time keryxlib sees deletes from the WAL log, the information about the fields that were deleted is already gone. Therefore delete messages will not have any field level information, including any IDs of the row in question.  The tuple id will be available.  This means that if your system needs to publish the ids of a specific delete then you will need to augment keryxlib with an external mapping between tuple id and entity id.

#### Population lag causes missed message population

Keryxlib runs behind the postgres replication application.  If a second update or delete is applied to a row that keryxlib is trying to populate before keryxlib can populate it, that message will have a population error applied to it, and message field information will not be available for that message.  In cases where lots of WAL log entries are written, keryxlib will fall behind and the lag between it and postgres will increase.  In that case the chance of an overwrite and subsequent population error increases.

#### WAL log files removed before keryxlib can read them.

If WAL log rotation happens on files that keryxlib has not read then that data will be missed by keryxlib.  In some degenerate cases the WAL log rotation happens very fast and keryxlib cannot keep up.  Conversely, in some cases keryxlib is reading too *fast* and encounters WAL log files that are not yet populated with new replication data.  In that case it will wait for the WAL log application to catch up.

#### Insufficient query priveleges to populate a message.

While keryxlib will filter any messages for databases it does not have a connection for, if a message comes in for a database with a connection, but for a schema or table that the connections user cannot read, the message will be published with a population error.
