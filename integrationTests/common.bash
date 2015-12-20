
setup () {
	SCRIPT_PATH=`dirname $0`
	DATABASE_NAME=smoke_test

	prepare_temp_dir () {
		TEMP_PATH="$SCRIPT_PATH/tmp"
		cleanup_temp_dir
		mkdir -p "$TEMP_PATH"
	}

	cleanup_temp_dir () {
		if [[ ! -z "$TEMP_PATH" && -d "$TEMP_PATH" ]]; then
			rm -rf "$TEMP_PATH"
		fi
	}

	prepare_postgres () {
		export PGDATA="$TEMP_PATH/postgres"
		export PGPORT=15432

		cleanup_postgres
		mkdir -p "$PGDATA"
		initdb 2>&1 > /dev/null
		pg_ctl -w -l "$PGDATA/logfile" start
		createdb "$DATABASE_NAME"
		echo "create table if not exists test (id serial primary key, count int not null);" | psql "$DATABASE_NAME" 2>&1 > /dev/null
	}

	cleanup_postgres () {
		if [[ ! -z "$PGDATA" && -d "$PGDATA" ]]; then
			pg_ctl -w stop -m immediate
			rm -rf "$PGDATA"
		fi
	}


	run_test () {
		SMOKE_CONFIG="$TEMP_PATH/smoke_config.json"
		BDIR="$TEMP_PATH/xact_buffer"
		mkdir -p "$BDIR"

		export SMOKE_DEBUG_LOG_FILE=`pwd`/`date +%s`_debug_log.txt
		sleep 1

		echo '{
			"data_dir": "PGDATA",
			"pg_conn_strings": [
				"postgres://USER:@localhost:PGPORT/smoke_test?sslmode=disable"
			],
			"buffer_max": 100000,
			"bind_address": ":19999",
			"relations_to_include": {
				"smoke_test.public.test": ["id", "count"]
			},
			"buffer_directory": "BDIR"
		}' | perl -p -e "my \$pgd = '$PGDATA'; my \$bdir = '$BDIR'; s/USER/$USER/; s/PGDATA/\$pgd/; s/PGPORT/$PGPORT/; s/BDIR/\$bdir/;" > "$SMOKE_CONFIG"

		echo "$SQL" | psql "$DATABASE_NAME" > /dev/null &
		echo "$CONDITION" | smoke --config "$SMOKE_CONFIG" --timeout 10 > /dev/null
	}

	cleanup_smoke () {
		if [[ ! -z "$SMOKE_CONFIG" && -f "$SMOKE_CONFIG" ]]; then
			rm "$SMOKE_CONFIG"
		fi
	}

	make_noise () {
		while [[ ! -z "$NOISE" ]]; do
			echo "$NOISE"
			sleep 1
		done | head -n 100 | psql "$DATABASE_NAME" > /dev/null || true
	}

	start () {
		prepare_temp_dir
		prepare_postgres
	}

	finish () {
		cleanup_smoke
		cleanup_postgres
		cleanup_temp_dir
	}
}
