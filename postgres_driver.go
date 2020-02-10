package es

import (
	"database/sql"
	"encoding/json"
	"io"
	"time"

	// Postgres driver
	_ "github.com/lib/pq"
	"github.com/rs/zerolog/log"
)

const createTable = `
	CREATE TABLE events (
		ID               BIGSERIAL PRIMARY KEY,
		Type             VARCHAR(255) NOT NULL,
		-- TODO: Author VARCHAR(255) NOT NULL,
		Created          TIMESTAMPTZ DEFAULT now() NOT NULL,
		AggregateID      UUID NOT NULL,
		AggregateVersion INT NOT NULL,
		AggregateType    VARCHAR(255) NOT NULL,
		Payload          JSON NOT NULL,

		CONSTRAINT OptimisticLocking UNIQUE (AggregateID, AggregateVersion)
	)
`

// PostgresDriver implements a Postgres-backed event-store.
type PostgresDriver struct {
	DB *sql.DB
}

// CreateTable creates the event-store table with the necessary columns and
// constraints. It's name is dictated by the `Table` property set when
// initializing the `PostgresDriver` struct.
func (d *PostgresDriver) CreateTable() error {
	_, err := d.DB.Exec(createTable)
	if err != nil {
		return err
	}

	return nil
}

// Load loads all events for the given aggregateID ordered by version
func (d *PostgresDriver) Load(aggregateID string) ([]*Event, error) {
	rows, err := d.DB.Query(`
		SELECT
			ID,
			Type,
			Created,
			AggregateID,
			AggregateVersion,
			AggregateType,
			Payload
		FROM events
		WHERE AggregateID = $1
		ORDER BY AggregateVersion
	`, aggregateID)
	if err != nil {
		return nil, err
	}
	defer ShouldClose(rows)

	var events []*Event
	for rows.Next() {
		var event Event
		var rawPayload []byte
		err := rows.Scan(
			&event.ID,
			&event.Type,
			&event.Created,
			&event.AggregateID,
			&event.AggregateVersion,
			&event.AggregateType,
			&rawPayload,
		)
		if err != nil {
			return nil, err
		}
		typedPayload, err := resolveType(event.Type)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(rawPayload, typedPayload)
		if err != nil {
			return nil, err
		}
		event.Payload = typedPayload
		events = append(events, &event)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return events, nil
}

// Save saves all given events in the underlying event-store table. It does so
// in a transactional manner, meaning that if any of the events violates any
// constraints, none of the events will be persisted.
func (d *PostgresDriver) Save(events []*Event) error {
	tx, err := d.DB.Begin() // TODO: double check the most appropriate isolation level for an append-only table (Read Committed?)
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`
		INSERT INTO events (
			Type,
			AggregateID,
			AggregateVersion,
			AggregateType,
			Payload
		) VALUES($1, $2, $3, $4, $5)
	`)
	if err != nil {
		rErr := tx.Rollback()
		if rErr != nil {
			log.
				Fatal().
				Err(rErr).
				Msg("Failed rolling back transaction")
		}
		return err
	}
	defer ShouldClose(stmt)

	for _, event := range events {
		payload, err := json.Marshal(event.Payload)
		if err != nil {
			return err
		}

		_, err = stmt.Exec(
			event.Type,
			event.AggregateID,
			event.AggregateVersion,
			event.AggregateType,
			payload,
		)
		if err != nil {
			rErr := tx.Rollback()
			if rErr != nil {
				log.
					Fatal().
					Err(rErr).
					Msg("Failed rolling back transaction")
			}
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

// ReadEventsForward .
func (d *PostgresDriver) ReadEventsForward(position int64) ([]*Event, error) {
	rows, err := d.DB.Query(`
   		SELECT
			ID,
			Type,
			Created,
			AggregateID,
			AggregateVersion,
			AggregateType,
			Payload
	   	FROM events
	   	WHERE ID > $1
	   	ORDER BY AggregateVersion
   	`, position)
	if err != nil {
		return nil, err
	}
	defer ShouldClose(rows)

	events, err := d.rowsToEvents(rows)
	if err != nil {
		return nil, err
	}

	return events, err
}

func (d *PostgresDriver) rowsToEvents(rows *sql.Rows) ([]*Event, error) {
	var events []*Event
	for rows.Next() {
		var event Event
		var rawPayload []byte
		err := rows.Scan(
			&event.ID,
			&event.Type,
			&event.Created,
			&event.AggregateID,
			&event.AggregateVersion,
			&event.AggregateType,
			&rawPayload,
		)
		if err != nil {
			return nil, err
		}
		typedPayload, err := resolveType(event.Type)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(rawPayload, typedPayload)
		if err != nil {
			return nil, err
		}
		event.Payload = typedPayload
		events = append(events, &event)
	}
	err := rows.Err()
	if err != nil {
		return nil, err
	}

	return events, nil
}

// MustConnect ensures a healthy connection is established with the
// given URL. Panics otherwise.
func MustConnect(url string) *sql.DB {
	db, err := sql.Open("postgres", url)
	if err != nil {
		log.
			Fatal().
			Err(err).
			Msg("Failed opening connection to the database")
	}
	err = db.Ping()
	if err != nil {
		log.
			Fatal().
			Err(err).
			Msg("Failed sending ping to the database")
	}
	db.SetConnMaxLifetime(time.Hour)
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	return db
}

// ShouldClose ensures the given `io.Closer` is successfully closed.
// Warns otherwise.
func ShouldClose(closer io.Closer) {
	err := closer.Close()
	if err != nil {
		log.
			Warn().
			Err(err).
			Msg("Failed closing resource")
	}
}
