package es

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
	// postgres driver
	_ "github.com/lib/pq"
)

func MustConnectPostgres(url string) *sql.DB {
	db, err := sql.Open("postgres", url)
	if err != nil {
		panic(fmt.Sprintf("Failed connecting to the database: %v", err))
	}
	err = db.Ping()
	if err != nil {
		panic(fmt.Sprintf("Failed connecting to the database: %v", err))
	}
	db.SetConnMaxLifetime(time.Hour)
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	return db
}

type PostgresDriver struct {
	DB    *sql.DB
	Table string
}

func (d *PostgresDriver) Load(aggregateID string) ([]*Event, error) {
	rows, err := d.DB.Query(fmt.Sprintf(`
		SELECT
			ID,
			Created,
			AggregateID,
			AggregateVersion,
			AggregateType,
			Type,
			Payload
		FROM %s
		WHERE AggregateID = $1
		ORDER BY AggregateVersion
	`, d.tableName()), aggregateID)
	if err != nil {
		return []*Event{}, err
	}
	defer rows.Close()

	var events []*Event
	for rows.Next() {
		var event Event
		var rawPayload []byte
		err := rows.Scan(
			&event.ID,
			&event.Created,
			&event.AggregateID,
			&event.AggregateVersion,
			&event.AggregateType,
			&event.Type,
			&rawPayload,
		)
		if err != nil {
			return []*Event{}, err
		}
		typedPayload, err := resolveType(event.Type)
		if err != nil {
			return []*Event{}, err
		}
		err = json.Unmarshal(rawPayload, typedPayload)
		if err != nil {
			return []*Event{}, err
		}
		event.Payload = typedPayload
		events = append(events, &event)
	}
	err = rows.Err()
	if err != nil {
		return []*Event{}, err
	}
	return events, nil
}

func (d *PostgresDriver) Save(events []*Event) error {
	tx, err := d.DB.Begin() // TODO: double check the most appropriate isolation level for an append-only table (Read Committed?)
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(fmt.Sprintf(`
		INSERT INTO %s (
			AggregateID,
			AggregateVersion,
			AggregateType,
			Type,
			Payload
		) VALUES($1, $2, $3, $4, $5)
	`, d.tableName()))
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, event := range events {
		payload, err := json.Marshal(event.Payload)
		if err != nil {
			return err
		}

		_, err = stmt.Exec(
			event.AggregateID,
			event.AggregateVersion,
			event.AggregateType,
			event.Type,
			payload,
		)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (d *PostgresDriver) tableName() string {
	return pq.QuoteIdentifier(d.Table)
}
