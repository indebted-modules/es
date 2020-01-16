package es

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"

	// MySQL Driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"
)

// NewMySQLDriver creates a new MySQLDriver
func NewMySQLDriver(client *sql.DB, tableName string) *MySQLDriver {
	return &MySQLDriver{
		Client: client,
		TableName: tableName,
	}
}

// MySQLDriver implementation for deployed environments
type MySQLDriver struct {
	Client    *sql.DB
	TableName string
}

// MustConnectMySQL .
func MustConnectMySQL(dataSourceName string) *sql.DB {
	client, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		log.
			Fatal().
			Err(err).
			Msgf("Failed to connect")
	}
	return client
}

// Load all events by aggregate ID
func (s *MySQLDriver) Load(aggregateID string) ([]*Event, error) {
	rows, err := s.Client.Query(`SELECT * FROM event.event WHERE AggregateID = ? ORDER BY AggregateVersion`, aggregateID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := []*Event{}
	for rows.Next() {
		var rawPayload []byte
		var event Event
		var id uint64 // TODO revisit vs. string
		err := rows.Scan(&id, &event.Created, &event.AggregateID, &event.AggregateVersion, &event.Type, &event.AggregateType, &rawPayload)
		event.ID = strconv.FormatUint(id, 10) // TODO vs. string
		if err != nil {
			return nil, err
		}

		payload, err := resolveType(event.Type)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(rawPayload, payload)
		if err != nil {
			return nil, err
		}

		event.Payload = payload
		events = append(events, &event)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return events, nil
}

// Save all events into DynamoDB
func (s *MySQLDriver) Save(events []*Event) error {
	if len(events) == 0 {
		return nil
	}

	tx, err := s.Client.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`INSERT INTO event.event (Type, AggregateID, AggregateType, AggregateVersion, Payload) VALUES(?,?,?,?,?)`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	for _, event := range events {
		b, err := json.Marshal(event.Payload)
		if err != nil {
			return err
		}
		payload := string(b)

		_, err = stmt.Exec(
			//event.ID,
			event.Type,
			event.AggregateID,
			event.AggregateType,
			event.AggregateVersion,
			payload,
			//event.Created,
		)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}
