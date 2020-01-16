package es_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/indebted-modules/es"
	"github.com/stretchr/testify/suite"

	// MySQL Driver
	_ "github.com/go-sql-driver/mysql"
)

type MySQLDriverSuite struct {
	suite.Suite
	Client *sql.DB
	Driver es.Driver
}

type eventRow struct {
	id               uint64
	typ              string
	aggregateID      string
	aggregateType    string
	aggregateVersion int64
	rawPayload       []byte
	created          time.Time
}

type eventPayload struct {
	ID string
}

func (eventPayload) PayloadType() string {
	return "eventPayload"
}

func (eventPayload) AggregateType() string {
	return "AggregateType"
}

func TestMySQLDriverSuite(t *testing.T) {
	suite.Run(t, new(MySQLDriverSuite))
}

func (s *MySQLDriverSuite) SetupTest() {
	es.Register(eventPayload{})
	db, err := sql.Open("mysql", "root:example@/?parseTime=true")
	s.Nil(err)

	_, err = db.Exec("CREATE DATABASE test")
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS test.event (
	  ID BIGINT(20) UNSIGNED AUTO_INCREMENT not null,
	  Type VARCHAR(255) not null,
	  AggregateID VARCHAR(255) not null,
	  AggregateType VARCHAR(255) not null,
	  AggregateVersion INT not null,
	  Payload JSON not null,
	  Created TIMESTAMP DEFAULT CURRENT_TIMESTAMP not null,
	  PRIMARY KEY (ID),
	  UNIQUE KEY (AggregateID, AggregateVersion)
	)`)
	s.Nil(err)

	s.Client = db
	s.Driver = &es.MySQLDriver{
		Client: s.Client,
	}
}

func (s *MySQLDriverSuite) TearDownTest() {
	_, err := s.Client.Exec(`TRUNCATE TABLE test.event`)
	s.Nil(err)
}

func (s *MySQLDriverSuite) TestLoad() {
	stmt, err := s.Client.Prepare(`INSERT INTO test.event VALUES(?,?,?,?,?,?,?)`)
	s.Nil(err)

	_, err = stmt.Exec(
		"load-event-id-1",
		"eventPayload",
		"load-aggregate-id",
		"AggregateType",
		0,
		`{"ID": "load-event-id-1"}`,
		"2019-06-30 00:00:00",
	)
	s.Nil(err)

	_, err = stmt.Exec(
		"load-event-id-2",
		"eventPayload",
		"load-aggregate-id",
		"AggregateType",
		1,
		`{"ID": "load-event-id-2"}`,
		"2019-06-30 00:00:00",
	)
	s.Nil(err)

	_, err = stmt.Exec(
		"load-event-id-3",
		"eventPayload",
		"load-another-aggregate-id",
		"AggregateType",
		0,
		`{"ID": "load-event-id-3"}`,
		"2019-06-30 00:00:00",
	)
	s.Nil(err)

	events, err := s.Driver.Load("load-aggregate-id")
	s.Equal(2, len(events))
	s.Nil(err)

	s.Equal(&es.Event{
		ID:               "load-event-id-1",
		Type:             "eventPayload",
		AggregateID:      "load-aggregate-id",
		AggregateType:    "AggregateType",
		AggregateVersion: 0,
		Payload:          &eventPayload{ID: "load-event-id-1"},
		Created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
	}, events[0])

	s.Equal(&es.Event{
		ID:               "load-event-id-2",
		Type:             "eventPayload",
		AggregateID:      "load-aggregate-id",
		AggregateType:    "AggregateType",
		AggregateVersion: 1,
		Payload:          &eventPayload{ID: "load-event-id-2"},
		Created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
	}, events[1])

	events, err = s.Driver.Load("load-another-aggregate-id")
	s.Equal(1, len(events))
	s.Nil(err)

	s.Equal(&es.Event{
		ID:               "load-event-id-3",
		Type:             "eventPayload",
		AggregateID:      "load-another-aggregate-id",
		AggregateType:    "AggregateType",
		AggregateVersion: 0,
		Payload:          &eventPayload{ID: "load-event-id-3"},
		Created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
	}, events[0])
}

func (s *MySQLDriverSuite) TestSave() {
	events := []*es.Event{
		{
			ID:               "1001",
			Type:             "eventPayload",
			AggregateID:      "save-aggregate-id",
			AggregateType:    "AggregateType",
			AggregateVersion: 0,
			Payload:          &eventPayload{ID: "save-event-id-1"},
			Created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
		},
		{
			ID:               "1002",
			Type:             "eventPayload",
			AggregateID:      "save-aggregate-id",
			AggregateType:    "AggregateType",
			AggregateVersion: 1,
			Payload:          &eventPayload{ID: "save-event-id-2"},
			Created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
		},
		{
			ID:               "1003",
			Type:             "eventPayload",
			AggregateID:      "save-another-aggregate-id",
			AggregateType:    "AggregateType",
			AggregateVersion: 0,
			Payload:          &eventPayload{ID: "save-event-id-3"},
			Created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
		},
	}

	err := s.Driver.Save(events)
	s.Nil(err)

	rows, err := s.Client.Query("SELECT * FROM test.event")
	s.Nil(err)

	resultSet, err := readResultSet(rows)
	s.Nil(err)

	s.Contains(resultSet, &eventRow{
		id:               1001,
		typ:              "eventPayload",
		aggregateID:      "save-aggregate-id",
		aggregateType:    "AggregateType",
		aggregateVersion: 0,
		rawPayload:       []byte(`{"ID": "save-event-id-1"}`),
		created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
	})

	s.Contains(resultSet, &eventRow{
		id:               1002,
		typ:              "eventPayload",
		aggregateID:      "save-aggregate-id",
		aggregateType:    "AggregateType",
		aggregateVersion: 1,
		rawPayload:       []byte(`{"ID": "save-event-id-2"}`),
		created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
	})

	s.Contains(resultSet, &eventRow{
		id:               1003,
		typ:              "eventPayload",
		aggregateID:      "save-another-aggregate-id",
		aggregateType:    "AggregateType",
		aggregateVersion: 0,
		rawPayload:       []byte(`{"ID": "save-event-id-3"}`),
		created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
	})
}

func (s *MySQLDriverSuite) TestSaveOptimisticLocking() {
	events := []*es.Event{
		{
			ID:               "lock-event-id-1",
			Type:             "eventPayload",
			AggregateID:      "lock-aggregate-id",
			AggregateType:    "AggregateType",
			AggregateVersion: 0,
			Payload:          &eventPayload{ID: "lock-event-id-1"},
			Created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
		},
	}

	err := s.Driver.Save(events)
	s.Nil(err)

	events = []*es.Event{
		{
			ID:               "lock-event-id-2",
			Type:             "eventPayload",
			AggregateID:      "lock-aggregate-id",
			AggregateType:    "AggregateType",
			AggregateVersion: 0,
			Payload:          &eventPayload{ID: "lock-event-id-2"},
			Created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
		},
	}

	err = s.Driver.Save(events)
	s.Error(err)
}

func (s *MySQLDriverSuite) TestSaveInTransaction() {
	events := []*es.Event{
		{
			ID:               "tx-event-id-1",
			Type:             "eventPayload",
			AggregateID:      "tx-aggregate-id",
			AggregateType:    "AggregateType",
			AggregateVersion: 0,
			Payload:          &eventPayload{ID: "tx-event-id-1"},
			Created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
		},
		{
			ID:               "tx-event-id-2",
			Type:             "eventPayload",
			AggregateID:      "tx-aggregate-id",
			AggregateType:    "AggregateType",
			AggregateVersion: 0,
			Payload:          &eventPayload{ID: "tx-event-id-2"},
			Created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
		},
	}

	err := s.Driver.Save(events)
	s.Error(err)

	rows, err := s.Client.Query("SELECT * FROM test.event WHERE aggregate_id=? AND aggregate_version=?", "tx-aggregate-id", 0)
	s.Nil(err)

	resultSet, err := readResultSet(rows)
	s.Nil(err)
	s.Empty(resultSet)
}

func (s *MySQLDriverSuite) TestSaveEmptyEvents() {
	err := s.Driver.Save([]*es.Event{})
	s.Nil(err)
}

func readResultSet(rows *sql.Rows) ([]*eventRow, error) {
	var resultSet []*eventRow
	for rows.Next() {
		var row eventRow
		err := rows.Scan(&row.id, &row.typ, &row.aggregateID, &row.aggregateType, &row.aggregateVersion, &row.rawPayload, &row.created)
		if err != nil {
			return nil, err
		}
		resultSet = append(resultSet, &row)
	}
	return resultSet, nil
}
