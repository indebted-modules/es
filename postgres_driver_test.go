package es_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/indebted-modules/es"
	"github.com/stretchr/testify/suite"
)

type PostgresDriverSuite struct {
	suite.Suite
	db     *sql.DB
	driver es.Driver
}

type Row struct {
	ID               int64
	Type             string
	Created          time.Time
	AggregateID      string
	AggregateVersion int64
	AggregateType    string
	Payload          []byte
}

func TestPostgresDriverSuite(t *testing.T) {
	suite.Run(t, new(PostgresDriverSuite))
}

func (s *PostgresDriverSuite) SetupTest() {
	s.db = es.MustConnect(os.Getenv("POSTGRES_URL"))

	_, err := s.db.Exec(`
		CREATE SCHEMA stub; CREATE FUNCTION stub.now() RETURNS TIMESTAMPTZ LANGUAGE SQL AS $$ SELECT '1985-10-26 01:22+00'::timestamptz; $$;
		SET search_path = stub,"$user",public,pg_catalog;
	`)
	s.NoError(err)

	postgresDriver := &es.PostgresDriver{
		DB: s.db,
	}
	err = postgresDriver.CreateTable()
	s.NoError(err)

	s.driver = postgresDriver
}

func (s *PostgresDriverSuite) TearDownTest() {
	_, err := s.db.Exec(`DROP TABLE IF EXISTS events`)
	s.NoError(err)
	_, err = s.db.Exec(`DROP SCHEMA IF EXISTS stub CASCADE`)
	s.NoError(err)
	err = s.db.Close()
	s.NoError(err)
}

func (s *PostgresDriverSuite) TestLoad() {
	stmt, err := s.db.Prepare(`
		INSERT INTO events (
			ID,
			Type,
			Created,
			AggregateID,
			AggregateVersion,
			AggregateType,
			Payload
		) VALUES($1, $2, $3, $4, $5, $6, $7)
	`)
	s.NoError(err)
	defer es.ShouldClose(stmt)

	_, err = stmt.Exec(
		1,
		"SomethingHappened",
		time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
		phonyUUID(1),
		0,
		"AggregateType",
		`{"Data": "AggregateID#1 - V0"}`,
	)
	s.NoError(err)

	_, err = stmt.Exec(
		2,
		"SomethingHappened",
		time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
		phonyUUID(2),
		0,
		"AggregateType",
		`{"Data": "AggregateID#2 - V0"}`,
	)
	s.NoError(err)

	_, err = stmt.Exec(
		3,
		"SomethingHappened",
		time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
		phonyUUID(1),
		1,
		"AggregateType",
		`{"Data": "AggregateID#1 - V1"}`,
	)
	s.NoError(err)

	events, err := s.driver.Load(phonyUUID(1))
	s.NoError(err)
	s.Equal([]*es.Event{
		{
			ID:               "1",
			Type:             "SomethingHappened",
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
			AggregateID:      phonyUUID(1),
			AggregateVersion: 0,
			AggregateType:    "AggregateType",
			Payload:          &SomethingHappened{Data: "AggregateID#1 - V0"},
		},
		{
			ID:               "3",
			Type:             "SomethingHappened",
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
			AggregateID:      phonyUUID(1),
			AggregateVersion: 1,
			AggregateType:    "AggregateType",
			Payload:          &SomethingHappened{Data: "AggregateID#1 - V1"},
		},
	}, events)

	events, err = s.driver.Load(phonyUUID(2))
	s.NoError(err)
	s.Equal([]*es.Event{
		{
			ID:               "2",
			Type:             "SomethingHappened",
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
			AggregateID:      phonyUUID(2),
			AggregateVersion: 0,
			AggregateType:    "AggregateType",
			Payload:          &SomethingHappened{Data: "AggregateID#2 - V0"},
		},
	}, events)
}

func (s *PostgresDriverSuite) TestSave() {
	events := []*es.Event{
		{
			Type:             "SomethingHappened",
			AggregateID:      phonyUUID(1),
			AggregateVersion: 0,
			AggregateType:    "AggregateType",
			Payload:          &SomethingHappened{Data: "AggregateID#1 - V0"},
		},
		{
			Type:             "SomethingHappened",
			AggregateID:      phonyUUID(1),
			AggregateVersion: 1,
			AggregateType:    "AggregateType",
			Payload:          &SomethingHappened{Data: "AggregateID#1 - V1"},
		},
		{
			Type:             "SomethingHappened",
			AggregateID:      phonyUUID(2),
			AggregateVersion: 0,
			AggregateType:    "AggregateType",
			Payload:          &SomethingHappened{Data: "AggregateID#2 - V0"},
		},
	}

	err := s.driver.Save(events)
	s.NoError(err)

	rows, err := s.db.Query(`
		SELECT
			ID,
			Type,
			Created,
			AggregateID,
			AggregateVersion,
			AggregateType,
			Payload
		FROM events
	`)
	s.NoError(err)
	result, err := readResult(rows)
	s.NoError(err)

	s.Equal([]*Row{
		{
			ID:               1,
			Type:             "SomethingHappened",
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
			AggregateID:      phonyUUID(1),
			AggregateVersion: 0,
			AggregateType:    "AggregateType",
			Payload:          []byte(`{"Data":"AggregateID#1 - V0"}`),
		},
		{
			ID:               2,
			Type:             "SomethingHappened",
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
			AggregateID:      phonyUUID(1),
			AggregateVersion: 1,
			AggregateType:    "AggregateType",
			Payload:          []byte(`{"Data":"AggregateID#1 - V1"}`),
		},
		{
			ID:               3,
			Type:             "SomethingHappened",
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
			AggregateID:      phonyUUID(2),
			AggregateVersion: 0,
			AggregateType:    "AggregateType",
			Payload:          []byte(`{"Data":"AggregateID#2 - V0"}`),
		},
	}, result)
}

func (s *PostgresDriverSuite) TestSaveOptimisticLocking() {
	events := []*es.Event{
		{
			Type:             "SomethingHappened",
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
			AggregateID:      phonyUUID(1),
			AggregateVersion: 0,
			AggregateType:    "AggregateType",
			Payload:          &SomethingHappened{Data: "AggregateID#1 - V0"},
		},
	}

	err := s.driver.Save(events)
	s.NoError(err)

	events = []*es.Event{
		{
			Type:             "SomethingHappened",
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
			AggregateID:      phonyUUID(1),
			AggregateVersion: 0,
			AggregateType:    "AggregateType",
			Payload:          &SomethingHappened{Data: "AggregateID#1 - V0"},
		},
	}

	err = s.driver.Save(events)
	s.Error(err)
	s.Regexp(".*violates unique constraint.*", err.Error())
}

func (s *PostgresDriverSuite) TestSaveInTransaction() {
	events := []*es.Event{
		{
			Type:             "SomethingHappened",
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
			AggregateID:      "AggregateID#1 - TX",
			AggregateVersion: 0,
			AggregateType:    "AggregateType",
			Payload:          &SomethingHappened{Data: "AggregateID#1 - TX"},
		},
		{
			Type:             "SomethingHappened",
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
			AggregateID:      "AggregateID#1 - TX",
			AggregateVersion: 0,
			AggregateType:    "AggregateType",
			Payload:          &SomethingHappened{Data: "AggregateID#1 - TX"},
		},
	}

	err := s.driver.Save(events)
	s.Error(err)

	var count int
	result := s.db.QueryRow(`SELECT COUNT(*) FROM events`)
	err = result.Scan(&count)
	s.NoError(err)
	s.Equal(0, count)
}

func (s *PostgresDriverSuite) TestSaveEmptyEvents() {
	err := s.driver.Save([]*es.Event{})
	s.NoError(err)
}

func (s *PostgresDriverSuite) TestReadEventsForward() {
	stmt, err := s.db.Prepare(`
   		INSERT INTO events (
	   		ID,
	   		Type,
	   		Created,
	   		AggregateID,
	   		AggregateVersion,
	   		AggregateType,
	   		Payload
	   	) VALUES($1, $2, $3, $4, $5, $6, $7)
   	`)
	s.NoError(err)
	defer es.ShouldClose(stmt)

	_, err = stmt.Exec(
		1,
		"SomethingHappened",
		time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
		phonyUUID(1),
		0,
		"SampleAggregate",
		`{"Data": "1"}`,
	)
	s.NoError(err)

	_, err = stmt.Exec(
		2,
		"SomethingHappened",
		time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
		phonyUUID(2),
		0,
		"SampleAggregate",
		`{"Data": "2"}`,
	)
	s.NoError(err)

	_, err = stmt.Exec(
		3,
		"SomethingElseHappened",
		time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
		phonyUUID(3),
		0,
		"AnotherSampleAggregate",
		`{"Data": "3"}`,
	)
	s.NoError(err)

	events, err := s.driver.ReadEventsOfTypes(0, 1)
	s.NoError(err)
	s.Equal([]*es.Event{
		{
			ID:               "1",
			Type:             "SomethingHappened",
			AggregateID:      phonyUUID(1),
			AggregateType:    "SampleAggregate",
			AggregateVersion: 0,
			Payload:          &SomethingHappened{Data: "1"},
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
		},
	}, events)

	events, err = s.driver.ReadEventsOfTypes(1, 1)
	s.NoError(err)
	s.Equal([]*es.Event{
		{
			ID:               "2",
			Type:             "SomethingHappened",
			AggregateID:      phonyUUID(2),
			AggregateType:    "SampleAggregate",
			AggregateVersion: 0,
			Payload:          &SomethingHappened{Data: "2"},
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
		},
	}, events)

	events, err = s.driver.ReadEventsOfTypes(2, 1)
	s.NoError(err)
	s.Equal([]*es.Event{
		{
			ID:               "3",
			Type:             "SomethingElseHappened",
			AggregateID:      phonyUUID(3),
			AggregateType:    "AnotherSampleAggregate",
			AggregateVersion: 0,
			Payload:          &SomethingElseHappened{Data: "3"},
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
		},
	}, events)

	events, err = s.driver.ReadEventsOfTypes(3, 1)
	s.NoError(err)
	s.Empty(events)

	events, err = s.driver.ReadEventsOfTypes(0, 2)
	s.NoError(err)
	s.Equal([]*es.Event{
		{
			ID:               "1",
			Type:             "SomethingHappened",
			AggregateID:      phonyUUID(1),
			AggregateType:    "SampleAggregate",
			AggregateVersion: 0,
			Payload:          &SomethingHappened{Data: "1"},
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
		},
		{
			ID:               "2",
			Type:             "SomethingHappened",
			AggregateID:      phonyUUID(2),
			AggregateType:    "SampleAggregate",
			AggregateVersion: 0,
			Payload:          &SomethingHappened{Data: "2"},
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
		},
	}, events)

	events, err = s.driver.ReadEventsOfTypes(1, 10)
	s.NoError(err)
	s.Equal([]*es.Event{
		{
			ID:               "2",
			Type:             "SomethingHappened",
			AggregateID:      phonyUUID(2),
			AggregateType:    "SampleAggregate",
			AggregateVersion: 0,
			Payload:          &SomethingHappened{Data: "2"},
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
		},
		{
			ID:               "3",
			Type:             "SomethingElseHappened",
			AggregateID:      phonyUUID(3),
			AggregateType:    "AnotherSampleAggregate",
			AggregateVersion: 0,
			Payload:          &SomethingElseHappened{Data: "3"},
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
		},
	}, events)
}

func readResult(rows *sql.Rows) ([]*Row, error) {
	defer es.ShouldClose(rows)
	var result []*Row
	for rows.Next() {
		var row Row
		err := rows.Scan(
			&row.ID,
			&row.Type,
			&row.Created,
			&row.AggregateID,
			&row.AggregateVersion,
			&row.AggregateType,
			&row.Payload,
		)
		if err != nil {
			return nil, err
		}
		result = append(result, &row)
	}
	return result, nil
}

func phonyUUID(n int) string {
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", n)
}
