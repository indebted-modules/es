package es_test

import (
	"database/sql"
	"fmt"
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

type TestPayload struct{ Data string }

func (TestPayload) PayloadType() string   { return "TestPayload" }
func (TestPayload) AggregateType() string { return "AggregateType" }
func init()                               { es.Register(TestPayload{}) }

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
	s.db = es.MustConnect("postgres://user:password@postgres/indebted?sslmode=disable")

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
	defer stmt.Close()

	_, err = stmt.Exec(
		1,
		"TestPayload",
		time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
		phonyUUID(1),
		0,
		"AggregateType",
		`{"Data": "AggregateID#1 - V0"}`,
	)
	s.NoError(err)

	_, err = stmt.Exec(
		2,
		"TestPayload",
		time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
		phonyUUID(2),
		0,
		"AggregateType",
		`{"Data": "AggregateID#2 - V0"}`,
	)
	s.NoError(err)

	_, err = stmt.Exec(
		3,
		"TestPayload",
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
			Type:             "TestPayload",
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
			AggregateID:      phonyUUID(1),
			AggregateVersion: 0,
			AggregateType:    "AggregateType",
			Payload:          &TestPayload{Data: "AggregateID#1 - V0"},
		},
		{
			ID:               "3",
			Type:             "TestPayload",
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
			AggregateID:      phonyUUID(1),
			AggregateVersion: 1,
			AggregateType:    "AggregateType",
			Payload:          &TestPayload{Data: "AggregateID#1 - V1"},
		},
	}, events)

	events, err = s.driver.Load(phonyUUID(2))
	s.NoError(err)
	s.Equal([]*es.Event{
		{
			ID:               "2",
			Type:             "TestPayload",
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
			AggregateID:      phonyUUID(2),
			AggregateVersion: 0,
			AggregateType:    "AggregateType",
			Payload:          &TestPayload{Data: "AggregateID#2 - V0"},
		},
	}, events)
}

func (s *PostgresDriverSuite) TestSave() {
	events := []*es.Event{
		{
			Type:             "TestPayload",
			AggregateID:      phonyUUID(1),
			AggregateVersion: 0,
			AggregateType:    "AggregateType",
			Payload:          &TestPayload{Data: "AggregateID#1 - V0"},
		},
		{
			Type:             "TestPayload",
			AggregateID:      phonyUUID(1),
			AggregateVersion: 1,
			AggregateType:    "AggregateType",
			Payload:          &TestPayload{Data: "AggregateID#1 - V1"},
		},
		{
			Type:             "TestPayload",
			AggregateID:      phonyUUID(2),
			AggregateVersion: 0,
			AggregateType:    "AggregateType",
			Payload:          &TestPayload{Data: "AggregateID#2 - V0"},
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
			Type:             "TestPayload",
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
			AggregateID:      phonyUUID(1),
			AggregateVersion: 0,
			AggregateType:    "AggregateType",
			Payload:          []byte(`{"Data":"AggregateID#1 - V0"}`),
		},
		{
			ID:               2,
			Type:             "TestPayload",
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
			AggregateID:      phonyUUID(1),
			AggregateVersion: 1,
			AggregateType:    "AggregateType",
			Payload:          []byte(`{"Data":"AggregateID#1 - V1"}`),
		},
		{
			ID:               3,
			Type:             "TestPayload",
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
			Type:             "TestPayload",
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
			AggregateID:      phonyUUID(1),
			AggregateVersion: 0,
			AggregateType:    "AggregateType",
			Payload:          &TestPayload{Data: "AggregateID#1 - V0"},
		},
	}

	err := s.driver.Save(events)
	s.NoError(err)

	events = []*es.Event{
		{
			Type:             "TestPayload",
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
			AggregateID:      phonyUUID(1),
			AggregateVersion: 0,
			AggregateType:    "AggregateType",
			Payload:          &TestPayload{Data: "AggregateID#1 - V0"},
		},
	}

	err = s.driver.Save(events)
	s.Error(err)
	s.Regexp(".*violates unique constraint.*", err.Error())
}

func (s *PostgresDriverSuite) TestSaveInTransaction() {
	events := []*es.Event{
		{
			Type:             "TestPayload",
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
			AggregateID:      "AggregateID#1 - TX",
			AggregateVersion: 0,
			AggregateType:    "AggregateType",
			Payload:          &TestPayload{Data: "AggregateID#1 - TX"},
		},
		{
			Type:             "TestPayload",
			Created:          time.Date(1985, time.October, 26, 1, 22, 0, 0, time.UTC),
			AggregateID:      "AggregateID#1 - TX",
			AggregateVersion: 0,
			AggregateType:    "AggregateType",
			Payload:          &TestPayload{Data: "AggregateID#1 - TX"},
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
	s.Nil(err)
}

func readResult(rows *sql.Rows) ([]*Row, error) {
	defer rows.Close()
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
