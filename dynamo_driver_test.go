package es_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/indebted-modules/es"
	"github.com/indebted-modules/uuid"
	"github.com/stretchr/testify/suite"
)

type DynamoDriverSuite struct {
	suite.Suite
	Client    *dynamodb.DynamoDB
	Driver    es.Driver
	TableName string
}

func TestDynamoDriverSuite(t *testing.T) {
	suite.Run(t, new(DynamoDriverSuite))
}

func (s *DynamoDriverSuite) SetupSuite() {
	es.Register(EventType{})
	s.TableName = fmt.Sprintf("store-test-%s", uuid.NewID())

	s.Client = dynamodb.New(
		session.Must(session.NewSession()),
		&aws.Config{
			Region:      aws.String("local"),
			Credentials: credentials.NewStaticCredentials("id", "secret", "token"),
			Endpoint:    aws.String(os.Getenv("DYNAMO_URL")),
		},
	)

	s.Driver = &es.DynamoDriver{
		Client:    s.Client,
		TableName: s.TableName,
	}

	_, err := s.Client.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(s.TableName),
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("AggregateID"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("AggregateVersion"),
				KeyType:       aws.String("RANGE"),
			},
		},
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("AggregateID"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("AggregateVersion"),
				AttributeType: aws.String("N"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	})
	s.Nil(err)

}

func (s *DynamoDriverSuite) TestLoad() {
	_, err := s.Client.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(s.TableName),
		Item: map[string]*dynamodb.AttributeValue{
			"ID":               {S: aws.String("load-event-id-1")},
			"Type":             {S: aws.String("EventType")},
			"AggregateID":      {S: aws.String("load-aggregate-id")},
			"AggregateType":    {S: aws.String("AggregateType")},
			"AggregateVersion": {N: aws.String("0")},
			"Payload":          {S: aws.String(`{"ID": "load-event-id-1"}`)},
			"Created":          {S: aws.String("2019-06-30T00:00:00Z")},
		},
	})
	s.Nil(err)

	_, err = s.Client.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(s.TableName),
		Item: map[string]*dynamodb.AttributeValue{
			"ID":               {S: aws.String("load-event-id-2")},
			"Type":             {S: aws.String("EventType")},
			"AggregateID":      {S: aws.String("load-aggregate-id")},
			"AggregateType":    {S: aws.String("AggregateType")},
			"AggregateVersion": {N: aws.String("1")},
			"Payload":          {S: aws.String(`{"ID": "load-event-id-2"}`)},
			"Created":          {S: aws.String("2019-06-30T00:00:00Z")},
		},
	})
	s.Nil(err)

	_, err = s.Client.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(s.TableName),
		Item: map[string]*dynamodb.AttributeValue{
			"ID":               {S: aws.String("load-event-id-3")},
			"Type":             {S: aws.String("EventType")},
			"AggregateID":      {S: aws.String("load-another-aggregate-id")},
			"AggregateType":    {S: aws.String("AggregateType")},
			"AggregateVersion": {N: aws.String("0")},
			"Payload":          {S: aws.String(`{"ID": "load-event-id-3"}`)},
			"Created":          {S: aws.String("2019-06-30T00:00:00Z")},
		},
	})
	s.Nil(err)

	events, err := s.Driver.Load("load-aggregate-id")
	s.Equal(2, len(events))
	s.Nil(err)

	s.Equal(&es.Event{
		ID:               "load-event-id-1",
		Type:             "EventType",
		AggregateID:      "load-aggregate-id",
		AggregateType:    "AggregateType",
		AggregateVersion: 0,
		Payload:          &EventType{ID: "load-event-id-1"},
		Created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
	}, events[0])

	s.Equal(&es.Event{
		ID:               "load-event-id-2",
		Type:             "EventType",
		AggregateID:      "load-aggregate-id",
		AggregateType:    "AggregateType",
		AggregateVersion: 1,
		Payload:          &EventType{ID: "load-event-id-2"},
		Created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
	}, events[1])

	events, err = s.Driver.Load("load-another-aggregate-id")
	s.Equal(1, len(events))
	s.Nil(err)

	s.Equal(&es.Event{
		ID:               "load-event-id-3",
		Type:             "EventType",
		AggregateID:      "load-another-aggregate-id",
		AggregateType:    "AggregateType",
		AggregateVersion: 0,
		Payload:          &EventType{ID: "load-event-id-3"},
		Created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
	}, events[0])
}

func (s *DynamoDriverSuite) TestSave() {
	events := []*es.Event{
		{
			ID:               "save-event-id-1",
			Type:             "EventType",
			AggregateID:      "save-aggregate-id",
			AggregateType:    "AggregateType",
			AggregateVersion: 0,
			Payload:          &EventType{ID: "save-event-id-1"},
			Created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
		},
		{
			ID:               "save-event-id-2",
			Type:             "EventType",
			AggregateID:      "save-aggregate-id",
			AggregateType:    "AggregateType",
			AggregateVersion: 1,
			Payload:          &EventType{ID: "save-event-id-2"},
			Created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
		},
		{
			ID:               "save-event-id-3",
			Type:             "EventType",
			AggregateID:      "save-another-aggregate-id",
			AggregateType:    "AggregateType",
			AggregateVersion: 0,
			Payload:          &EventType{ID: "save-event-id-3"},
			Created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
		},
	}

	err := s.Driver.Save(events)
	s.Nil(err)

	out, err := s.Client.Scan(&dynamodb.ScanInput{
		TableName: aws.String(s.TableName),
	})
	s.Nil(err)

	s.Contains(out.Items, map[string]*dynamodb.AttributeValue{
		"ID":               {S: aws.String("save-event-id-1")},
		"Type":             {S: aws.String("EventType")},
		"AggregateID":      {S: aws.String("save-aggregate-id")},
		"AggregateType":    {S: aws.String("AggregateType")},
		"AggregateVersion": {N: aws.String("0")},
		"Payload":          {S: aws.String(`{"ID":"save-event-id-1"}`)},
		"Created":          {S: aws.String("2019-06-30T00:00:00Z")},
	})

	s.Contains(out.Items, map[string]*dynamodb.AttributeValue{
		"ID":               {S: aws.String("save-event-id-2")},
		"Type":             {S: aws.String("EventType")},
		"AggregateID":      {S: aws.String("save-aggregate-id")},
		"AggregateType":    {S: aws.String("AggregateType")},
		"AggregateVersion": {N: aws.String("1")},
		"Payload":          {S: aws.String(`{"ID":"save-event-id-2"}`)},
		"Created":          {S: aws.String("2019-06-30T00:00:00Z")},
	})

	s.Contains(out.Items, map[string]*dynamodb.AttributeValue{
		"ID":               {S: aws.String("save-event-id-3")},
		"Type":             {S: aws.String("EventType")},
		"AggregateID":      {S: aws.String("save-another-aggregate-id")},
		"AggregateType":    {S: aws.String("AggregateType")},
		"AggregateVersion": {N: aws.String("0")},
		"Payload":          {S: aws.String(`{"ID":"save-event-id-3"}`)},
		"Created":          {S: aws.String("2019-06-30T00:00:00Z")},
	})
}

func (s *DynamoDriverSuite) TestSaveOptimisticLocking() {
	events := []*es.Event{
		{
			ID:               "lock-event-id-1",
			Type:             "EventType",
			AggregateID:      "lock-aggregate-id",
			AggregateType:    "AggregateType",
			AggregateVersion: 0,
			Payload:          &EventType{ID: "lock-event-id-1"},
			Created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
		},
	}

	err := s.Driver.Save(events)
	s.Nil(err)

	events = []*es.Event{
		{
			ID:               "lock-event-id-2",
			Type:             "EventType",
			AggregateID:      "lock-aggregate-id",
			AggregateType:    "AggregateType",
			AggregateVersion: 0,
			Payload:          &EventType{ID: "lock-event-id-2"},
			Created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
		},
	}

	err = s.Driver.Save(events)
	s.Error(err)
}

func (s *DynamoDriverSuite) TestSaveInTransaction() {
	events := []*es.Event{
		{
			ID:               "tx-event-id-1",
			Type:             "EventType",
			AggregateID:      "tx-aggregate-id",
			AggregateType:    "AggregateType",
			AggregateVersion: 0,
			Payload:          &EventType{ID: "tx-event-id-1"},
			Created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
		},
		{
			ID:               "tx-event-id-2",
			Type:             "EventType",
			AggregateID:      "tx-aggregate-id",
			AggregateType:    "AggregateType",
			AggregateVersion: 0,
			Payload:          &EventType{ID: "tx-event-id-2"},
			Created:          time.Date(2019, 6, 30, 0, 0, 0, 0, time.UTC),
		},
	}

	err := s.Driver.Save(events)
	s.Error(err)

	out, err := s.Client.GetItem(&dynamodb.GetItemInput{
		TableName:      aws.String(s.TableName),
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			"AggregateID": &dynamodb.AttributeValue{
				S: aws.String("tx-aggregate-id"),
			},
			"AggregateVersion": &dynamodb.AttributeValue{
				N: aws.String("0"),
			},
		},
	})
	s.Nil(err)
	s.Empty(out)
}

func (s *DynamoDriverSuite) TestSaveEmptyEvents() {
	err := s.Driver.Save([]*es.Event{})
	s.Nil(err)
}
