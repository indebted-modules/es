package es

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/indebted-modules/cfg"
)

// NewDynamoDriver creates a new DynamoDriver
func NewDynamoDriver(tableName string) *DynamoDriver {
	return &DynamoDriver{
		Client:    dynamodb.New(cfg.Sess),
		TableName: tableName,
	}
}

// DynamoDriver implementation for deployed environments
type DynamoDriver struct {
	Client    *dynamodb.DynamoDB
	TableName string
}

// Load all events by aggregate ID
func (s *DynamoDriver) Load(aggregateID string) ([]*Event, error) {
	out, err := s.Client.Query(&dynamodb.QueryInput{
		TableName:              aws.String(s.TableName),
		ConsistentRead:         aws.Bool(true),
		KeyConditionExpression: aws.String("AggregateID = :key"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":key": {S: aws.String(aggregateID)},
		},
	})
	if err != nil {
		return nil, err
	}

	events := []*Event{}
	for _, item := range out.Items {

		aggregateVersion, err := strconv.ParseInt(aws.StringValue(item["AggregateVersion"].N), 10, 64)
		if err != nil {
			return nil, err
		}

		payload, err := resolveType(aws.StringValue(item["Type"].S))
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal([]byte(aws.StringValue(item["Payload"].S)), payload)
		if err != nil {
			return nil, err
		}

		var created time.Time
		s := fmt.Sprintf(`"%s"`, aws.StringValue(item["Created"].S))
		err = json.Unmarshal([]byte(s), &created)
		if err != nil {
			return nil, err
		}

		events = append(events, &Event{
			ID:               aws.StringValue(item["ID"].S),
			Type:             aws.StringValue(item["Type"].S),
			AggregateID:      aws.StringValue(item["AggregateID"].S),
			AggregateType:    aws.StringValue(item["AggregateType"].S),
			AggregateVersion: aggregateVersion,
			Payload:          payload,
			Created:          created,
		})
	}

	return events, nil
}

// Save all events into DynamoDB
func (s *DynamoDriver) Save(events []*Event) error {
	if len(events) == 0 {
		return nil
	}

	items := []*dynamodb.TransactWriteItem{}
	for _, event := range events {

		b, err := json.Marshal(event.Payload)
		if err != nil {
			return err
		}
		payload := string(b)

		b, err = json.Marshal(event.Created)
		if err != nil {
			return err
		}
		created := strings.Trim(string(b), `"`)

		items = append(items, &dynamodb.TransactWriteItem{
			Put: &dynamodb.Put{
				TableName:           aws.String(s.TableName),
				ConditionExpression: aws.String("attribute_not_exists(AggregateVersion)"),
				Item: map[string]*dynamodb.AttributeValue{
					"ID":               {S: aws.String(event.ID)},
					"Type":             {S: aws.String(event.Type)},
					"AggregateID":      {S: aws.String(event.AggregateID)},
					"AggregateType":    {S: aws.String(event.AggregateType)},
					"AggregateVersion": {N: aws.String(strconv.FormatInt(event.AggregateVersion, 10))},
					"Payload":          {S: aws.String(payload)},
					"Created":          {S: aws.String(created)},
				},
			},
		})
	}

	_, err := s.Client.TransactWriteItems(&dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	})
	if err != nil {
		return err
	}

	return nil
}
