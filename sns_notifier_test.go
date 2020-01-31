package es_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/indebted-modules/es"
	"github.com/stretchr/testify/suite"
)

type SNSNotifierSuite struct {
	suite.Suite
	snsSvc   *sns.SNS
	sqsSvc   *sqs.SQS
	topicArn *string
	queueUrl *string
}

func TestSNSNotifierSuite(t *testing.T) {
	suite.Run(t, new(SNSNotifierSuite))
}

func (s *SNSNotifierSuite) SetupTest() {
	snsEndpoint := "http://localstack:4575"
	sqsEndpoint := "http://localstack:4576"
	s.Eventually(func() bool {
		_, err1 := http.Get(snsEndpoint)
		_, err2 := http.Get(sqsEndpoint)
		return err1 == nil && err2 == nil
	}, 10*time.Second, time.Second, "Localstack services are not ready or running")

	sess := session.Must(session.NewSession())
	cred := credentials.NewStaticCredentials("id", "secret", "token")
	s.snsSvc = sns.New(sess, aws.NewConfig().WithRegion("ap-southeast-2").WithCredentials(cred).WithEndpoint(snsEndpoint))
	topicResponse, err := s.snsSvc.CreateTopic(&sns.CreateTopicInput{Name: aws.String("test-topic")})
	s.NoError(err)
	s.topicArn = topicResponse.TopicArn

	s.sqsSvc = sqs.New(sess, aws.NewConfig().WithRegion("ap-southeast-2").WithCredentials(cred).WithEndpoint(sqsEndpoint))
	queueResponse, err := s.sqsSvc.CreateQueue(&sqs.CreateQueueInput{QueueName: aws.String("test-queue")})
	s.NoError(err)
	s.queueUrl = queueResponse.QueueUrl

	_, err = s.snsSvc.Subscribe(&sns.SubscribeInput{
		Protocol: aws.String("sqs"),
		Endpoint: queueResponse.QueueUrl,
		TopicArn: topicResponse.TopicArn,
	})
	s.NoError(err)
}

func (s *SNSNotifierSuite) TearDownTest() {
	_, err := s.sqsSvc.DeleteQueue(&sqs.DeleteQueueInput{QueueUrl: s.queueUrl})
	s.NoError(err)
	_, err = s.snsSvc.DeleteTopic(&sns.DeleteTopicInput{TopicArn: s.topicArn})
	s.NoError(err)
}

func (s *SNSNotifierSuite) TestPublish() {
	notifier := es.NewSNSNotifier(s.snsSvc, *s.topicArn)
	err := notifier.Publish(struct{ Content string }{Content: "some message"})
	s.NoError(err)

	response, err := s.sqsSvc.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            s.queueUrl,
		MaxNumberOfMessages: aws.Int64(1),
		WaitTimeSeconds:     aws.Int64(1),
	})
	s.NoError(err)
	s.Equal(1, len(response.Messages))

	body := &struct{ Message string }{}
	err = json.Unmarshal([]byte(*response.Messages[0].Body), body)
	s.NoError(err)
	s.Equal(`{"Content":"some message"}`, body.Message)
}
