package sequences

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/adjust/rmq/v3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
	"github.com/go-redis/redis/v7"
	"os"
	"time"
)

// Construct tree of stages
// Emit first event
// Consume first event
// Call matching consumer
func SetupConsumersForSequence(db *gorm.DB, redisURL string, taskQueueName string, numberOfConsumersForSequence int, sequence Sequence, storeFunc StoreFunc, readFunc ReadFunc) (*rmq.Queue, error) {
	// TODO: Error channel
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(opt)

	errChannel := make(chan error)
	go func(errChannel chan error) {
		fmt.Println("HELLO ERROR CHANNEL")
		for err := range errChannel {
			fmt.Println(err)
		}
		fmt.Println("GOODBYE ERROR CHANNEL")
	}(errChannel)

	errChannel<-errors.New("test error")

	connection, err := rmq.OpenConnectionWithRedisClient("", client, errChannel)
	if err != nil {
		return nil, err
	}

	go func() {
		defer func() {
			fmt.Println("LEAVING STATS")
		}()
		fmt.Println("HIYA")
		for {
			queues, err := connection.GetOpenQueues()
			if err != nil {
				fmt.Println("error", err)
				return
			}
			fmt.Println("QUEUES")
			fmt.Println(queues)
			stats, err := connection.CollectStats(queues)
			if err != nil {
				fmt.Println("ERROR GETTING STATS")
				fmt.Println("error", err)
				return
			}

			fmt.Println("Stats")
			fmt.Println(stats.String())
			time.Sleep(time.Second*5)
		}
	}()

	taskQueue, err := connection.OpenQueue(taskQueueName)
	if err != nil {
		return nil, err
	}

	err = taskQueue.StartConsuming(10, time.Second)
	if err != nil {
		return nil, err
	}

	for i := 1; i <= numberOfConsumersForSequence; i++ {
		taskConsumer := &Consumer{
			db:        db,
			sequence:  sequence,
			taskQueue: taskQueue,
			storeFunc: storeFunc,
			readFunc: readFunc,
		}
		_, err = taskQueue.AddConsumer(fmt.Sprintf("task-consumer-%d", i), taskConsumer)
		if err != nil {
			return nil, err
		}

	}

	return &taskQueue, nil
}

type Event struct {
	EventType  string `json:"event_type"`
	SequenceID uint   `json:"sequence_id"`
	Payload    []byte `json:"input"`
	WaitUntil *time.Time `json:"wait_until"`
}

type StoreFunc func(db *gorm.DB, ID uint, stage string, status Status, errorMessage string) error
// TODO: Move to struct
type ReadFunc func(db *gorm.DB, ID uint, stage string) (bool, string, string, error)

type Consumer struct {
	db        *gorm.DB
	sequence  Sequence
	taskQueue rmq.Queue
	storeFunc StoreFunc
	readFunc  ReadFunc
	logger zerolog.Logger
}

func NewConsumer(db *gorm.DB, sequence Sequence, queue rmq.Queue, store StoreFunc, read ReadFunc) Consumer {
	return Consumer{
		db:        db,
		sequence:  sequence,
		taskQueue: queue,
		storeFunc: store,
		readFunc: read,
		logger: zerolog.New(os.Stdout),
	}
}

func (consumer *Consumer) SetLogger(logger zerolog.Logger) {
	consumer.logger = logger
}

func (consumer *Consumer) Consume(delivery rmq.Delivery) {
	fmt.Println("IN CONSUME")
	var event Event
	if err := json.Unmarshal([]byte(delivery.Payload()), &event); err != nil {
		// handle json error
		if err := delivery.Reject(); err != nil {
			// handle reject error
			log.Error().Err(err).Msg("error rejecting message")
		}
		log.Error().Err(err).Msg("error unmarshalling JSON payload")
		return
	}

	fmt.Println("CONSUMING EVENT", event.EventType)
	
	if event.WaitUntil != nil {
		if time.Now().UTC().Before(*event.WaitUntil) {
			consumer.republishEvent(delivery, event)
			return
		}
	}

	currentStage, err := consumer.findMatchingStage(event, delivery)
	if err != nil {
		fmt.Println(err)
		return
	}

	if err := consumer.processEvent(consumer.db, currentStage, event, delivery); err != nil {
		fmt.Println(err)
		return
	}
}

func (consumer *Consumer) emitNextEvent(currentStage *Stage, sequenceID uint, payload []byte, waitUntil *time.Time) error {
	if currentStage.NextStage == nil {
		return nil
	}

	nextTask := Event{
		SequenceID: sequenceID,
		EventType:  currentStage.NextStage.EventName,
		Payload:    payload,
		WaitUntil: waitUntil,
	}

	fmt.Println("EMITTING EVNET")
	fmt.Println(nextTask.EventType)

	taskBytes, err := json.Marshal(nextTask)
	if err != nil {
		// handle error
		return err
	}

	fmt.Println(string(taskBytes))

	err = consumer.taskQueue.PublishBytes(taskBytes)
	if err != nil {
		// handle error
		return err
	}
	return nil
}


func (consumer *Consumer) processEvent(db *gorm.DB, currentStage *Stage, event Event, delivery rmq.Delivery) error {
	// Handle panics
	defer func() {
		fmt.Println("IN THE DEFER")
		if err := recover(); err != nil {
			log.Error().Str("panic", fmt.Sprintf("%v", err))
			err := consumer.storeFunc(db, event.SequenceID, currentStage.EventName, ERROR, fmt.Sprintf("%v", err))
			if err != nil {
				log.Debug().Err(err)
			}

			return
		}
	}()
	fmt.Println("PROCESSING EVENT", event.EventType)

	// Read stage
	exists, existingStatus, _, err := consumer.readFunc(db ,event.SequenceID, event.EventType)
	if err != nil {
		// Not acking or rejecting because we want to retry this message
		return err
	}

	// If already run successfully and we're not set up to retry then emit next event
	if exists && Status(existingStatus) == SUCCESS {
		if err := consumer.emitNextEvent(currentStage, event.SequenceID, event.Payload, nil); err != nil {
			return err
		}
		return nil
	}

	status, description, waitUntil := currentStage.ConsumerFunc(db, event.Payload)
	if status == ERROR {
		err := consumer.storeFunc(db, event.SequenceID, currentStage.EventName, ERROR, description)
		if err != nil {
			log.Debug().Err(err)
			// Still reject msg on error
		}
		fmt.Println("ATTEMPTING TO REJECT")
		if err := delivery.Reject(); err != nil {
			fmt.Println("ERROR REJECTING MESSAGE")
			return err
		}
		return err
	}

	err = consumer.storeFunc(db, event.SequenceID, currentStage.EventName, status, description)
	if err != nil {
		return err
	}
	fmt.Println("ATTEMPTING TO ACK")
	if err := delivery.Ack(); err != nil {
		fmt.Println("FAILED TO ACK")
		return err
	}
	fmt.Println("ACKED SUCCESSFULLY")

	if status == RETRY {
		fmt.Println("REQUEUING SAME EVENT")
		// Requeue event
		// Emit same event
		event.WaitUntil = waitUntil
		consumer.republishEvent(delivery, event)
		return nil
	} else {
		fmt.Println("ATTEMPTING TO EMIT NEXT EVENT")
		fmt.Println("CURRENT STAGE", currentStage.EventName)
		fmt.Println("NEXT STAGE", currentStage.NextStage)
		if currentStage.NextStage != nil {
			fmt.Println("NEXT STAGE NAME", currentStage.NextStage.EventName)
		}
		if err := consumer.emitNextEvent(currentStage, event.SequenceID, event.Payload, nil); err != nil {
			fmt.Println("FAILED TO EMIT NEXT EVENT")
			return err
		}
	}

	return nil
}

func (consumer *Consumer) findMatchingStage(task Event, delivery rmq.Delivery) (*Stage, error) {
	currentStage := consumer.sequence.Stages
	for {
		fmt.Println("LOOPING BABY")
		if currentStage.EventName == task.EventType {
			// Found Stage return it
			return currentStage, nil
		}

		// If there if isn't next stage reject as can't find
		// appropriate stage
		if currentStage.NextStage == nil {
			if err := delivery.Reject(); err != nil {
				return nil, err
			}
			return nil, errors.New("no stage found")
		}

		currentStage = currentStage.NextStage
	}
}

func (consumer *Consumer) republishEvent(delivery rmq.Delivery, event Event) {
	log.Trace().Str("event_type", event.EventType).Msg("republishing event")
	taskBytes, err := json.Marshal(event)
	if err != nil {
		// DO NOT REJECT JUST WAIT UNTIL WE REPROCESS IT
		log.Debug().Err(err)
	}

	err = consumer.taskQueue.PublishBytes(taskBytes)
	if err != nil {
		// handle error
		log.Debug().Err(err)
	}

	// Then ack old one so that if we error on ack we'll at least reprocess both rather than neither
	if err := delivery.Ack(); err != nil {
		log.Debug().Err(err)
		return
	}
	return
}

type Status string
const (
	ERROR Status = "ERROR"
	SUCCESS Status = "SUCCESS"
	SKIPPING Status = "SKIPPING"
	RETRY Status = "RETRY"
)