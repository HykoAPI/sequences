package sequences

import (
	"gorm.io/gorm"
	"time"
)

type Sequence struct {
	Stages *Stage `json:"stages"`
}

type Stage struct {
	EventName    string                                `json:"event_name"`
	ConsumerFunc func(db *gorm.DB, input []byte) (Status, string, *time.Time) `json:"-"`
	NextStage    *Stage                                `json:"-"`
}
