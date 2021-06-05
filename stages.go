package sequences

import (
	"gorm.io/gorm"
)

type Sequence struct {
	Stages *Stage `json:"stages"`
}

type Stage struct {
	EventName    string                                `json:"event_name"`
	ConsumerFunc func(db *gorm.DB, input []byte) (Status, string) `json:"-"`
	NextStage    *Stage                                `json:"-"`
}
