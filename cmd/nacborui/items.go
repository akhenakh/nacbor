package main

import (
	"fmt"
	"time"
)

type state int

const (
	stateMainMenu state = iota
	stateBucketList
	stateStreamList
	stateKVKeyList
	stateStreamMsgList
	stateDetailView
	stateSubjectInput
	stateWatching
)

type menuItem struct {
	title string
	desc  string
}

func (m menuItem) Title() string       { return m.title }
func (m menuItem) Description() string { return m.desc }
func (m menuItem) FilterValue() string { return m.title }

type kvKeyItem struct {
	bucket string
	key    string
}

func (k kvKeyItem) Title() string       { return k.key }
func (k kvKeyItem) Description() string { return fmt.Sprintf("Bucket: %s", k.bucket) }
func (k kvKeyItem) FilterValue() string { return k.key }

type streamMsgItem struct {
	stream  string
	seq     uint64
	subject string
	data    []byte
	time    time.Time
}

func (s streamMsgItem) Title() string       { return fmt.Sprintf("[%d] %s", s.seq, s.subject) }
func (s streamMsgItem) Description() string { return s.time.Format(time.RFC3339) }
func (s streamMsgItem) FilterValue() string { return s.subject }
