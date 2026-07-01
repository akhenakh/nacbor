package main

import "charm.land/bubbles/v2/list"

type bucketsLoadedMsg []string

type streamsLoadedMsg []string

type kvKeysLoadedMsg struct {
	bucket string
	items  []list.Item
	total  int // total live keys in the bucket (may exceed len(items) when truncated)
}

type streamMsgsLoadedMsg struct {
	stream string
	items  []list.Item
}

type detailLoadedMsg struct {
	title       string
	payloadType string
	content     string
}

type logMsg string

type errorMsg error
