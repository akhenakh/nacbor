package main

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"charm.land/bubbles/v2/list"
	tea "charm.land/bubbletea/v2"
)

// maxListItems caps the number of entries shown in the KV and stream lists.
const maxListItems = 500

func fetchBuckets(js jetstream.JetStream) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var res []string
		iter := js.KeyValueStoreNames(ctx)
		for name := range iter.Name() {
			res = append(res, name)
		}
		if err := iter.Error(); err != nil {
			return errorMsg(err)
		}
		return bucketsLoadedMsg(res)
	}
}

func fetchStreams(js jetstream.JetStream) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var res []string
		iter := js.StreamNames(ctx)
		for name := range iter.Name() {
			res = append(res, name)
		}
		if err := iter.Err(); err != nil {
			return errorMsg(err)
		}
		return streamsLoadedMsg(res)
	}
}

func fetchKVKeys(js jetstream.JetStream, bucket string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		kv, err := js.KeyValue(ctx, bucket)
		if err != nil {
			return errorMsg(err)
		}

		keys, err := kv.ListKeys(ctx)
		if err != nil {
			if err == jetstream.ErrNoKeysFound {
				return kvKeysLoadedMsg{bucket: bucket, items: nil}
			}
			return errorMsg(err)
		}
		defer keys.Stop()

		var all []string
		for key := range keys.Keys() {
			all = append(all, key)
		}
		total := len(all)

		// ListKeys emits live keys in ascending stream-sequence order, so the
		// most recently written keys are at the end. Reverse to surface the
		// newest first and cap the displayed list at maxListItems.
		for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
			all[i], all[j] = all[j], all[i]
		}
		if len(all) > maxListItems {
			all = all[:maxListItems]
		}

		items := make([]list.Item, len(all))
		for i, key := range all {
			items[i] = kvKeyItem{bucket: bucket, key: key}
		}
		return kvKeysLoadedMsg{bucket: bucket, items: items, total: total}
	}
}

func fetchKVValue(js jetstream.JetStream, bucket, key string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		kv, err := js.KeyValue(ctx, bucket)
		if err != nil {
			return errorMsg(err)
		}

		entry, err := kv.Get(ctx, key)
		if err != nil {
			return errorMsg(err)
		}

		payloadType, content := formatPayload(entry.Value())
		return detailLoadedMsg{
			title:       fmt.Sprintf("%s > %s (Rev: %d)", bucket, key, entry.Revision()),
			payloadType: payloadType,
			content:     content,
		}
	}
}

func fetchStreamMsgs(js jetstream.JetStream, stream string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		str, err := js.Stream(ctx, stream)
		if err != nil {
			return errorMsg(err)
		}

		info, err := str.Info(ctx)
		if err != nil {
			return errorMsg(err)
		}

		var items []list.Item
		last := info.State.LastSeq
		first := info.State.FirstSeq

		if last == 0 && first == 0 {
			return streamMsgsLoadedMsg{stream: stream, items: nil}
		}

		// Fetch up to the last maxListItems messages.
		limit := uint64(maxListItems)
		start := uint64(1)
		if last >= limit {
			start = last - limit + 1
		}
		if start < first {
			start = first
		}

		for i := start; i <= last; i++ {
			msg, err := str.GetMsg(ctx, i)
			if err == nil {
				items = append(items, streamMsgItem{
					stream:  stream,
					seq:     msg.Sequence,
					subject: msg.Subject,
					data:    msg.Data,
					time:    msg.Time,
				})
			}
		}

		for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
			items[i], items[j] = items[j], items[i]
		}

		return streamMsgsLoadedMsg{stream: stream, items: items}
	}
}

func waitForMessage(ch <-chan string) tea.Cmd {
	return func() tea.Msg {
		msg, ok := <-ch
		if !ok {
			return nil
		}
		return logMsg(msg)
	}
}

func (m *appModel) stopWatch() {
	if m.watchCancel != nil {
		m.watchCancel()
		m.watchCancel = nil
	}
	m.logs = nil
	m.vp.SetContent("")
}

func (m *appModel) startWatchNATS(subject string) {
	ctx, cancel := context.WithCancel(context.Background())
	m.watchCancel = cancel
	m.logs = []string{fmt.Sprintf("Watching NATS Subject: %s", subject)}
	m.vp.SetContent(m.logs[0])

	go func() {
		sub, err := m.nc.Subscribe(subject, func(msg *nats.Msg) {
			_, content := formatPayload(msg.Data)
			m.logCh <- fmt.Sprintf("[%s]\n%s\n", msg.Subject, content)
		})
		if err != nil {
			m.logCh <- fmt.Sprintf("Error subscribing: %v", err)
			return
		}
		defer sub.Unsubscribe()
		<-ctx.Done()
	}()
}
