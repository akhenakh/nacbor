package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"charm.land/bubbles/v2/list"
	"charm.land/bubbles/v2/textinput"
	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

type appModel struct {
	nc *nats.Conn
	js jetstream.JetStream

	state        state
	mainList     list.Model
	browseList   list.Model
	itemList     list.Model
	vp           viewport.Model
	subjectInput textinput.Model

	currentBucket string
	currentStream string

	detailTitle string
	detailType  string

	// detailItems is the list the current detail view was opened from, used
	// for prev/next navigation. detailIndex is the index into that list.
	detailItems []list.Item
	detailIndex int

	logCh       chan string
	logs        []string
	watchCancel context.CancelFunc

	width, height int
}

func (m appModel) Init() tea.Cmd {
	return nil
}

func (m appModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.mainList.SetSize(msg.Width, msg.Height)
		m.browseList.SetSize(msg.Width, msg.Height)
		m.itemList.SetSize(msg.Width, msg.Height)

		m.vp.SetWidth(msg.Width)
		m.vp.SetHeight(msg.Height - 3)
		return m, nil

	case bucketsLoadedMsg:
		var items []list.Item
		for _, b := range msg {
			items = append(items, menuItem{title: b, desc: "KV Bucket"})
		}
		m.browseList.SetItems(items)
		m.browseList.Title = "KV Buckets"
		m.state = stateBucketList
		return m, nil

	case streamsLoadedMsg:
		var items []list.Item
		for _, s := range msg {
			items = append(items, menuItem{title: s, desc: "JetStream Stream"})
		}
		m.browseList.SetItems(items)
		m.browseList.Title = "Streams"
		m.state = stateStreamList
		return m, nil

	case kvKeysLoadedMsg:
		m.itemList.SetItems(msg.items)
		if msg.total > len(msg.items) {
			m.itemList.Title = fmt.Sprintf("Keys in %s (showing %d of %d)", msg.bucket, len(msg.items), msg.total)
		} else {
			m.itemList.Title = fmt.Sprintf("Keys in %s (%d)", msg.bucket, msg.total)
		}
		m.state = stateKVKeyList
		return m, nil

	case streamMsgsLoadedMsg:
		m.itemList.SetItems(msg.items)
		m.itemList.Title = fmt.Sprintf("Latest events in %s [r: refresh]", msg.stream)
		m.state = stateStreamMsgList
		return m, nil

	case detailLoadedMsg:
		m.detailTitle = msg.title
		m.detailType = msg.payloadType
		m.vp.SetContent(msg.content)
		m.vp.GotoTop()
		m.state = stateDetailView
		return m, nil

	case errorMsg:
		m.detailTitle = "Error"
		m.detailType = "Error"
		m.vp.SetContent(lipgloss.NewStyle().Foreground(lipgloss.Color("9")).Render(msg.Error()))
		m.vp.GotoTop()
		m.state = stateDetailView
		return m, nil

	case logMsg:
		m.logs = append(m.logs, string(msg))
		m.vp.SetContent(strings.Join(m.logs, "\n"))
		m.vp.GotoBottom()
		return m, waitForMessage(m.logCh)

	case tea.KeyPressMsg:
		switch msg.String() {
		case "ctrl+c":
			return m, tea.Quit
		case "q", "esc":
			switch m.state {
			case stateMainMenu:
				return m, tea.Quit
			case stateSubjectInput:
				m.subjectInput.Blur()
				m.subjectInput.Reset()
				m.state = stateMainMenu
				return m, nil
			case stateWatching:
				m.stopWatch()
				m.state = stateMainMenu
				return m, nil
			case stateBucketList, stateStreamList:
				m.state = stateMainMenu
			case stateKVKeyList:
				m.state = stateBucketList
			case stateStreamMsgList:
				m.state = stateStreamList
			case stateDetailView:
				if m.currentBucket != "" {
					m.state = stateKVKeyList
				} else if m.currentStream != "" {
					m.state = stateStreamMsgList
				} else {
					m.state = stateMainMenu
				}
			}
			return m, nil

		case "r":
			if m.state == stateStreamMsgList && m.currentStream != "" && m.itemList.FilterState() != list.Filtering {
				return m, fetchStreamMsgs(m.js, m.currentStream)
			}

		case "left", "right":
			if m.state != stateDetailView || len(m.detailItems) == 0 {
				return m, nil
			}
			delta := -1
			if msg.String() == "right" {
				delta = 1
			}
			newIdx := m.detailIndex + delta
			if newIdx < 0 || newIdx >= len(m.detailItems) {
				return m, nil
			}
			m.detailIndex = newIdx
			m.itemList.Select(newIdx)

			if m.currentBucket != "" {
				if it, ok := m.detailItems[newIdx].(kvKeyItem); ok {
					return m, fetchKVValue(m.js, it.bucket, it.key)
				}
			} else if m.currentStream != "" {
				if it, ok := m.detailItems[newIdx].(streamMsgItem); ok {
					ptype, content := formatPayload(it.data)
					m.detailTitle = fmt.Sprintf("%s > %s [Seq: %d]", it.stream, it.subject, it.seq)
					m.detailType = ptype
					m.vp.SetContent(content)
					m.vp.GotoTop()
					return m, nil
				}
			}
			return m, nil

		case "enter":
			switch m.state {
			case stateMainMenu:
				if i, ok := m.mainList.SelectedItem().(menuItem); ok {
					switch i.title {
					case "KV Buckets":
						return m, fetchBuckets(m.js)
					case "JetStream Streams":
						return m, fetchStreams(m.js)
					case "NATS Core":
						m.subjectInput = newSubjectInput()
						m.subjectInput.Focus()
						m.state = stateSubjectInput
						return m, textinput.Blink
					}
				}
			case stateSubjectInput:
				subject := m.subjectInput.Value()
				if subject == "" {
					subject = defaultSubject
				}
				m.subjectInput.Blur()
				m.startWatchNATS(subject)
				m.state = stateWatching
				return m, waitForMessage(m.logCh)
			case stateBucketList:
				if i, ok := m.browseList.SelectedItem().(menuItem); ok {
					m.currentBucket = i.title
					m.currentStream = ""
					return m, fetchKVKeys(m.js, i.title)
				}
			case stateStreamList:
				if i, ok := m.browseList.SelectedItem().(menuItem); ok {
					m.currentStream = i.title
					m.currentBucket = ""
					return m, fetchStreamMsgs(m.js, i.title)
				}
			case stateKVKeyList:
				visible := m.itemList.VisibleItems()
				idx := m.itemList.Index()
				if idx < 0 || idx >= len(visible) {
					return m, nil
				}
				it, ok := visible[idx].(kvKeyItem)
				if !ok {
					return m, nil
				}
				m.detailItems = visible
				m.detailIndex = idx
				m.itemList.Select(idx)
				return m, fetchKVValue(m.js, it.bucket, it.key)
			case stateStreamMsgList:
				visible := m.itemList.VisibleItems()
				idx := m.itemList.Index()
				if idx < 0 || idx >= len(visible) {
					return m, nil
				}
				it, ok := visible[idx].(streamMsgItem)
				if !ok {
					return m, nil
				}
				m.detailItems = visible
				m.detailIndex = idx
				m.itemList.Select(idx)
				ptype, content := formatPayload(it.data)
				m.detailTitle = fmt.Sprintf("%s > %s [Seq: %d]", it.stream, it.subject, it.seq)
				m.detailType = ptype
				m.vp.SetContent(content)
				m.vp.GotoTop()
				m.state = stateDetailView
				return m, nil
			}
		}
	}

	if m.state == stateSubjectInput {
		m.subjectInput, cmd = m.subjectInput.Update(msg)
		cmds = append(cmds, cmd)
		return m, tea.Batch(cmds...)
	}

	switch m.state {
	case stateMainMenu:
		m.mainList, cmd = m.mainList.Update(msg)
		cmds = append(cmds, cmd)
	case stateBucketList, stateStreamList:
		m.browseList, cmd = m.browseList.Update(msg)
		cmds = append(cmds, cmd)
	case stateKVKeyList, stateStreamMsgList:
		m.itemList, cmd = m.itemList.Update(msg)
		cmds = append(cmds, cmd)
	case stateWatching, stateDetailView:
		m.vp, cmd = m.vp.Update(msg)
		cmds = append(cmds, cmd)
	}

	return m, tea.Batch(cmds...)
}

func (m appModel) View() tea.View {
	var content string

	switch m.state {
	case stateMainMenu:
		content = m.mainList.View()
	case stateSubjectInput:
		content = m.subjectInputView()
	case stateBucketList, stateStreamList:
		content = m.browseList.View()
	case stateKVKeyList, stateStreamMsgList:
		content = m.itemList.View()
	case stateDetailView:
		header := lipgloss.NewStyle().Bold(true).Render(fmt.Sprintf(" %s ", m.detailTitle))
		badgeColor := lipgloss.Color("62")
		switch m.detailType {
		case "CBOR":
			badgeColor = lipgloss.Color("99")
		case "JSON":
			badgeColor = lipgloss.Color("208")
		case "Raw":
			badgeColor = lipgloss.Color("240")
		}

		badge := lipgloss.NewStyle().
			Bold(true).
			Background(badgeColor).
			Foreground(lipgloss.Color("230")).
			Padding(0, 1).
			Render(m.detailType)

		nav := "[esc: back]"
		if len(m.detailItems) > 0 {
			nav = fmt.Sprintf("[%d/%d  ←prev  next→]", m.detailIndex+1, len(m.detailItems))
		}
		infoBar := lipgloss.JoinHorizontal(lipgloss.Left, header, " ", badge, "  ", nav)
		content = lipgloss.JoinVertical(lipgloss.Left, infoBar, "", m.vp.View())

	case stateWatching:
		header := lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("205")).
			Render(" Watching NATS | Press 'esc' to go back")
		content = lipgloss.JoinVertical(lipgloss.Left, header, "", m.vp.View())
	}

	v := tea.NewView(content)
	v.AltScreen = true
	return v
}
