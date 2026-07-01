package main

import (
	"charm.land/bubbles/v2/textinput"
	"charm.land/lipgloss/v2"
)

const defaultSubject = ">"

// newSubjectInput builds a focused textinput model for entering a NATS subject.
func newSubjectInput() textinput.Model {
	ti := textinput.New()
	ti.Prompt = "> "
	ti.Placeholder = defaultSubject
	ti.CharLimit = 256
	ti.SetWidth(40)
	ti.Focus()
	return ti
}

// subjectInputView renders a centered popup containing the subject text input.
func (m appModel) subjectInputView() string {
	title := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("99")).
		Render("NATS Core Subject")

	hint := lipgloss.NewStyle().
		Foreground(lipgloss.Color("240")).
		Render("enter to subscribe • esc to cancel")

	body := lipgloss.JoinVertical(lipgloss.Left,
		title,
		"",
		m.subjectInput.View(),
		"",
		hint,
	)

	box := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("99")).
		Padding(1, 2).
		Render(body)

	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, box)
}
