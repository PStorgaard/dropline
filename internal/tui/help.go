package tui

import (
	"charm.land/bubbles/v2/key"
)

// keyMap declares every TUI keybinding once so both the footer hint
// strip and the `?` overlay (via the help.KeyMap interface) read from
// a single source. The bindings here mirror what model.Update actually
// matches against — switch cases in Update still use msg.String() rather
// than key.Matches because the rest of the file predates this map.
type keyMap struct {
	Pause  key.Binding
	Reset  key.Binding
	Save   key.Binding
	Copy   key.Binding
	Help   key.Binding
	Quit   key.Binding
	Cancel key.Binding
}

func newKeyMap() keyMap {
	return keyMap{
		Pause: key.NewBinding(
			key.WithKeys("p"),
			key.WithHelp("p", "pause / resume"),
		),
		Reset: key.NewBinding(
			key.WithKeys("r"),
			key.WithHelp("r", "reset history"),
		),
		Save: key.NewBinding(
			key.WithKeys("s"),
			key.WithHelp("s", "save JSON"),
		),
		Copy: key.NewBinding(
			key.WithKeys("c"),
			key.WithHelp("c", "copy JSON"),
		),
		Help: key.NewBinding(
			key.WithKeys("?"),
			key.WithHelp("?", "toggle help"),
		),
		Quit: key.NewBinding(
			key.WithKeys("q", "ctrl+c"),
			key.WithHelp("q", "quit"),
		),
		Cancel: key.NewBinding(
			key.WithKeys("esc"),
			key.WithHelp("esc", "close help / quit"),
		),
	}
}

// ShortHelp satisfies help.KeyMap; unused (we always render FullHelp on
// the `?` overlay) but kept for completeness.
func (k keyMap) ShortHelp() []key.Binding {
	return []key.Binding{k.Pause, k.Reset, k.Save, k.Copy, k.Help, k.Quit}
}

// FullHelp returns the two-column layout shown by the `?` overlay. Left
// column is the per-test live controls; right column is the
// chrome-and-output controls.
func (k keyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{
		{k.Pause, k.Reset},
		{k.Save, k.Copy},
		{k.Help, k.Quit, k.Cancel},
	}
}
