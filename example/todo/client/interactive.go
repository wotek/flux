package client

import (
	"context"
	"io"
	"os"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/wotek/flux"
)

// RunInteractive launches an interactive Bubble Tea terminal user interface.
// It displays the available todo lists catalog, allowing keyboard cycling (arrow keys / j / k),
// creating new lists, and navigating into lists to view, add, delete, and complete tasks.
func RunInteractive(ctx context.Context, c Client, initialListID flux.Identifier, in io.Reader, out io.Writer) error {
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	m := newTUIModel(runCtx, c, initialListID)

	opts := []tea.ProgramOption{
		tea.WithInput(in),
		tea.WithOutput(out),
		tea.WithContext(runCtx),
	}

	// Use alternate screen when output is standard output
	if out == os.Stdout {
		opts = append(opts, tea.WithAltScreen())
	}

	p := tea.NewProgram(m, opts...)
	_, err := p.Run()
	return err
}
