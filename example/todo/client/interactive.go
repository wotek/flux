package client

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/wotek/flux"
)

// RunInteractive launches an interactive CLI session allowing the user to
// list, cycle through, add, remove, and complete tasks in a todo list.
func RunInteractive(ctx context.Context, c Client, listIdentifier flux.Identifier, in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	selectedIndex := 0
	statusMessage := "Welcome! Type 'h' or 'help' for command list."

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 1. Fetch current list
		todoList, err := c.GetTodoList(ctx, listIdentifier)
		if err != nil {
			statusMessage = fmt.Sprintf("Error loading list: %v", err)
		}

		// 2. Fetch counter stats
		stats, _ := c.GetCounter(ctx)

		// 3. Keep selected index within bounds
		activeCount := len(todoList.Active)
		if activeCount == 0 {
			selectedIndex = -1
		} else {
			if selectedIndex < 0 {
				selectedIndex = 0
			} else if selectedIndex >= activeCount {
				selectedIndex = activeCount - 1
			}
		}

		// 4. Render terminal screen
		renderScreen(out, listIdentifier.String(), todoList.Active, todoList.Archived, selectedIndex, stats.Active, stats.Archived, stats.Removed, statusMessage)
		statusMessage = ""

		// 5. Read command input
		fmt.Fprint(out, "todo> ")
		if !scanner.Scan() {
			// EOF reached
			fmt.Fprintln(out, "\nExiting interactive session.")
			return scanner.Err()
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			// Empty enter cycles to next task
			if activeCount > 0 {
				selectedIndex = (selectedIndex + 1) % activeCount
			}
			continue
		}

		cmd, arg, hasArg := parseCommand(input)

		switch cmd {
		case "q", "quit", "exit":
			fmt.Fprintln(out, "Goodbye!")
			return nil

		case "h", "help":
			statusMessage = "Commands: [n]ext, [p]rev, [a]dd <task>, [d]elete [num], [c]omplete [num], [l]ist <urn>, [r]efresh, [q]uit, or enter item number to select."

		case "n", "next", "j":
			if activeCount > 0 {
				selectedIndex = (selectedIndex + 1) % activeCount
				statusMessage = fmt.Sprintf("Cycled to task [%d]: %s", selectedIndex+1, todoList.Active[selectedIndex])
			} else {
				statusMessage = "No active tasks to cycle through."
			}

		case "p", "prev", "k":
			if activeCount > 0 {
				selectedIndex = (selectedIndex - 1 + activeCount) % activeCount
				statusMessage = fmt.Sprintf("Cycled to task [%d]: %s", selectedIndex+1, todoList.Active[selectedIndex])
			} else {
				statusMessage = "No active tasks to cycle through."
			}

		case "a", "add":
			taskText := arg
			if !hasArg {
				fmt.Fprint(out, "Enter task description: ")
				if scanner.Scan() {
					taskText = strings.TrimSpace(scanner.Text())
				}
			}
			if taskText == "" {
				statusMessage = "Task description cannot be empty."
				continue
			}
			if err := c.AddTask(ctx, listIdentifier, taskText); err != nil {
				statusMessage = fmt.Sprintf("Error adding task: %v", err)
			} else {
				statusMessage = fmt.Sprintf("Added task: %q", taskText)
				// Set cursor to the newly added item (end of list)
				selectedIndex = activeCount
			}

		case "d", "del", "delete", "rm", "remove":
			targetIdx := selectedIndex
			if hasArg {
				num, err := strconv.Atoi(arg)
				if err == nil && num >= 1 && num <= activeCount {
					targetIdx = num - 1
				} else {
					statusMessage = fmt.Sprintf("Invalid task number: %s", arg)
					continue
				}
			}

			if targetIdx >= 0 && targetIdx < activeCount {
				taskToRemove := todoList.Active[targetIdx]
				if err := c.RemoveTask(ctx, listIdentifier, taskToRemove); err != nil {
					statusMessage = fmt.Sprintf("Error removing task: %v", err)
				} else {
					statusMessage = fmt.Sprintf("Removed task: %q", taskToRemove)
				}
			} else {
				statusMessage = "No active task selected to delete."
			}

		case "c", "done", "complete":
			targetIdx := selectedIndex
			if hasArg {
				num, err := strconv.Atoi(arg)
				if err == nil && num >= 1 && num <= activeCount {
					targetIdx = num - 1
				} else {
					statusMessage = fmt.Sprintf("Invalid task number: %s", arg)
					continue
				}
			}

			if targetIdx >= 0 && targetIdx < activeCount {
				taskToDone := todoList.Active[targetIdx]
				if err := c.DoneTasks(ctx, listIdentifier, taskToDone); err != nil {
					statusMessage = fmt.Sprintf("Error completing task: %v", err)
				} else {
					statusMessage = fmt.Sprintf("Completed task: %q", taskToDone)
				}
			} else {
				statusMessage = "No active task selected to complete."
			}

		case "r", "refresh":
			statusMessage = "Refreshed."

		case "l", "list":
			if hasArg {
				listIdentifier = flux.NewIdentifierFromString(arg)
				selectedIndex = 0
				statusMessage = fmt.Sprintf("Switched to list: %s", arg)
			} else {
				fmt.Fprint(out, "Enter target list URN: ")
				if scanner.Scan() {
					newURN := strings.TrimSpace(scanner.Text())
					if newURN != "" {
						listIdentifier = flux.NewIdentifierFromString(newURN)
						selectedIndex = 0
						statusMessage = fmt.Sprintf("Switched to list: %s", newURN)
					}
				}
			}

		default:
			// Check if user entered a direct index number (e.g. "1", "2")
			num, err := strconv.Atoi(cmd)
			if err == nil && num >= 1 && num <= activeCount {
				selectedIndex = num - 1
				statusMessage = fmt.Sprintf("Selected task [%d]: %s", num, todoList.Active[selectedIndex])
			} else {
				statusMessage = fmt.Sprintf("Unknown command %q. Type 'h' for help.", input)
			}
		}
	}
}

func parseCommand(input string) (cmd string, arg string, hasArg bool) {
	parts := strings.SplitN(input, " ", 2)
	cmd = strings.ToLower(parts[0])
	if len(parts) > 1 {
		arg = strings.TrimSpace(parts[1])
		hasArg = arg != ""
	}
	return cmd, arg, hasArg
}

func renderScreen(out io.Writer, listURN string, active, archived []string, selectedIdx, statsActive, statsArchived, statsRemoved int, msg string) {
	fmt.Fprintln(out)
	fmt.Fprintln(out, "================================================================================")
	fmt.Fprintln(out, "  FLUX CQRS TODO APP (Interactive Mode)")
	fmt.Fprintf(out, "  List: %s\n", listURN)
	fmt.Fprintf(out, "  Stats (Counter Projection): Active: %d | Archived: %d | Removed: %d\n", statsActive, statsArchived, statsRemoved)
	fmt.Fprintln(out, "================================================================================")

	fmt.Fprintf(out, "\nACTIVE TASKS (%d):\n", len(active))
	if len(active) == 0 {
		fmt.Fprintln(out, "  (no active tasks — press 'a' to add one)")
	} else {
		for i, task := range active {
			cursor := "  "
			suffix := ""
			if i == selectedIdx {
				cursor = "▶ "
				suffix = "  <-- [SELECTED]"
			}
			fmt.Fprintf(out, "%s[%d] %s%s\n", cursor, i+1, task, suffix)
		}
	}

	if len(archived) > 0 {
		fmt.Fprintf(out, "\nARCHIVED / COMPLETED (%d):\n", len(archived))
		for _, task := range archived {
			fmt.Fprintf(out, "    ✓ %s\n", task)
		}
	}

	if msg != "" {
		fmt.Fprintf(out, "\nStatus: %s\n", msg)
	}

	fmt.Fprintln(out, "\nCommands:")
	fmt.Fprintln(out, "  [n] Next item      [p] Prev item      [a] Add task       [d] Delete selected")
	fmt.Fprintln(out, "  [c] Mark done      [r] Refresh        [l] Switch list    [q] Quit")
	fmt.Fprintln(out, "  (Or type: add <text> | del <num> | done <num> | <num> to select)")
	fmt.Fprintln(out, "--------------------------------------------------------------------------------")
}
