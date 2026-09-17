package client

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/wotek/flux"
	"github.com/wotek/flux/example/todo/projections/lists"
)

type screenView int

const (
	viewLists screenView = iota
	viewTasks
)

// RunInteractive launches an interactive CLI session. The entry screen presents
// all available todo lists with options to manage them, and allows navigating into
// a selected list to view, add, remove, and complete tasks.
func RunInteractive(ctx context.Context, c Client, initialListID flux.Identifier, in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	currentView := viewLists
	selectedListIdx := 0
	selectedTaskIdx := 0
	currentListID := initialListID
	currentListTitle := "Default List"
	statusMessage := "Welcome! Select a list to open, or create a new one."

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Always fetch current global lists & stats
		allLists, _ := c.GetLists(ctx)
		stats, _ := c.GetCounter(ctx)

		// ---------------------------------------------------------------------
		// SCREEN 1: AVAILABLE LISTS CATALOG
		// ---------------------------------------------------------------------
		if currentView == viewLists {
			listCount := len(allLists)
			if listCount == 0 {
				selectedListIdx = -1
			} else {
				if selectedListIdx < 0 {
					selectedListIdx = 0
				} else if selectedListIdx >= listCount {
					selectedListIdx = listCount - 1
				}
			}

			renderListsScreen(out, allLists, selectedListIdx, stats.Active, stats.Archived, stats.Removed, statusMessage)
			statusMessage = ""

			fmt.Fprint(out, "lists> ")
			if !scanner.Scan() {
				fmt.Fprintln(out, "\nExiting interactive session.")
				return scanner.Err()
			}

			input := strings.TrimSpace(scanner.Text())
			cmd, arg, hasArg := parseCommand(input)

			if input == "" {
				// Empty Enter opens the currently selected list (if any), or does nothing
				if selectedListIdx >= 0 && selectedListIdx < listCount {
					currentListID = flux.NewIdentifierFromString(allLists[selectedListIdx].Identifier)
					currentListTitle = allLists[selectedListIdx].Title
					selectedTaskIdx = 0
					currentView = viewTasks
					statusMessage = fmt.Sprintf("Opened list: %q", currentListTitle)
				}
				continue
			}

			switch cmd {
			case "q", "quit", "exit":
				fmt.Fprintln(out, "Goodbye!")
				return nil

			case "h", "help":
				statusMessage = "Commands: [n]ext, [p]rev, [o]pen [num], [a]dd/new <title>, [r]efresh, [q]uit, or enter list number to open."

			case "n", "next", "j":
				if listCount > 0 {
					selectedListIdx = (selectedListIdx + 1) % listCount
					statusMessage = fmt.Sprintf("Selected list [%d]: %s", selectedListIdx+1, allLists[selectedListIdx].Title)
				}

			case "p", "prev", "k":
				if listCount > 0 {
					selectedListIdx = (selectedListIdx - 1 + listCount) % listCount
					statusMessage = fmt.Sprintf("Selected list [%d]: %s", selectedListIdx+1, allLists[selectedListIdx].Title)
				}

			case "o", "open":
				targetIdx := selectedListIdx
				if hasArg {
					num, err := strconv.Atoi(arg)
					if err == nil && num >= 1 && num <= listCount {
						targetIdx = num - 1
					} else {
						statusMessage = fmt.Sprintf("Invalid list number: %s", arg)
						continue
					}
				}
				if targetIdx >= 0 && targetIdx < listCount {
					currentListID = flux.NewIdentifierFromString(allLists[targetIdx].Identifier)
					currentListTitle = allLists[targetIdx].Title
					selectedTaskIdx = 0
					currentView = viewTasks
					statusMessage = fmt.Sprintf("Opened list: %q", currentListTitle)
				} else {
					statusMessage = "No list available to open. Press 'a' to create one."
				}

			case "a", "new", "create", "add":
				title := arg
				if !hasArg {
					fmt.Fprint(out, "Enter new list title: ")
					if scanner.Scan() {
						title = strings.TrimSpace(scanner.Text())
					}
				}
				if title == "" {
					title = "New Todo List"
				}
				slug := strings.ToLower(strings.ReplaceAll(title, " ", "-"))
				newListID := flux.NewIdentifierFromString(fmt.Sprintf("urn:todo:prod:lists:1:list:%s-%d", slug, time.Now().UnixNano()%10000))
				if err := c.CreateList(ctx, newListID, title); err != nil {
					statusMessage = fmt.Sprintf("Error creating list: %v", err)
				} else {
					currentListID = newListID
					currentListTitle = title
					selectedTaskIdx = 0
					currentView = viewTasks
					statusMessage = fmt.Sprintf("Created and opened list %q", title)
				}

			case "r", "refresh":
				statusMessage = "Refreshed lists."

			default:
				// Number typed directly opens that list
				num, err := strconv.Atoi(cmd)
				if err == nil && num >= 1 && num <= listCount {
					currentListID = flux.NewIdentifierFromString(allLists[num-1].Identifier)
					currentListTitle = allLists[num-1].Title
					selectedTaskIdx = 0
					currentView = viewTasks
					statusMessage = fmt.Sprintf("Opened list: %q", currentListTitle)
				} else {
					statusMessage = fmt.Sprintf("Unknown command %q. Type 'h' for help.", input)
				}
			}

			continue
		}

		// ---------------------------------------------------------------------
		// SCREEN 2: TASKS DETAIL SCREEN FOR A SPECIFIC LIST
		// ---------------------------------------------------------------------
		todoList, err := c.GetTodoList(ctx, currentListID)
		if err != nil {
			statusMessage = fmt.Sprintf("Error loading tasks: %v", err)
		}

		// Sync title if list appears in projection
		for _, l := range allLists {
			if l.Identifier == currentListID.String() {
				currentListTitle = l.Title
				break
			}
		}

		activeCount := len(todoList.Active)
		if activeCount == 0 {
			selectedTaskIdx = -1
		} else {
			if selectedTaskIdx < 0 {
				selectedTaskIdx = 0
			} else if selectedTaskIdx >= activeCount {
				selectedTaskIdx = activeCount - 1
			}
		}

		renderTasksScreen(out, currentListTitle, currentListID.String(), todoList.Active, todoList.Archived, selectedTaskIdx, statusMessage)
		statusMessage = ""

		fmt.Fprint(out, "tasks> ")
		if !scanner.Scan() {
			fmt.Fprintln(out, "\nExiting interactive session.")
			return scanner.Err()
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			// Empty enter cycles forward through active tasks
			if activeCount > 0 {
				selectedTaskIdx = (selectedTaskIdx + 1) % activeCount
			}
			continue
		}

		cmd, arg, hasArg := parseCommand(input)

		switch cmd {
		case "b", "back", "lists", "catalog":
			currentView = viewLists
			statusMessage = "Returned to lists catalog."

		case "q", "quit", "exit":
			fmt.Fprintln(out, "Goodbye!")
			return nil

		case "h", "help":
			statusMessage = "Commands: [n]ext, [p]rev, [a]dd <task>, [d]elete [num], [c]omplete [num], [b]ack to lists, [r]efresh, [q]uit."

		case "n", "next", "j":
			if activeCount > 0 {
				selectedTaskIdx = (selectedTaskIdx + 1) % activeCount
				statusMessage = fmt.Sprintf("Selected task [%d]: %s", selectedTaskIdx+1, todoList.Active[selectedTaskIdx])
			} else {
				statusMessage = "No active tasks to cycle through."
			}

		case "p", "prev", "k":
			if activeCount > 0 {
				selectedTaskIdx = (selectedTaskIdx - 1 + activeCount) % activeCount
				statusMessage = fmt.Sprintf("Selected task [%d]: %s", selectedTaskIdx+1, todoList.Active[selectedTaskIdx])
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
			if err := c.AddTask(ctx, currentListID, taskText); err != nil {
				statusMessage = fmt.Sprintf("Error adding task: %v", err)
			} else {
				statusMessage = fmt.Sprintf("Added task: %q", taskText)
				selectedTaskIdx = activeCount
			}

		case "d", "del", "delete", "rm", "remove":
			targetIdx := selectedTaskIdx
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
				if err := c.RemoveTask(ctx, currentListID, taskToRemove); err != nil {
					statusMessage = fmt.Sprintf("Error removing task: %v", err)
				} else {
					statusMessage = fmt.Sprintf("Removed task: %q", taskToRemove)
				}
			} else {
				statusMessage = "No active task selected to delete."
			}

		case "c", "done", "complete", "archive":
			targetIdx := selectedTaskIdx
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
				if err := c.DoneTasks(ctx, currentListID, taskToDone); err != nil {
					statusMessage = fmt.Sprintf("Error completing task: %v", err)
				} else {
					statusMessage = fmt.Sprintf("Completed task: %q", taskToDone)
				}
			} else {
				statusMessage = "No active task selected to complete."
			}

		case "r", "refresh":
			statusMessage = "Refreshed list."

		default:
			num, err := strconv.Atoi(cmd)
			if err == nil && num >= 1 && num <= activeCount {
				selectedTaskIdx = num - 1
				statusMessage = fmt.Sprintf("Selected task [%d]: %s", num, todoList.Active[selectedTaskIdx])
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

func renderListsScreen(out io.Writer, allLists []lists.ListSummary, selectedIdx, statsActive, statsArchived, statsRemoved int, msg string) {
	fmt.Fprintln(out)
	fmt.Fprintln(out, "================================================================================")
	fmt.Fprintln(out, "  FLUX CQRS TODO APP — All Todo Lists")
	fmt.Fprintf(out, "  Global Stats: Active: %d | Archived: %d | Removed: %d | Total Lists: %d\n", statsActive, statsArchived, statsRemoved, len(allLists))
	fmt.Fprintln(out, "================================================================================")

	fmt.Fprintf(out, "\nAVAILABLE TODO LISTS (%d):\n", len(allLists))
	if len(allLists) == 0 {
		fmt.Fprintln(out, "  (No lists found — press 'a' or 'new <title>' to create your first list!)")
	} else {
		for i, l := range allLists {
			cursor := "  "
			suffix := ""
			if i == selectedIdx {
				cursor = "▶ "
				suffix = "  <-- [SELECTED]"
			}
			fmt.Fprintf(out, "%s[%d] %s (Active: %d, Archived: %d)%s\n      URN: %s\n", cursor, i+1, l.Title, l.Active, l.Archived, suffix, l.Identifier)
		}
	}

	if msg != "" {
		fmt.Fprintf(out, "\nStatus: %s\n", msg)
	}

	fmt.Fprintln(out, "\nCommands:")
	fmt.Fprintln(out, "  [n] Next list      [p] Prev list      [o/Enter] Open list")
	fmt.Fprintln(out, "  [a] New list       [r] Refresh        [q] Quit")
	fmt.Fprintln(out, "  (Or type: new <title> | open <num> | <num> to open directly)")
	fmt.Fprintln(out, "--------------------------------------------------------------------------------")
}

func renderTasksScreen(out io.Writer, listTitle, listURN string, active, archived []string, selectedIdx int, msg string) {
	fmt.Fprintln(out)
	fmt.Fprintln(out, "================================================================================")
	fmt.Fprintf(out, "  FLUX CQRS TODO APP — %s\n", listTitle)
	fmt.Fprintf(out, "  URN: %s\n", listURN)
	fmt.Fprintf(out, "  List Status: %d Active | %d Archived\n", len(active), len(archived))
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
	fmt.Fprintln(out, "  [n] Next task      [p] Prev task      [a] Add task       [d] Delete task")
	fmt.Fprintln(out, "  [c] Mark done      [b] Back to lists  [r] Refresh        [q] Quit")
	fmt.Fprintln(out, "  (Or type: add <text> | del <num> | done <num> | <num> to select)")
	fmt.Fprintln(out, "--------------------------------------------------------------------------------")
}
