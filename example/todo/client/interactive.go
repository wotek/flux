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

// RunInteractive launches an interactive CLI session allowing the user to
// list and cycle through multiple todo lists, view tasks, and add/remove/complete items.
func RunInteractive(ctx context.Context, c Client, currentListID flux.Identifier, in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	selectedTaskIdx := 0
	statusMessage := "Welcome! Type 'h' or 'help' for available commands."

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 1. Fetch all known lists from read-model projection
		allLists, _ := c.GetLists(ctx)

		// 2. Fetch current list tasks from aggregate
		todoList, err := c.GetTodoList(ctx, currentListID)
		if err != nil {
			statusMessage = fmt.Sprintf("Error loading list: %v", err)
		}

		// 3. Fetch global stats
		stats, _ := c.GetCounter(ctx)

		// 4. Find title for current list
		currentTitle := "Default List"
		currentListIdx := -1
		for i, l := range allLists {
			if l.Identifier == currentListID.String() {
				currentTitle = l.Title
				currentListIdx = i
				break
			}
		}

		// 5. Keep task cursor within bounds
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

		// 6. Render dashboard
		renderDashboard(out, currentTitle, currentListID.String(), allLists, todoList.Active, todoList.Archived, selectedTaskIdx, stats.Active, stats.Archived, stats.Removed, statusMessage)
		statusMessage = ""

		// 7. Prompt user
		fmt.Fprint(out, "todo> ")
		if !scanner.Scan() {
			fmt.Fprintln(out, "\nExiting interactive session.")
			return scanner.Err()
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			// Empty enter cycles to next task in current list
			if activeCount > 0 {
				selectedTaskIdx = (selectedTaskIdx + 1) % activeCount
			}
			continue
		}

		cmd, arg, hasArg := parseCommand(input)

		switch cmd {
		case "q", "quit", "exit":
			fmt.Fprintln(out, "Goodbye!")
			return nil

		case "h", "help":
			statusMessage = "Commands: [n]ext, [p]rev, [a]dd <task>, [d]elete [num], [c]omplete [num], [nl] new list <title>, [ls] lists, [l] switch list, [tab] next list, [r]efresh, [q]uit."

		case "n", "next", "j":
			if activeCount > 0 {
				selectedTaskIdx = (selectedTaskIdx + 1) % activeCount
				statusMessage = fmt.Sprintf("Cycled to task [%d]: %s", selectedTaskIdx+1, todoList.Active[selectedTaskIdx])
			} else {
				statusMessage = "No active tasks to cycle through."
			}

		case "p", "prev", "k":
			if activeCount > 0 {
				selectedTaskIdx = (selectedTaskIdx - 1 + activeCount) % activeCount
				statusMessage = fmt.Sprintf("Cycled to task [%d]: %s", selectedTaskIdx+1, todoList.Active[selectedTaskIdx])
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

		case "c", "done", "complete":
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

		case "nl", "new", "create", "newlist":
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
				selectedTaskIdx = 0
				statusMessage = fmt.Sprintf("Created and switched to list %q (%s)", title, newListID.String())
			}

		case "ls", "lists":
			renderListsOverview(out, allLists, currentListID.String())
			fmt.Fprint(out, "Press Enter to return to active list...")
			_ = scanner.Scan()

		case "tab", "nextlist":
			if len(allLists) > 1 {
				nextIdx := (currentListIdx + 1) % len(allLists)
				currentListID = flux.NewIdentifierFromString(allLists[nextIdx].Identifier)
				selectedTaskIdx = 0
				statusMessage = fmt.Sprintf("Switched to list: %s (%s)", allLists[nextIdx].Title, allLists[nextIdx].Identifier)
			} else {
				statusMessage = "Only 1 list known. Create another list using 'nl <title>'."
			}

		case "l", "switch", "list":
			if hasArg {
				// Check if user passed a 1-based number from allLists
				num, err := strconv.Atoi(arg)
				if err == nil && num >= 1 && num <= len(allLists) {
					currentListID = flux.NewIdentifierFromString(allLists[num-1].Identifier)
					selectedTaskIdx = 0
					statusMessage = fmt.Sprintf("Switched to list: %s", allLists[num-1].Title)
				} else {
					currentListID = flux.NewIdentifierFromString(arg)
					selectedTaskIdx = 0
					statusMessage = fmt.Sprintf("Switched to list URN: %s", arg)
				}
			} else {
				if len(allLists) == 0 {
					fmt.Fprint(out, "Enter target list URN: ")
					if scanner.Scan() {
						urn := strings.TrimSpace(scanner.Text())
						if urn != "" {
							currentListID = flux.NewIdentifierFromString(urn)
							selectedTaskIdx = 0
							statusMessage = fmt.Sprintf("Switched to list: %s", urn)
						}
					}
				} else {
					renderListsOverview(out, allLists, currentListID.String())
					fmt.Fprint(out, "Select list number or enter URN: ")
					if scanner.Scan() {
						chosen := strings.TrimSpace(scanner.Text())
						num, err := strconv.Atoi(chosen)
						if err == nil && num >= 1 && num <= len(allLists) {
							currentListID = flux.NewIdentifierFromString(allLists[num-1].Identifier)
							selectedTaskIdx = 0
							statusMessage = fmt.Sprintf("Switched to list: %s", allLists[num-1].Title)
						} else if chosen != "" {
							currentListID = flux.NewIdentifierFromString(chosen)
							selectedTaskIdx = 0
							statusMessage = fmt.Sprintf("Switched to list: %s", chosen)
						}
					}
				}
			}

		case "r", "refresh":
			statusMessage = "Refreshed."

		default:
			// Check if user entered a task index number directly
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

func renderDashboard(out io.Writer, listTitle, listURN string, allLists []lists.ListSummary, active, archived []string, selectedIdx, statsActive, statsArchived, statsRemoved int, msg string) {
	fmt.Fprintln(out)
	fmt.Fprintln(out, "================================================================================")
	fmt.Fprintf(out, "  FLUX CQRS TODO APP — %s\n", listTitle)
	fmt.Fprintf(out, "  URN: %s\n", listURN)
	fmt.Fprintf(out, "  Global Stats: Active: %d | Archived: %d | Removed: %d | Total Lists: %d\n", statsActive, statsArchived, statsRemoved, len(allLists))
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
	fmt.Fprintln(out, "  [c] Mark done      [nl] New list      [l] Switch list    [tab] Next list")
	fmt.Fprintln(out, "  [ls] Overview      [r] Refresh        [q] Quit")
	fmt.Fprintln(out, "--------------------------------------------------------------------------------")
}

func renderListsOverview(out io.Writer, allLists []lists.ListSummary, currentURN string) {
	fmt.Fprintln(out)
	fmt.Fprintln(out, "--------------------------------------------------------------------------------")
	fmt.Fprintln(out, "  ALL TODO LISTS (Read-Model Overview)")
	fmt.Fprintln(out, "--------------------------------------------------------------------------------")
	if len(allLists) == 0 {
		fmt.Fprintln(out, "  No lists found yet. Press 'nl <title>' to create one.")
	} else {
		for i, l := range allLists {
			marker := "  "
			if l.Identifier == currentURN {
				marker = "▶ "
			}
			fmt.Fprintf(out, "%s[%d] %s (Active: %d, Archived: %d)\n      URN: %s\n", marker, i+1, l.Title, l.Active, l.Archived, l.Identifier)
		}
	}
	fmt.Fprintln(out, "--------------------------------------------------------------------------------")
}
