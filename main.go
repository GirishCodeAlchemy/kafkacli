package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/IBM/sarama"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/spf13/cobra"
)

var brokers []string
var app *tview.Application
var table *tview.Table
var detailView *tview.TextView
var inputField *tview.InputField

func main() {
	// Cobra CLI root command
	var rootCmd = &cobra.Command{
		Use:   "kafka-cli",
		Short: "A Kafka CLI tool to list topics and display consumer group lag",
	}

	// Add "list" command to list topics
	var listCmd = &cobra.Command{
		Use:   "list",
		Short: "List all Kafka topics",
		Run: func(cmd *cobra.Command, args []string) {
			listTopics(brokers)
		},
	}
	rootCmd.AddCommand(listCmd)

	// Define flags for brokers
	rootCmd.PersistentFlags().StringSliceVarP(&brokers, "brokers", "b", []string{"localhost:9092"}, "Kafka brokers")

	// Execute the root command
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// listTopics connects to Kafka brokers and lists all topics
func listTopics(brokers []string) {
	// Initialize the Kafka client
	client, err := sarama.NewClient(brokers, sarama.NewConfig())
	if err != nil {
		log.Fatalf("Error creating Kafka client: %v", err)
	}
	defer client.Close()

	// Fetch the list of topics from Kafka
	topics, err := client.Topics()
	if err != nil {
		log.Fatalf("Error fetching topics: %v", err)
	}

	// Initialize the application
	app = tview.NewApplication()

	// Create the header TextView
	header := tview.NewTextView().
		SetTextAlign(tview.AlignLeft).
		SetDynamicColors(true).
		SetText("[yellow]Kafka CLI[white] - Brokers: [cyan]" + strings.Join(brokers, ", ")).
		SetBorder(true)

	// Create the footer TextView with keybinding instructions
	footer := tview.NewTextView().
		SetTextAlign(tview.AlignLeft).
		SetDynamicColors(true).
		SetText("[yellow](q)[white] Quit  [yellow](Enter)[white] Describe Topic [yellow](e)[white] Edit Topic").
		SetBorder(true)

	// Create a new table for displaying topics
	table = tview.NewTable().SetBorders(false).SetSelectable(true, false)

	// Set table headers (minimal info)
	table.SetCell(0, 0, tview.NewTableCell("[::b]Topic Name").SetTextColor(tcell.ColorYellow))
	table.SetCell(0, 1, tview.NewTableCell("[::b]PARTITIONS").SetTextColor(tcell.ColorYellow))

	// Populate the table with topics
	for i, topic := range topics {
		partitions, err := client.Partitions(topic)
		if err != nil {
			log.Fatalf("Error fetching partitions for topic %s: %v", topic, err)
		}
		table.SetCell(i+1, 0, tview.NewTableCell(topic).SetTextColor(tcell.ColorWhite))
		table.SetCell(i+1, 1, tview.NewTableCell(fmt.Sprintf("%d", len(partitions))).SetTextColor(tcell.ColorWhite))
	}

	// Create a detailed view for describing the topic
	detailView = tview.NewTextView().
		SetDynamicColors(true)

	// Cast to tview.Box to set border and title
	detailView.SetBorder(true).SetTitle("[cyan]Topic Details[white]")

	inputField = tview.NewInputField().SetLabel("Command: ").SetFieldWidth(0)
	inputField.SetBorder(true).SetTitle("Enter Command")

	// Flex layout for the topic table and detailed view (initially empty)
	content := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(table, 0, 1, true).      // List view
		AddItem(detailView, 0, 2, false) // Details view (hidden by default)

	// Create a Flex layout to hold the components (header, table, footer)
	layout := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(header, 3, 1, false). // Header
		AddItem(inputField, 3, 1, false).
		AddItem(content, 0, 8, true). // Main table and description
		AddItem(footer, 2, 1, false)  // Footer

	// Set selected function for describing topic
	table.SetSelectedFunc(func(row, column int) {
		if row > 0 {
			topic := table.GetCell(row, 0).Text
			describeTopic(brokers, topic)
		}
	})

	// Set input capture for handling keybindings (quit, edit, etc.)
	app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEscape, tcell.KeyRune:
			switch event.Rune() {
			case 'q': // Quit on 'q'
				app.Stop()
			case 'c': // Clear detail view on 'c'
				detailView.Clear()
			case 'e': // Edit topic on 'e'
				row, _ := table.GetSelection()
				if row > 0 {
					topic := table.GetCell(row, 0).Text
					editTopic(brokers, topic)
				}
			}
		}
		return event
	})

	// Set the root of the application to the Flex layout and start the app
	if err := app.SetRoot(layout, true).Run(); err != nil {
		panic(err)
	}
}

// describeTopic shows detailed information about the selected topic
func describeTopic(brokers []string, topic string) {
	// Create an admin client
	admin, err := sarama.NewClusterAdmin(brokers, sarama.NewConfig())
	if err != nil {
		log.Fatalf("Error creating Kafka admin client: %v", err)
	}
	defer admin.Close()

	// Fetch topic configurations
	configs, err := admin.DescribeConfig(sarama.ConfigResource{
		Type: sarama.TopicResource,
		Name: topic,
	})
	if err != nil {
		log.Fatalf("Error fetching configs for topic %s: %v", topic, err)
	}

	var description strings.Builder
	description.WriteString(fmt.Sprintf("[yellow]CONFIG            VALUE\n"))
	for _, entry := range configs {
		description.WriteString(fmt.Sprintf("%-18s %s\n", entry.Name, entry.Value))
	}

	// Fetch partition and leader information
	client, err := sarama.NewClient(brokers, sarama.NewConfig())
	if err != nil {
		log.Fatalf("Error creating Kafka client: %v", err)
	}
	defer client.Close()

	partitions, _ := client.Partitions(topic)
	description.WriteString("\n[yellow]PARTITION   OLDEST_OFFSET   NEWEST_OFFSET   EMPTY   LEADER           REPLICAS   IN_SYNC_REPLICAS\n")

	for _, partition := range partitions {
		oldestOffset, _ := client.GetOffset(topic, partition, sarama.OffsetOldest)
		newestOffset, _ := client.GetOffset(topic, partition, sarama.OffsetNewest)
		replicas, _ := client.Replicas(topic, partition)
		inSyncReplicas, _ := client.InSyncReplicas(topic, partition)
		leader, _ := client.Leader(topic, partition)

		isEmpty := (newestOffset == 0)
		description.WriteString(fmt.Sprintf(
			"%-11d %-14d %-14d %-6t %-15s %-9d %-16d\n",
			partition, oldestOffset, newestOffset, isEmpty, leader.Addr(), len(replicas), len(inSyncReplicas)))
	}

	detailView.SetText(description.String())
	app.ForceDraw()

	// // Simulate fetching detailed information about the topic
	// description := fmt.Sprintf("[yellow]Topic:[white] %s\n\nPartitions: [cyan]3\nLog End Offset: [cyan]100\n", topic)

	// // Display the description in the detail view
	// detailView.SetText(description)

	// // Update the Flex layout to show the detailed view alongside the list
	// app.ForceDraw()
}

// editTopic handles the editing of the selected topic (placeholder functionality)
func editTopic(brokers []string, topic string) {
	// In a real implementation, you would allow the user to edit the topic details here
	detailView.SetText(fmt.Sprintf("[yellow]Editing topic: [white]%s", topic))

	// Update the Flex layout to show the editing view alongside the list
	app.ForceDraw()
}
