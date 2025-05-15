package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"

	"syn/internal/dsl"
)

func main() {
	// Set up colored output
	green := color.New(color.FgGreen).SprintFunc()
	red := color.New(color.FgRed).SprintFunc()

	// Compiler parameters
	compileFile := flag.String("compile", "", "Path to file for compilation")
	shortCompile := flag.String("c", "", "Path to file for compilation (short form)")
	saveScript := flag.Bool("save", false, "Save generated Python script")
	pythonPath := flag.String("python", "python3", "Path to Python interpreter")
	scriptDir := flag.String("outdir", "output", "Directory for output files")
	debug := flag.Bool("debug", false, "Enable debug mode (verbose output)")

	// Parse command line
	flag.Parse()

	// Check if compilation file is specified (main flag has priority)
	var filePath string
	if *compileFile != "" {
		filePath = *compileFile
	} else if *shortCompile != "" {
		filePath = *shortCompile
	}

	if filePath == "" {
		fmt.Printf("%s You must specify a code file (--compile or -c)\n", red("✗"))
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Show header
	fmt.Println()
	fmt.Println(green("  ███████╗██╗   ██╗███╗   ██╗ ██████╗"))
	fmt.Println(green("  ██╔════╝╚██╗ ██╔╝████╗  ██║██╔════╝"))
	fmt.Println(green("  ███████╗ ╚████╔╝ ██╔██╗ ██║██║     "))
	fmt.Println(green("  ╚════██║  ╚██╔╝  ██║╚██╗██║██║     "))
	fmt.Println(green("  ███████║   ██║   ██║ ╚████║╚██████╗"))
	fmt.Println(green("  ╚══════╝   ╚═╝   ╚═╝  ╚═══╝ ╚═════╝"))
	fmt.Println()

	executeDSL(filePath, *saveScript, *pythonPath, *scriptDir, *debug)
}

// formatDuration converts duration to a readable format
func formatDuration(d time.Duration) string {
	// Round to seconds
	d = d.Round(time.Second)

	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%dh %dm %ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

// executeDSL executes DSL code from a file
func executeDSL(filePath string, saveScript bool, pythonPath, scriptDir string, debug bool) {
	// Set up colored output
	green := color.New(color.FgGreen).SprintFunc()
	red := color.New(color.FgRed).SprintFunc()

	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fmt.Printf("%s File not found: %s\n", red("✗"), filePath)
		os.Exit(1)
	}

	// Initialize spinner to show loading status
	s := spinner.New(spinner.CharSets[11], 100*time.Millisecond)
	s.Suffix = fmt.Sprintf(" Processing file: %s", filePath)
	s.Color("green")
	s.Start()

	// Create DSL object
	dslEngine := dsl.NewDSL(pythonPath, scriptDir)

	// Set debug mode
	dslEngine.SetDebug(debug)

	// Stop spinner
	s.Stop()

	// Parse and compile DSL
	fmt.Printf("%s Parsing and compiling code...\n", green("→"))

	// Read file content
	content, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Printf("%s Error reading file: %v\n", red("✗"), err)
		os.Exit(1)
	}

	// Parse and compile
	pythonCode, err := dslEngine.ParseAndCompile(string(content))
	if err != nil {
		fmt.Printf("%s Compilation error: %v\n", red("✗"), err)
		os.Exit(1)
	}

	fmt.Printf("%s Code successfully compiled (generated %d bytes of Python code).\n", green("✓"), len(pythonCode))
	fmt.Println()

	// Output information about what we will do
	if saveScript {
		fmt.Printf("%s Saving and executing generated Python script...\n", green("→"))
	} else {
		fmt.Printf("%s Executing generated Python script...\n", green("→"))
	}

	// Start spinner again
	s.Suffix = " Executing..."
	s.Restart()

	// Execute script
	startTime := time.Now()

	// Stop spinner before execution to avoid interfering with output
	s.Stop()

	err = dslEngine.ExecuteFromFile(filePath, saveScript)

	duration := time.Since(startTime)

	if err != nil {
		fmt.Printf("%s Execution error: %v\n", red("✗"), err)
		os.Exit(1)
	}

	fmt.Printf("%s Execution completed in %s.\n", green("✓"), formatDuration(duration))
	fmt.Println()
}
