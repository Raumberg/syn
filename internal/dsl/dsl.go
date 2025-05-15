package dsl

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
)

// DSL is a facade for working with our SYNC compiler
type DSL struct {
	parser    *Parser
	compiler  *Compiler
	executor  *Executor
	scriptDir string
	debug     bool
}

// NewDSL creates a new compiler object
func NewDSL(pythonPath, scriptDir string) *DSL {
	return &DSL{
		executor:  NewExecutor(pythonPath),
		scriptDir: scriptDir,
		debug:     false,
	}
}

// SetDebug sets the debug mode
func (d *DSL) SetDebug(debug bool) {
	d.debug = debug
}

// ParseAndCompile parses the code and compiles it to Python
func (d *DSL) ParseAndCompile(input string) (string, error) {
	// Create a parser
	d.parser = NewParser(input)
	d.parser.SetDebug(d.debug)

	// Parse the code
	program, err := d.parser.Parse()
	if err != nil {
		return "", fmt.Errorf("parsing error: %w", err)
	}

	// Create a compiler
	d.compiler = NewCompiler(program)
	d.compiler.SetDebug(d.debug)

	// Compile to Python
	pythonCode := d.compiler.Compile()

	return pythonCode, nil
}

// ExecuteFromFile reads code from a file, compiles and executes it
func (d *DSL) ExecuteFromFile(filePath string, saveScript bool) error {
	// Read the file
	input, err := ioutil.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("file reading error: %w", err)
	}

	// Parse and compile
	pythonCode, err := d.ParseAndCompile(string(input))
	if err != nil {
		return err
	}

	// Generate output file name
	baseName := filepath.Base(filePath)
	ext := filepath.Ext(baseName)
	outputName := baseName[:len(baseName)-len(ext)] + ".py"
	outputPath := filepath.Join(d.scriptDir, outputName)

	// Execute the script
	return d.executor.Execute(pythonCode, saveScript, outputPath, d.debug)
}

// ExecuteFromString compiles and executes code from a string
func (d *DSL) ExecuteFromString(input string, saveScript bool, scriptName string) error {
	// Parse and compile
	pythonCode, err := d.ParseAndCompile(input)
	if err != nil {
		return err
	}

	// Generate output file name
	var outputPath string
	if scriptName != "" {
		outputPath = filepath.Join(d.scriptDir, scriptName)
	}

	// Execute the script
	return d.executor.Execute(pythonCode, saveScript, outputPath, d.debug)
}
