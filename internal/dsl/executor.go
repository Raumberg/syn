package dsl

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// Executor executes the generated Python code
type Executor struct {
	pythonPath string
	tempDir    string
	debug      bool
}

// NewExecutor creates a new executor
func NewExecutor(pythonPath string) *Executor {
	if pythonPath == "" {
		pythonPath = "python3"
	}

	return &Executor{
		pythonPath: pythonPath,
		tempDir:    "output",
		debug:      false,
	}
}

// SetDebug sets the debug mode
func (e *Executor) SetDebug(debug bool) {
	e.debug = debug
}

// Execute executes the generated Python code
func (e *Executor) Execute(pythonCode string, saveScript bool, scriptPath string, debug bool) error {
	// Set debug mode
	e.debug = debug

	// Create a temporary directory if it doesn't exist
	if err := os.MkdirAll(e.tempDir, 0755); err != nil {
		return fmt.Errorf("error creating script directory: %w", err)
	}

	// If path to script is not specified, generate a temporary one
	if scriptPath == "" {
		scriptPath = filepath.Join(e.tempDir, "syn_script.py")
	}

	// Save Python script to a temporary file
	if err := ioutil.WriteFile(scriptPath, []byte(pythonCode), 0644); err != nil {
		return fmt.Errorf("error saving Python script: %w", err)
	}

	// In debug mode, output more information
	if e.debug {
		fmt.Printf("Executing Python script: %s\n", scriptPath)
	}

	// Run Python script with the correct environment variables
	cmd := exec.Command(e.pythonPath, scriptPath)

	// Create channels to capture output
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("error creating stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("error creating stderr pipe: %w", err)
	}

	// Add environment variable for debug mode
	env := os.Environ()
	if e.debug {
		env = append(env, "SYN_DEBUG=1")
	} else {
		env = append(env, "SYN_DEBUG=0")
	}
	cmd.Env = env

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start the process
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("error starting Python script: %w", err)
	}

	// Start a goroutine to track signals
	go func() {
		sig := <-sigChan

		// Immediately unregister to avoid duplicate signal handling
		signal.Stop(sigChan)

		// Ignore nil signal that somehow comes at termination
		if sig == nil {
			fmt.Println("Detected empty signal at termination. Ignoring.")
			return
		}

		fmt.Printf("\nReceived signal %v. Python script will handle the signal itself if it has a SIGINT handler enabled.\n", sig)

		// DO NOT pass the signal to the Python process, it will receive the signal through its own handler
		// Just wait for the process to terminate

		// In extreme cases, after a long timeout (30 sec), forcefully terminate the process
		timeout := time.After(30 * time.Second)
		done := make(chan bool)

		go func() {
			for {
				// Check if the process has terminated
				if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
					done <- true
					return
				}
				time.Sleep(500 * time.Millisecond)
			}
		}()

		select {
		case <-done:
			// Process terminated on its own
			fmt.Println("Python process terminated.")
		case <-timeout:
			// Timeout expired, forcefully terminate
			fmt.Println("Timeout waiting for Python process to terminate. Forcefully terminating...")
			if cmd.Process != nil {
				cmd.Process.Kill()
			}
		}
	}()

	// Create scanners to read output
	stdoutScanner := bufio.NewScanner(stdout)
	stderrScanner := bufio.NewScanner(stderr)

	// Start a goroutine to read stderr
	go func() {
		for stderrScanner.Scan() {
			fmt.Fprintln(os.Stderr, stderrScanner.Text())
		}
	}()

	// Process output line by line
	for stdoutScanner.Scan() {
		line := stdoutScanner.Text()

		// Pause on lines with dataset messages
		if strings.Contains(line, "Loading dataset") ||
			strings.Contains(line, "Saving dataset") ||
			strings.Contains(line, "Done! Processed") {
			// Output message with line break
			fmt.Println(line)
		} else {
			// Other output without changes
			fmt.Println(line)
		}
	}

	// Wait for process to terminate
	if err := cmd.Wait(); err != nil {
		// Ignore termination error if it was caused by an interrupt signal
		if exitErr, ok := err.(*exec.ExitError); ok {
			if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
				if status.Signaled() && (status.Signal() == syscall.SIGINT || status.Signal() == syscall.SIGTERM) {
					fmt.Println("Process interrupted by user.")
					return nil
				}
			}
		}
		return fmt.Errorf("error executing Python script: %w", err)
	}

	// Unregister signal channels (just in case the handler didn't work)
	signal.Stop(sigChan)

	// Remove temporary file if it shouldn't be saved
	if !saveScript && scriptPath != "" {
		if err := os.Remove(scriptPath); err != nil {
			return fmt.Errorf("error removing temporary file: %w", err)
		}
	}

	return nil
}
