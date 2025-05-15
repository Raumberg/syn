.PHONY: build clean run goenv deps

# Variables
GOROOT := /usr/lib/go
GOPATH := /home/nshestopalov/projects
BINARY_NAME := sync
GO := GOROOT=$(GOROOT) GOPATH=$(GOPATH) go

# Main target - building the binary
build:
	@echo "Building $(BINARY_NAME)..."
	@$(GO) build -o $(BINARY_NAME) cmd/main.go

# Cleaning binaries and temporary files
clean:
	@echo "Cleaning..."
	@rm -f $(BINARY_NAME)
	@rm -rf output/*

# Running a specific DSL file
run:
	@echo "Running $(DSL_FILE)..."
	@./$(BINARY_NAME) --compile $(DSL_FILE)

# Example showing how to compile a file with debug mode
example:
	@echo "Running an example with debug mode..."
	@./$(BINARY_NAME) --compile examples/example1_simple.syn --debug

# Show Go environment variables
goenv:
	@echo "GOROOT: " $(GOROOT)
	@echo "GOPATH: " $(GOPATH)
	@$(GO) env

# Install all dependencies
deps:
	@echo "Installing dependencies..."
	@$(GO) mod tidy 