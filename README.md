# SYN - Domain-Specific Language for Datasets

A powerful Go utility with a domain-specific language (DSL) for processing datasets through LLM APIs and other data sources.

## Features

- 🔧 **Simple DSL Syntax**: Intuitive language for dataset operations
- 🧩 **Versatility**: Works with various data formats and structures
- 🧠 **LLM API Integration**: Process data through machine learning models
- 📊 **Data Filtering**: Built-in filtering by numeric or string values
- 🛡️ **Robust Error Handling**: Reliable processing with detailed diagnostics
- 🎨 **Beautiful Interface**: Colorful output with progress indicators
- 📝 **Detailed Statistics**: Informative output about task progress

## Installation

```bash
# Clone repository
git clone https://github.com/Raumberg/syn.git
cd syn

# Build
make build
```

## Quick Start

```bash
# Run with the example DSL file
./sync --compile examples/example1_simple.syn

# Run with detailed output in debug mode
./sync --compile examples/example1_simple.syn --debug

# Or use the make command for a quick example
make example
```

## Command Line Parameters

- `--compile` or `-c` - path to the DSL code file
- `--save` - save the generated Python script
- `--python` - path to the Python interpreter (default `python3`)
- `--outdir` - directory for saving generated scripts (default `output`)
- `--debug` - enable debug mode (detailed output)

## DSL Syntax

### Basic Constructs

#### FROM - Data Source

Specifies which dataset to load from Hugging Face.

```
FROM squad
```

or with an instruction block:

```
FROM zwhe99/DeepMath-103K {
    FIELDS ["question", "final_answer", "difficulty"]
    FILTER difficulty >= 8
}
```

#### FIELDS - Field Selection

Defines which fields to select from the dataset.

```
FIELDS ["question", "answers", "context"]
```

or for a single field:

```
FIELDS "question"
```

#### FILTER - Data Filtering

Allows filtering data by various criteria.

```
FILTER difficulty >= 8
```

or for nested fields:

```
FILTER instruction {
    language = "Russian";
    length >= 2048;
}
```

#### SAVE - Saving Results

Allows saving the processed dataset to a file:

```
SAVE "output/result.json"
```

### Comments

DSL supports single-line Python-style comments:

```
# This is a comment
FROM squad # This is also a comment
```

## Examples

### Simple Example

```
FROM squad
FIELDS ["question", "answers"]
SAVE "output.json"
```

### Example with Filtering

```
FROM zwhe99/DeepMath-103K {
    FIELDS ["question", "final_answer", "difficulty"]
    FILTER difficulty >= 8
}
SAVE "filtered_math.json"
```

### Example with Multiple Datasets

```
FROM squad {
    FIELDS ["question", "answers", "context"]
    FILTER context.length <= 2000
}

FROM zwhe99/DeepMath-103K {
    FIELDS ["question", "final_answer", "difficulty"]
    FILTER difficulty >= 8
}

SAVE "latest_dataset.json"
```

## How It Works

1. SYN parses your DSL script into an abstract syntax tree (AST)
2. Compiles AST into equivalent Python code using Hugging Face Datasets API
3. Executes the generated code for data processing
4. Saves results in the specified format

## Usage in Command Line

```bash
# Basic usage
./sync --compile script.syn

# Save the generated Python script
./sync --compile script.syn --save

# Specify the path to the Python interpreter
./sync --compile script.syn --python /usr/bin/python3.9

# Specify the directory for scripts
./sync --compile script.syn --outdir ./scripts
```

## Syntax Reference

### Supported Operators

- `FROM` - loads a dataset from a source
- `FIELDS` - selects fields from the dataset
- `FILTER` - filters data by criteria
- `SAVE` - saves the processed dataset

### Expressions in FILTER

FILTER supports the following operators:
- `=`, `==` - equality
- `!=` - inequality
- `>`, `>=` - greater than, greater than or equal
- `<`, `<=` - less than, less than or equal

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 