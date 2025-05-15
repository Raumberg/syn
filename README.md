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

#### PRAGMA - Compiler Directives

Used to control compiler behavior, similar to preprocessor directives in C/C++.

```
PRAGMA AUTOSAVE
```

Supported directives:
- `AUTOSAVE` - enables auto-saving when the program is interrupted by Ctrl+C signal (SIGINT)
- `CONCURRENCY <number>` - sets the global number of parallel threads for processing

Example:
```
# Enable auto-saving on Ctrl+C
PRAGMA AUTOSAVE

# Set 8 parallel threads for processing
PRAGMA CONCURRENCY 8
```

#### WITH - Contextual Settings

Defines how the dataset will be processed. Can be used with or without a code block.

```
WITH CONCURRENCY 32 {
    FROM squad
    FIELDS ["question", "answers"]
}
```

or

```
WITH STREAM {
    FROM squad
    FIELDS ["question", "answers"]
}
```

Supported parameters:
- `CONCURRENCY` - number of parallel threads for processing
- `STREAM` - load the dataset in streaming mode

#### USING - API Settings

Defines parameters for API requests.

```
USING MODEL t-tech/T-pro-it-1.0
```

or as a block:

```
USING {
    MODEL t-tech/T-pro-it-1.0
    KEY token-abc123
    URL http://0.0.0.0:8000/v1
}
```

Supported parameters:
- `MODEL` - model name for processing
- `KEY` - API key
- `URL` - base URL for requests

#### MERGE - Merging Datasets

Allows merging multiple datasets into one:

```
MERGE ds_squad, ds_deepmath
```

or with more than two datasets:

```
MERGE [ds_squad, ds_deepmath, ds_other]
```

#### PROMPT - Template for Generation

Defines a template for generating text using LLM:

```
PROMPT summarize {
    FIELDS ["question", "context"]
    "Make a brief summary of the following question and context: Question: {question} Context: {context}"
}
```

Template format:
- Template name is specified immediately after the keyword `PROMPT`
- In the `FIELDS` block, specify the fields that will be substituted into the template
- Template text is enclosed in quotes, and field names are enclosed in curly braces: `{field_name}`

You can also specify a template without a block:

```
PROMPT simple "Rewrite the question: {question}"
```

#### GENERATE - Field Generation

Creates a new field in the dataset using LLM:

```
GENERATE source_field AS target_field {
    PROMPT template_name
    TEMPERATURE 0.7
    MAX_TOKENS 100
}
```

Required parameters:
- `source_field` - original field used for generation
- `target_field` - name of the new field where the result will be saved

Additional parameters in the block:
- `PROMPT` - name of the template defined by the PROMPT operator
- `TEMPERATURE` - generation temperature (0.0 to 1.0)
- `MAX_TOKENS` - maximum number of tokens in the response

You can also use a simplified syntax:

```
GENERATE question AS summary
```

In this case, default parameters (temperature=0.7, max_tokens=1024) will be used.

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

### Example with Merging Datasets

```
FROM squad {
    FIELDS ["question", "answers"]
}

FROM zwhe99/DeepMath-103K {
    FIELDS ["question", "final_answer"]
}

# Merge datasets
MERGE ds_squad, ds_zwhe99_DeepMath_103K

SAVE "merged_dataset.json"
```

### Example with Text Generation

```
# Define the dataset
FROM squad {
    FIELDS ["question", "answers", "context"]
}

# Define a prompt template
PROMPT summarize {
    FIELDS ["question", "context"]
    "Create a one-sentence summary of the following question and context:
    Question: {question}
    Context: {context}"
}

# Generate new fields using the template
GENERATE question, context AS summary {
    PROMPT summarize
    TEMPERATURE 0.3
    MAX_TOKENS 100
}

SAVE "generated_summaries.json"
```

### Example with API Configuration

```
# Set API parameters
USING {
    MODEL "gpt-3.5-turbo"
    KEY "your-api-key"
    URL "https://api.openai.com/v1"
}

FROM squad {
    FIELDS ["question", "answers"]
}

PROMPT rewrite "Rewrite this question in a more formal way: {question}"

GENERATE question AS formal_question {
    PROMPT rewrite
    TEMPERATURE 0.5
}

SAVE "formal_questions.json"
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
- `PRAGMA` - sets compiler directives
- `WITH` - defines contextual settings
- `USING` - configures API parameters
- `MERGE` - combines multiple datasets
- `PROMPT` - defines templates for generation
- `GENERATE` - creates new fields using LLM

### Expressions in FILTER

FILTER supports the following operators:
- `=`, `==` - equality
- `!=` - inequality
- `>`, `>=` - greater than, greater than or equal
- `<`, `<=` - less than, less than or equal

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 