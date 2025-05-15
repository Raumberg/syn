# SYN DSL Examples

This directory contains examples of using the SYN DSL language of varying complexity. Each example demonstrates different capabilities of the language and can serve as a starting point for your own scripts.

## Running Examples

All examples can be run using the following command:

```bash
./syn --dsl examples/example_name.syn [--debug]
```

To save the generated Python script, add:

```bash
./syn --dsl examples/example_name.syn --dsl-save-script
```

## Example 1: Simple Dataset Query

**File:** [example1_simple.syn](example1_simple.syn)

A basic example demonstrating loading the SQuAD dataset, selecting the necessary fields, and configuring the API for LLM. Includes the global PRAGMA CONCURRENCY and PRAGMA AUTOSAVE directives.

**Key features:**
- Basic data loading syntax
- Field selection
- API configuration
- Global parallelism parameters
- Auto-save on interruption

**Run:**
```bash
./syn --dsl examples/example1_simple.syn
```

## Example 2: Data Filtering

**File:** [example2_filtering.syn](example2_filtering.syn)

Demonstrates dataset filtering by various criteria, including filtering by the difficulty of math problems and answer length.

**Key features:**
- Filtering by numeric values
- Filtering by nested fields
- Parallelism configuration

**Run:**
```bash
./syn --dsl examples/example2_filtering.syn
```

## Example 3: Prompts and Generating New Fields

**File:** [example3_prompt_generation.syn](example3_prompt_generation.syn)

This example demonstrates creating prompts (both system and user) and generating new fields based on them using LLM.

**Key features:**
- System prompts
- User prompts with templates
- Generating new fields
- Configuring temperature and tokens for LLM

**Run:**
```bash
./syn --dsl examples/example3_prompt_generation.syn
```

## Example 4: Working with Multiple Datasets

**File:** [example4_multiple_datasets.syn](example4_multiple_datasets.syn)

The example demonstrates loading, processing, and merging multiple datasets, including creating different prompts for each of them.

**Key features:**
- Loading multiple datasets
- Different processing for different data sources
- Merging datasets using MERGE
- Global and local API settings

**Run:**
```bash
./syn --dsl examples/example4_multiple_datasets.syn
```

## Example 5: Complex Workflow

**File:** [example5_complex_workflow.syn](example5_complex_workflow.syn)

An advanced example demonstrating a complex data processing pipeline with multiple steps of generation, filtering, and combining data from different sources.

**Key features:**
- Multi-stage data processing
- Local concurrency via WITH CONCURRENCY
- Processing different types of content (scientific articles and news)
- Intermediate saving of results
- Integration of data from different sources
- Comprehensive analysis using various models

**Run:**
```bash
./syn --dsl examples/example5_complex_workflow.syn
```

## Additional Resources

For more detailed information about the SYN DSL syntax, refer to the [full DSL documentation](../README-DSL.md).

To learn more about the capabilities of the SYN utility, see the [main documentation](../README.md). 