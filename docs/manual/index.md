# Manual Overview

This section is the operational manual.
The goal is to help you decide quickly which data to run, which mode to choose, and in what order to execute the workflow.

## Recommended reading order

1. [Chapter 1. Package Overview](package-overview.md)
2. [Chapter 2. Getting Started](getting-started.md)
3. [Chapter 3. JSON Modes](json-modes.md)
4. [Chapter 4. OpenAlex Example Workflow](openalex-workflow.md)
5. [Chapter 5. Review and Visualization](review-visualization.md)
6. [Chapter 6. Restart & Recovery](restart-recovery.md)

## Audience

- New operators: installation, mode selection, and the base execution flow
- Large-scale operators: restart/resume behavior and the split between parquet generation and materialization
- Downstream pipeline owners: OpenAlex examples and review artifact flow

## Decision map

- If DB completion speed is the priority, start with `ingest-fast*`
- If local artifacts and reuse are the priority, start with `parse-parquet*`
- For OpenAlex, the default recommendation is `parse-parquet-safe -> materialize`
- If schema inspection is the goal, use `review plan / pack / preview / schema-viewer`
