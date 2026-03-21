# LSM Tree Index System

This project implements a simple LSM (Log-Structured Merge Tree) based indexing system for a fact table.

## Features
- LSM index per column (D1, D2, ...)
- Insert and delete operations
- Query support with:
  - Equality conditions
  - AND / OR logic
- Aggregate functions:
  - MIN, MAX, AVG, SUM, COUNT
- Query execution:
  - With index
  - Without index (full scan)

## Data Model
FactTable(ID, D1, ..., Dn, Fact1, ..., Factn)

## LSM Structure
- Level 0 capacity: 1000 records
- Each next level is 3x larger than the previous

## Input
- Schema definition via text file
- Query definitions via text file

## Notes
- Designed for tables up to 13,000 rows
- Developed as part of the Database Systems course (ETF Belgrade)
