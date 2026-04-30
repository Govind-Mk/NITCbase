# NITCbase: A Relational Database Management System

## 🚀 Overview

**NITCbase** is an academic project designed to help students understand how a real DBMS works by building one from scratch.

It implements core components of a database engine, including storage management, indexing, and query execution, and supports SQL-like operations that simulate the internal architecture of modern database systems.

---

## ✨ Features

- SQL-like query execution
- Core relational operations:
  - SELECT  
  - PROJECT  
  - JOIN  
- B+ Tree indexing for efficient search  
- Record storage and disk management  
- Buffer and cache handling  
- Layered DBMS architecture

---

## ℹ️ Supported Operations & Example Queries

The system supports SQL-like commands with a simplified grammar as shown below.

### DDL (Data Definition Language)

Create a relation:
```sql
CREATE TABLE Students(rollno NUM, name STR, cgpa NUM);
```
Delete a relation:
```sql
DROP TABLE Students;
```
Open an existing relation:
```sql
OPEN TABLE Students;
```
Close an existing relation:
```sql
CLOSE TABLE Students;
```
Create index on an attribute:
```sql
CREATE INDEX ON Students.rollno;
```
Delete index on an attribute:
```sql
DROP INDEX ON Students.rollno;
```
Rename a relation/column:
```sql
ALTER TABLE RENAME Students TO Learners;
ALTER TABLE RENAME Learners COLUMN cgpa TO grade;
```

### DML (Data Manipulation Language)

**Note**: Most DML operations require the relation to be OPEN.

Insert one record:

```sql
INSERT INTO Students VALUES (1, alice, 9.2);
```

Insert from a CSV file (read from `../Files/Input_Files/`):

```sql
INSERT INTO Students VALUES FROM students.csv;
```

Project (copy all rows / subset of attributes):

```sql
SELECT * FROM Students INTO Students_copy;
SELECT rollno, name FROM Students INTO Students_rollno_name;
```

Select with a condition (operators supported: `=`, `<`, `<=`, `>`, `>=`, `!=`):

```sql
SELECT * FROM Students INTO Top WHERE cgpa >= 9;
SELECT rollno, name FROM Students INTO TopNames WHERE cgpa >= 9;
```

Equi-join:

```sql
SELECT * FROM X JOIN Y INTO XY WHERE X.a = Y.b;
SELECT X_a, Y_b FROM X JOIN Y INTO XYproj WHERE X.a = Y.b;
```

---

## 🏗️ Architecture

The system follows an **8-layer architecture**:

* Frontend Interface
* Schema Layer
* Algebra Layer
* B+ Tree Layer
* Block Access Layer
* Cache Layer
* Buffer Layer
* Physical Layer (Disk)

Each layer uses services of the lower layer, ensuring modularity and abstraction.

---

## 🛠️ Technical Highlights

* **Language:** C++
* **Indexing:** B+ Tree implementation for fast record retrieval.
* **Memory Management:** Custom Buffer Management system to minimize hardware bottlenecks using an LRU policy.
* **Relational Algebra:** Implementation of Equi-Join, Selection, and Projection.

---

## 📂 Project Structure

```text
NITCbase
├── Disk/                       # Disk Emulator and control tools
│   ├── disk                    # Simulated disk file
│   └── disk_run_copy           # Run copy of the disk
├── Files/                      # Data files for testing and execution
│   ├── Batch_Execution_Files/  # Batch scripts (e.g., s8test.txt, s11test.txt)
│   ├── Input_Files/            # CSV datasets for relation loading
│   └── Output_Files/           # Resultant CSVs from queries and catalogs
├── mynitcbase/                 # Core RDBMS Implementation
│   ├── Algebra/                # Relational Algebra (Select, Project, Join)
│   ├── BPlusTree/              # Indexing structures
│   ├── BlockAccess/            # Record-level disk block operations
│   ├── Buffer/                 # Buffer management (StaticBuffer, BlockBuffer)
│   ├── Cache/                  # Relational and Attribute caching
│   ├── Disk_Class/             # Low-level disk interaction
│   ├── Frontend/               # Command parsing
│   ├── FrontendInterface/      # Interface logic and Regex handlers
│   ├── Schema/                 # DDL operations (Create, Drop)
│   └── main.cpp                # Entry point
├── XFS_Interface/              # Interface for external filesystem commands
├── other/                      # Documentation, PDFs, and Dockerfile
└── Dockerfile                  # Containerization configuration
```

---

## ⚙️ Getting Started

### Prerequisites
* GCC/G++ Compiler
* Make build system
* Linux-based environment (or Docker)

### Build and Run
1.  **Clone the repository:**
    ```bash
    git clone https://github.com/Govind-Mk/NITCbase.git
    cd NITCbase/mynitcbase
    ```
2.  **Compile the project:**
    ```bash
    make
    ```
3.  **Launch the database:**
    ```bash
    ./nitcbase
    ```
    
---

## 📌 Acknowledgments
Developed as part of the Database Management Systems laboratory course at the **National Institute of Technology Calicut**.

Reference: https://nitcbase.github.io/
