# Airflow Daily Sales Pipeline

A learning project built incrementally over four weeks, with optional deployment in Week 5.
Current stage: Day 1. This starter is not yet the complete CSV-to-PostgreSQL pipeline.

## Day 1 (45–60 minutes)

### 1. Understand the workflow (10 minutes)

`extract → transform → report`

- A **task** is one unit of work, such as calculating sales.
- A **DAG** is a directed acyclic graph: tasks connected in a direction without loops.
- A **dependency** says a task must wait for another task.
- A **DAG run** is one execution of the workflow.
- The **scheduler** determines when work is ready to run.
- **Logs** record what happened inside a task.

Airflow coordinates work. Python and SQL implement the data processing.

### 2. Run the Python version (10 minutes)

Open a terminal in this project folder and run:

```powershell
python lessons/day01_python.py
```

Expected output: `Total sales: $16.00`.
The code uses integer cents to avoid floating-point arithmetic for money.

### 3. Compare the Airflow version (15 minutes)

Read `dags/hello_sales.py` beside `lessons/day01_python.py`.

- `@dag` defines the workflow.
- `@task` makes a Python function an Airflow task.
- `report(transform(extract()))` declares dependencies and data passing.
- With TaskFlow, calling decorated functions while loading the DAG builds the workflow; it does not immediately process the sales.
- `schedule=None` means manual triggering for this first lesson.
- `catchup=False` disables automatic creation of past scheduled runs.
- TaskFlow uses XCom for these small results. Large datasets should live in storage.

The DAG requires Airflow 3 inside a Linux environment, such as Docker. Running it directly with Windows Python will not start Airflow.

### 4. Practice (10 minutes)

In the Python lesson, change the notebook quantity from 2 to 4.
Predict the total before running it again. Then restore the original value.

Explain in your own words why `report` must wait for `transform`.

### 5. Prepare for Day 2 (5–15 minutes)

Start Docker Desktop and wait until its Linux engine is running. Check:

```powershell
docker info
docker compose version
```

Day 2: use the official Airflow Docker Compose learning setup, pin its Airflow version, mount this DAG folder, trigger `hello_sales`, and inspect its task logs.

Official references:
- https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
- https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html

## Upcoming milestones

- Week 1: local Airflow, dependencies, runs, and debugging.
- Week 2: daily CSV input and PostgreSQL sales summaries.
- Week 3: validation, retries, date-based processing, and duplicate-safe reruns.
- Week 4: pytest tests, DAG import checks, GitHub Actions CI, and documentation.
- Optional Week 5: demonstration deployment, approval, verification, and rollback.

CI/CD is planned; no CI/CD workflow or remote GitHub repository has been created yet.
