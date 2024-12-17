# Async Multiprocessing Logger and Communication Example

This Python project demonstrates inter-process communication, logging, and asynchronous processing using multiprocessing and asyncio. It is designed to showcase how child processes communicate with a parent process, maintain a logger, and handle tasks asynchronously.

---

## Table of Contents
1. [Features](#features)
2. [Dependencies](#dependencies)
3. [Architecture Overview](#architecture-overview)
4. [How It Works](#how-it-works)
5. [Installation and Usage](#installation-and-usage)

---

## Features
- **Multiprocessing**: Leverages Python's `multiprocessing` module to run a parent process and a child process.
- **Asynchronous Tasks**: Combines `asyncio` and multiprocessing to perform tasks in parallel and asynchronously.
- **Inter-process Communication**: Child and parent processes communicate using Pipes.
- **Robust Logging**: Implements a `RootLogger` process that consolidates and manages logs from all processes.
- **Task Cancellation**: Handles task cancellations cleanly without leaving orphaned processes.
- **Error Handling**: Manages unexpected exceptions and ensures graceful shutdown of processes.

---

## Dependencies
- Python 3.9+
- Built-in libraries:
  - `asyncio`
  - `multiprocessing`
  - `logging`
  - `sys`
  - `json`
  - `time`

---

## Architecture Overview
The program is built around the following key components:

### Components
1. **Logger Process**: A separate process that handles all log records sent via a `Queue`.
2. **Parent Process**: The main process that initiates communication and monitors child processes.
3. **Child Process**: Runs asynchronously, listens for requests, and performs calculations.
4. **Pipes**: Used for communication between the parent and child processes.
5. **Async Pipes**: Custom wrapper classes (`AsyncReadPipe` and `AsyncWritePipe`) to handle Pipes asynchronously.

### Flow
- The parent process sends messages (like `PING` or `REQUEST`) to the child process.
- The child process processes the requests and sends back responses (`PONG`, `OK`, `ERROR`).
- The parent process handles the responses and logs the events.
- All logs are sent to the `RootLogger` process for centralized logging.

---

## How It Works

### Workflow
1. **Health Check**:
   - The parent sends a `PING` message to verify the child process is ready.
   - The child replies with `PONG`.

2. **Request and Response**:
   - The parent sends `REQUEST` messages to the child.
   - The child performs a simulated calculation (with a random delay) and replies with `OK` or `ERROR`.

3. **Asynchronous Task Handling**:
   - Long-running tasks in the child process are executed asynchronously using `asyncio`.
   - Tasks can be canceled cleanly if new requests are received.

4. **Logging**:
   - All processes log their activity and send log records to the `RootLogger` process.
   - The logger ensures proper formatting and output to `stdout`.

5. **Graceful Shutdown**:
   - Both the child and logger processes are terminated gracefully on program exit.

---

## Installation and Usage
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-name>/src
   ```

2. Ensure you have Python 3.9 or later installed:
   ```bash
   python --version
   ```

3. Run the script:
   ```bash
   python main.py
   ```
