"""
Async Multiprocessing Communication and Logging Example
======================================================

This script demonstrates an implementation of inter-process communication, asynchronous task handling,
and centralized logging using Python's built-in `multiprocessing` and `asyncio` libraries.

Key Features:
-------------
- **Asynchronous Processing**: Child processes handle tasks asynchronously using `asyncio`.
- **Multiprocessing**: Parent and child processes communicate using multiprocessing Pipes.
- **Centralized Logging**: A separate RootLogger process consolidates logs from multiple processes.
- **Graceful Shutdown**: Ensures processes are terminated cleanly, even on errors or user interruption.
- **Task Management**: Simulated tasks are executed, canceled, or monitored based on messages received.

Components:
-----------
1. **RootLogger Process**:
   - Manages and formats log records sent by other processes.

2. **Parent Process**:
   - Sends requests to the child process, monitors responses, and handles communication.

3. **Child Process**:
   - Listens for incoming messages, performs calculations, and replies with appropriate responses.

4. **Async Pipes**:
   - `AsyncReadPipe` and `AsyncWritePipe` classes wrap multiprocessing Pipes for asynchronous usage.

Usage:
------
Run the script using Python 3.9+:
    $ python main.py

The program sends simulated tasks to the child process, logs events, and ensures graceful termination
on user interruption (CTRL+C).

Dependencies:
-------------
- Python 3.9+
- Built-in Libraries: `asyncio`, `multiprocessing`, `logging`, `sys`, `json`, `time`, `random`
"""

import asyncio
import json
import logging
import logging.handlers
import sys
from multiprocessing import Pipe, Process, Queue
from multiprocessing.connection import Connection
from multiprocessing.queues import Queue as MPQueue
from random import choice, randint
from time import perf_counter, sleep
from typing import Literal, TypedDict, cast


class MessageContract(TypedDict):
    """Simple Message Structure to test Pipe communication"""

    value: Literal["PING", "PONG", "OK", "ERROR", "REQUEST"]


class AsyncReadPipe:
    """
    Asynchronous wrapper for a multiprocessing Pipe's read functionality.

    Provides non-blocking methods to poll and read data from the pipe using asyncio.
    """

    def __init__(
        self,
        recv_channel: Connection,
    ) -> None:
        self._recv_ch = recv_channel

    async def poll(self, timeout: float = 100.0) -> bool:
        """
        Asynchronously check if there is data available to read from the pipe.

        This method uses an executor to prevent blocking the asyncio event loop.

        :param timeout: Maximum time (in seconds) to wait for data availability. Defaults to 100 seconds.
        :type timeout: float
        :return: True if data is available, False otherwise.
        :rtype: bool
        """

        loop = asyncio.get_running_loop()

        task_ready = await loop.run_in_executor(
            None,
            self._recv_ch.poll,
            timeout,
        )

        return task_ready

    async def read(self) -> MessageContract:
        """
        Asynchronously read data from the pipe.

        Reads a JSON-encoded message, decodes it, and returns the result as a dictionary
        conforming to the `MessageContract` type.

        :return: The message read from the pipe.
        :rtype: MessageContract
        :raises json.JSONDecodeError: If the incoming data is not valid JSON.
        """

        loop = asyncio.get_running_loop()

        read_bytes = await loop.run_in_executor(
            None,
            self._recv_ch.recv_bytes,
        )

        read_str = read_bytes.decode(encoding="utf-8")
        read_dict = json.loads(read_str)

        return cast(MessageContract, read_dict)


class AsyncWritePipe:
    """
    Asynchronous wrapper for a multiprocessing Pipe's write functionality.

    Provides non-blocking methods to send data through the pipe using asyncio.
    """

    def __init__(
        self,
        send_channel: Connection,
    ) -> None:
        self._send_ch = send_channel

    async def send(self, msg: MessageContract) -> None:
        """
        Asynchronously send a message through the pipe.

        This method JSON-encodes the message, converts it to bytes, and sends it
        without blocking the event loop.

        :param msg: The message to send, adhering to the `MessageContract` type.
        :type msg: MessageContract
        :raises TypeError: If the message cannot be JSON-encoded.
        """

        write_str = json.dumps(msg)
        write_bytes = write_str.encode(encoding="utf-8")

        loop = asyncio.get_running_loop()

        await loop.run_in_executor(
            None,
            self._send_ch.send_bytes,
            write_bytes,
        )


def loggin_process(logger_queue: MPQueue[logging.LogRecord | None]) -> None:
    """
    Root logger process to handle and display log records.

    Retrieves log records from the queue, formats them, and outputs them to `stdout`.

    :param logger_queue: A queue to receive log records from other processes.
    :type logger_queue: multiprocessing.queues.Queue
    :return: None
    """

    root_logger = logging.getLogger("RootLogger")
    root_logger.setLevel(logging.INFO)

    root_logger_handler = logging.StreamHandler()
    root_logger_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - "
        "PID: [%(processName)s:%(process)d] - "
        "Thread: [%(threadName)s:%(thread)d] - "
        "%(name)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s",
    )

    root_logger_handler.setFormatter(root_logger_formatter)
    root_logger.addHandler(root_logger_handler)

    root_logger.info("'Root Logger' is ready!")

    while True:
        try:
            log_record = logger_queue.get()

            if log_record is None:
                break

            root_logger.handle(log_record)

        except Exception as ex:
            print(
                f"ERROR: Unexpected Exception ('Root Logger') - "
                f"{ex.__class__.__name__}: {str(ex)}",
                file=sys.stderr,
            )
            return

    root_logger.info("'Root Logger' is gracefully finished")


def configure_logger(
    logger_queue: MPQueue[logging.LogRecord | None],
    name: str,
) -> logging.Logger:
    """
    Configures a logger to send log records to the RootLogger process.

    :param logger_queue: A queue to send log records to the RootLogger process.
    :type logger_queue: multiprocessing.queues.Queue
    :param name: The name of the logger.
    :type name: str
    :return: Configured logger instance.
    :rtype: logging.Logger
    """

    process_logger = logging.getLogger(name)

    process_logger_handler = logging.handlers.QueueHandler(logger_queue)

    process_logger.addHandler(process_logger_handler)
    process_logger.setLevel(logging.INFO)

    return process_logger


def stop_process(
    proc: Process,
    timeout: float = 1.5,
    logger: logging.Logger | None = None,
) -> None:
    """
    Gracefully stops a multiprocessing process.

    Attempts to terminate the process and escalates to killing it if it remains alive.

    :param proc: The process to be stopped.
    :type proc: multiprocessing.Process
    :param timeout: Time (in seconds) to wait before escalating termination. Defaults to 1.5 seconds.
    :type timeout: float
    :param logger: Logger instance to log process termination events. If None, prints to console.
    :type logger: logging.Logger | None
    :return: None
    """

    log_func = logger.info if logger is not None else print

    log_func(f"stopping {proc.name = }")

    if proc.is_alive():
        log_func(f"'{proc.name}' is still alive sending 'SIGTERM' signal")
        proc.terminate()
        sleep(timeout)

    if proc.is_alive():
        log_func(f"'{proc.name}' is still alive sending 'SIGKILL' signal")
        proc.kill()
        sleep(timeout)

    proc.join()
    log_func(f"'{proc.name}' was successfully stopped!")


def child_make_calc(
    value: str | None,
    logger: logging.Logger,
) -> float:
    """
    Simulates a calculation with a random delay.

    Logs the input value and simulates a task that may raise an error if the calculation
    time exceeds a certain threshold.

    :param value: Input value for the calculation task.
    :type value: str | None
    :param logger: Logger instance to log events.
    :type logger: logging.Logger
    :return: The time taken to complete the calculation.
    :rtype: float
    :raises ValueError: If the simulated calculation time exceeds 1.8 seconds.
    """

    logger.info("Got Input for calculation - %s", value)

    calc_time = randint(100, 200) / 100  # 1.00..2.00 s

    if calc_time > 1.8:
        raise ValueError(f"Test value error (calc_time > 1.8)")

    calc_start = perf_counter()

    sleep(calc_time)

    return perf_counter() - calc_start


async def child_make_calc_task(
    value: str | None,
    logger: logging.Logger,
) -> float:
    """
    Asynchronously executes the `child_make_calc` function in a thread pool.

    :param value: Input value for the calculation task.
    :type value: str | None
    :param logger: Logger instance to log events.
    :type logger: logging.Logger
    :return: The time taken for the calculation.
    :rtype: float
    :raises asyncio.CancelledError: If the task is canceled during execution.
    """

    loop = asyncio.get_running_loop()

    try:
        calc_res = await loop.run_in_executor(
            None,
            child_make_calc,
            value,
            logger,
        )
    except asyncio.CancelledError as e:
        logger.warning("Task is canceled!")
        raise e

    return calc_res


async def process_finished_task(
    task: asyncio.Task[float],
    writer: AsyncWritePipe,
    logger: logging.Logger,
) -> None:
    """
    Handles a completed task in the child process.

    Logs the task result or any exception raised during its execution. Sends the appropriate
    response (`OK` or `ERROR`) back to the parent process.

    :param task: The asyncio task that has completed.
    :type task: asyncio.Task[float]
    :param writer: Asynchronous pipe writer for sending responses.
    :type writer: AsyncWritePipe
    :param logger: Logger instance to log events.
    :type logger: logging.Logger
    :return: None
    """

    try:
        task_result = task.result()
        logger.info(
            "Task completed successfully, time taken - %.5f s",
            task_result,
        )
        await writer.send({"value": "OK"})
    except Exception as ex:
        logger.error(
            "Unexpected Exception ('Child Process') - %s: %s",
            ex.__class__.__name__,
            str(ex),
        )
        await writer.send({"value": "ERROR"})


async def child_loop(
    reader: AsyncReadPipe,
    writer: AsyncWritePipe,
    logger: logging.Logger,
) -> None:
    """
    Asynchronous loop for the child process.

    Listens for incoming requests, executes tasks (calculations), and sends responses
    back to the parent process. Manages active tasks and handles cancellations.

    :param reader: Asynchronous pipe reader for receiving messages.
    :type reader: AsyncReadPipe
    :param writer: Asynchronous pipe writer for sending messages.
    :type writer: AsyncWritePipe
    :param logger: Logger instance to log events.
    :type logger: logging.Logger
    :return: None
    """

    active_task: asyncio.Task[float] | None = None

    while True:

        poll_task = asyncio.create_task(reader.poll(), name="ChildPollTask")

        completed_tasks, _ = await asyncio.wait(
            (poll_task, active_task) if active_task is not None else (poll_task,),
            return_when=asyncio.FIRST_COMPLETED,
        )

        task = completed_tasks.pop()

        if task.get_name() == poll_task.get_name():

            request_ready = task.result()

            if not request_ready:
                # Log that channel is empty
                logger.info(f"'read_ch' is empty, waiting for tasks...")
                continue

            request = await reader.read()
            request_value = request.get("value")

            if (request_value is not None) and (request_value == "PING"):
                await writer.send({"value": "PONG"})
                continue

            if (active_task is not None) and (not active_task.done()):
                # Cancel task ongoing task
                active_task.cancel()

                try:
                    await active_task
                except asyncio.CancelledError:
                    logger.info("Task was canceled successfully!")
                except Exception as ex:
                    logger.error(
                        "Unexpected Exception ('Child Process') - %s: %s",
                        ex.__class__.__name__,
                        str(ex),
                    )

            elif (active_task is not None) and active_task.done():
                # Send result of completed task
                await process_finished_task(active_task, writer, logger)

            active_task = asyncio.create_task(
                child_make_calc_task(request_value, logger),
                name="ChildCalcTask",
            )

        elif (active_task is not None) and (task.get_name() == active_task.get_name()):
            # Send result of completed task
            await process_finished_task(active_task, writer, logger)

            active_task = None


def child_runner(
    recv_ch: Connection,
    send_ch: Connection,
    logger_queue: MPQueue[logging.LogRecord | None],
) -> None:
    """
    Runner function for the child process.

    Configures the logger and starts the asynchronous child loop to handle requests
    from the parent process.

    :param recv_ch: The receiving end of the pipe connected to the parent process.
    :type recv_ch: multiprocessing.connection.Connection
    :param send_ch: The sending end of the pipe connected to the parent process.
    :type send_ch: multiprocessing.connection.Connection
    :param logger_queue: A queue to send log records to the RootLogger process.
    :type logger_queue: multiprocessing.queues.Queue
    :return: None
    """

    child_logger = configure_logger(logger_queue, "ChildProcess")

    reader = AsyncReadPipe(recv_ch)
    writer = AsyncWritePipe(send_ch)

    try:
        asyncio.run(child_loop(reader, writer, child_logger))
    except KeyboardInterrupt:
        child_logger.info("'KeyboardInterrupt' - child process, finishing the process")
    except Exception as ex:
        child_logger.error(
            "Unexpected Exception ('Child Process') - %s: %s",
            ex.__class__.__name__,
            str(ex),
        )


async def parent_loop(
    recv_ch: Connection,
    send_ch: Connection,
    logger_queue: MPQueue[logging.LogRecord | None],
) -> None:
    """
    Asynchronous loop for the parent process.

    Handles communication with the child process by sending requests, receiving responses,
    and logging the results. Performs health checks to ensure the child process is operational.

    :param recv_ch: The receiving end of the pipe connected to the child process.
    :type recv_ch: multiprocessing.connection.Connection
    :param send_ch: The sending end of the pipe connected to the child process.
    :type send_ch: multiprocessing.connection.Connection
    :param logger_queue: A queue to send log records to the RootLogger process.
    :type logger_queue: multiprocessing.queues.Queue
    :return: None
    :raises ValueError: If health check (PING/PONG) fails.
    """

    logger = configure_logger(logger_queue, "MainLoop")

    poll_timeouts = [0.2, 1.2, 1.5, 1.7]

    reader = AsyncReadPipe(recv_ch)
    writer = AsyncWritePipe(send_ch)

    await writer.send({"value": "PING"})

    pong_ready = await reader.poll()
    if not pong_ready:
        raise ValueError(f"Child process is not available: {pong_ready = }")

    pong_result = await reader.read()
    pong_value = pong_result.get("value")
    if pong_value != "PONG":
        raise ValueError(f"Incorrect 'PING' response. Expected: 'PONG'. Got: {pong_value}")

    logger.info(
        "Healthcheck is passed. Got: 'pong_value' = '%s' from child. Ready to Go!",
        pong_value,
    )

    while True:
        await writer.send({"value": "REQUEST"})

        response_ready = await reader.poll(choice(poll_timeouts))
        if not response_ready:
            logger.warning("No response. Sending new Request...")
            continue

        response_result = await reader.read()
        response_value = response_result.get("value")

        logger.info(
            "Got response from child: 'response_value' = '%s'",
            response_value,
        )


def main() -> None:
    """
    Entry point of the program.

    Initializes the logging process, sets up inter-process communication channels, and
    starts the parent and child processes. Ensures graceful termination of processes
    upon program exit or exceptions.

    :return: None
    """

    logger_queue: MPQueue[logging.LogRecord | None] = Queue()
    log_process = Process(
        target=loggin_process,
        args=(logger_queue,),
        name="RootLoggerProcess",
    )
    log_process.start()

    main_logger = configure_logger(logger_queue, "__main__")

    parent_recv, child_send = Pipe(duplex=False)
    child_recv, parent_send = Pipe(duplex=False)

    child_process = Process(
        target=child_runner,
        args=(child_recv, child_send, logger_queue),
        name="AsycChildTest",
    )
    child_process.start()

    try:
        asyncio.run(parent_loop(parent_recv, parent_send, logger_queue))
    except KeyboardInterrupt:
        main_logger.info("'KeyboardInterrupt' - main process, exiting the program")
    except Exception as ex:
        main_logger.error(
            "Unexpected Exception ('__main__') - %s: %s",
            ex.__class__.__name__,
            str(ex),
        )

    main_logger.info(
        "Programm finished its execution, gracefully stoping child processes",
    )

    stop_process(child_process, logger=main_logger)

    logger_queue.put(None)

    stop_process(log_process)
    return


if __name__ == "__main__":
    main()
