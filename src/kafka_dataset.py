"""Copyright (C) 2020  Benjamin RIVIERE

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see
https://github.com/bendabir/torch-kafka/blob/master/LICENSE.
"""
import logging
import os
import signal
import sys
from kafka import KafkaConsumer
from kafka.errors import CommitFailedError
from torch.utils.data import get_worker_info
from torch.utils.data import IterableDataset
import torch.multiprocessing as mp


_logger = logging.getLogger(__name__)


class KafkaDataset(IterableDataset):
    """PyTorch dataset that streams data from Kafka. It runs with a PyTorch
    DataLoader, either single or multi processed.

    The class is designed to be inherited to define what to do with the Kafka
    records.

    Please note than all parameters you pass to the Kafka consumer will be
    duplicated when multiprocessing is on.

    Parameters
    ----------
    All parameters are passed to the KafkaConsumer constructor. Note that auto
    commit is disabled.
    See https://kafka-python.readthedocs.io/en/latest/apidoc/KafkaConsumer.html
    """
    # Signal the process should be listening to for Kafka consumer commits
    # On Windows and Mac OS, using SIGINT instead but I don't know if this will
    # work.
    if sys.platform in {"linux", "linux2"}:
        _COMMIT_SIGNAL = signal.SIGUSR1
    elif sys.platform in {"darwin", "win32", "win64"}:
        _COMMIT_SIGNAL = signal.SIGINT
    else:
        raise RuntimeError(f"Unsupported platform '{sys.platform}'.")

    def __init__(self, *args, **kwargs):
        # Mainly for logs and for the instance to be aware
        # that it's being multi-processed. It will be set during the worker
        # init.
        self._worker_id = None

        # Flag to monitor if the data need to be committed.
        # This is useful for multiprocessing.
        self._commit_required = False

        # Check what we want to create. For a placeholder, we don't bother
        # instanciating a consumer as we won't use it anyway.
        # Otherwise, all attributes are passed to the consumer builder.
        if kwargs.get("_is_placeholder", False):
            self._consumer = None
        else:
            if len(args) == 0:
                raise ValueError(
                    "No topic was provided. "
                    "Please use the placeholder() method "
                    "to create a dataset without consumer."
                )

            self._consumer = self.new_consumer(*args, **kwargs)

    def __del__(self):
        self.close()

    def close(self):
        """Close the Kafka consumer without committing the offsets.
        """
        if getattr(self, "_consumer", None) is not None:
            self._consumer.close(autocommit = False)

        self._commit_required = False

    def commit(self, signum = None, stack = None):
        # pylint: disable=unused-argument
        """Commit the offsets of the Kafka consumer. In multiprocessing mode,
        this is called when a POSIX signal is received. 'signum' and 'stack'
        are to be ignored in singleprocessing mode.
        """
        if self._consumer is None:
            raise RuntimeError("Consumer is not initialized.")

        # Committing right away if in main process
        if self._worker_id is None:
            # In main process, committing right away
            self._commit_if_required(force = True)
        # Otherwise, checking we got the right signal and set the flag
        elif signum is not None:
            if signum != self._COMMIT_SIGNAL:
                raise ValueError(
                    f"Worker {self._worker_id} received "
                    f"a bad signal ({signum})."
                )

            self._commit_required = True
        else:
            raise RuntimeError(
                "Direct commit should not be used with multiprocessing."
            )

    def _commit_if_required(self, force: bool = False):
        if not force and not self._commit_required:
            return

        if self._worker_id is None:
            _logger.debug("Committing offsets.")
        else:
            _logger.info("Committing offsets on worker %d.", self._worker_id)

        try:
            self._consumer.commit()
        except CommitFailedError:
            if self._worker_id is None:
                _logger.error("Commit failed.")
            else:
                _logger.error("Commit failed on worker %d.", self._worker_id)
        else:
            if self._worker_id is None:
                _logger.debug("Committed offsets.")
            else:
                _logger.debug(
                    "Committed offsets on worker %d.",
                    self._worker_id
                )
        finally:
            self._commit_required = False

    def __iter__(self):
        if self._consumer is None:
            raise RuntimeError("Consumer is not initialized.")

        # If we are in multi-processing mode, we need to listen for commit
        # signals
        if self._worker_id is not None:
            signal.signal(self._COMMIT_SIGNAL, self.commit)

        for record in self._consumer:
            # Not using walrus here to give the lib a shot to work on Python
            # versions prior 3.8
            data = self._process(record)

            if data is not None:
                yield data

            # Commit in the loop when multiprocessed.
            # This will block the loop and avoid deadlock issues.
            if self._worker_id is not None:
                self._commit_if_required()

        # Resetting the signal stuff once done iterating
        if self._worker_id is not None:
            signal.signal(self._COMMIT_SIGNAL, signal.SIG_DFL)

    def _process(self, record):
        """Define the processing that is applied to each Kafka record.

        Parameters
        ----------
        record : ???
            The Kafka record that you get when iterating the consumer.

        Returns
        -------
        ???
            What you want to output in your batches.
        """
        raise NotImplementedError()

    @classmethod
    def new_consumer(cls, *args, **kwargs):
        """Build a new Kafka consumer. It disables the auto-commit.

        Parameters
        ----------
        All parameters are passed to the KafkaConsumer constructor. See the
        Kafka documentation.
        """
        # Force auto commit to False if we have topics provided
        if len(args) == 0:
            raise ValueError("Cannot create a consumer without topic.")

        kwargs["enable_auto_commit"] = False

        if "_is_placeholder" in kwargs:
            del kwargs["_is_placeholder"]

        return KafkaConsumer(*args, **kwargs)

    @classmethod
    def init_worker(cls, *args, **kwargs):
        """Build an init function to use to init the workers in multiprocessing
        mode.

        Parameters
        ----------
        All parameters are passed to the KafkaConsumer constructor. See the
        Kafka documentation.
        """
        # Define a worker_init_fn that creates a consumer per dataset instance
        def func(worker_id):
            # pylint: disable=no-member,protected-access
            worker_info = get_worker_info()

            if worker_info is None:
                raise RuntimeError(
                    "Custom initialization should be used for multiprocessing "
                    "only."
                )

            dataset = worker_info.dataset
            dataset._consumer = cls.new_consumer(*args, **kwargs)
            dataset._worker_id = worker_id

        return func

    @classmethod
    def commit_worker(cls, worker: mp.Process):
        """Function to use to tell a worker to commit its offets.
        """
        os.kill(worker.pid, cls._COMMIT_SIGNAL)

    @classmethod
    def placeholder(cls):
        """Build a new dataset instance with no Kafka consumer. This should
        be used to build an "empty" dataset before using a multiprocessed
        DataLoader.
        """
        return cls(_is_placeholder = True)
