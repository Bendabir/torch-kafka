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
        self._consumer = self.new_consumer(*args, **kwargs)

    def __del__(self):
        self.close()

    def close(self):
        """Close the Kafka consumer without committing the offsets.
        """
        if getattr(self, "_consumer", None) is not None:
            self._consumer.close(autocommit = False)

    def commit(self, signum = None, stack = None):
        """Commit the offsets of the Kafka consumer. In multiprocessing mode,
        this is called when a POSIX signal is received. 'signum' and 'stack'
        are to be ignored in singleprocessing mode.
        """
        if self._consumer is None:
            raise RuntimeError("Consumer is not initialized.")

        # Committing right away if in main process
        if self._worker_id is None:
            _logger.debug("Committing offsets.")
            self._commit()
        # Otherwise, checking we got the right signal
        elif signum is not None:
            if signum != self._COMMIT_SIGNAL:
                raise ValueError(
                    f"Worker {self._worker_id} received "
                    f"a bad signal ({signum})."
                )

            _logger.debug(f"Committing offsets on worker {self._worker_id}.")
            self._commit()
        else:
            raise RuntimeError(
                "Direct commit should not be used with multiprocessing."
            )

    def _commit(self):
        # The actual commit stuff
        try:
            self._consumer.commit()
        except CommitFailedError as e:
            if e.retriable:
                _logger.error("Commit failed. Retrying.")
                self._commit()
            else:
                _logger.error(
                    "Commit failed and consumer cannot recover. Re-joigning."
                )
                # NOTE : This could also raise error. Perhaps to catch.
                self._consumer.seek_to_end()

    def __iter__(self):
        if self._consumer is None:
            raise RuntimeError("Consumer is not initialized.")

        # If we are in multi-processing mode, we need to listen for commit
        # signals
        if self._worker_id is not None:
            signal.signal(self._COMMIT_SIGNAL, self.commit)

        for record in self._consumer:
            yield self._process(record)

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

    @staticmethod
    def new_consumer(*args, **kwargs):
        """Build a new Kafka consumer. It disables the auto-commit.

        Parameters
        ----------
        All parameters are passed to the KafkaConsumer constructor. See the
        Kafka documentation.
        """
        # Force auto commit to False
        kwargs["enable_auto_commit"] = False

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
        def fn(worker_id):
            worker_info = get_worker_info()

            if worker_info is None:
                raise RuntimeError(
                    "Custom initialization should be used for multiprocessing "
                    "only."
                )

            # pylint: disable=no-member
            dataset = worker_info.dataset
            dataset._consumer = cls.new_consumer(*args, **kwargs)
            dataset._worker_id = worker_id

        return fn

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
        dataset = cls()
        dataset._consumer = None

        return dataset
