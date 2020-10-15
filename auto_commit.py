import itertools as it
from torch.utils.data import DataLoader
from kafka_dataset import KafkaDataset


class _AutoCommitterIterator:
    """Define a wrapper on top of a PyTorch DataLoader to auto-magically
    commits data from a KafkaDataset.

    Parameters
    ----------
    dataloader : torch.utils.data.DataLoader
        The dataloader to commit the dataset/workers from.
        It must uses a KafkaDataset.
    """
    def __init__(self, dataloader: DataLoader):
        if not isinstance(dataloader, DataLoader):
            raise TypeError("A DataLoader must be provided.")

        if not issubclass(dataloader.dataset.__class__, KafkaDataset):
            raise TypeError("The DataLoader must iterate over a KafkaDataset.")

        self.dataloader = dataloader
        self._dataset_class = dataloader.dataset.__class__

    def __iter__(self):
        # Define how to iter on the data
        # The way we process depends if we are using singleprocessing or
        # multiprocessing.
        if self.dataloader.num_workers == 0:
            # For singleprocessing, it's easy, we just need to commit the
            # dataset every time.
            for batch in self.dataloader:
                yield batch

                self.dataloader.dataset.commit()
        else:
            # Workers are actually created when iterated.
            # We need access to the iterator because it saves references to
            # workers and we need them to send commit signals.
            batches = iter(self.dataloader)

            # Cycling through the workers as the DataLoader
            workers = it.cycle(batches._workers)

            for w, batch in zip(workers, batches):
                yield batch

                self._dataset_class.commit_worker(w)


def auto_commit(dataloader: DataLoader):
    """Auto-magically commits data from a KafkaDataset.

    Parameters
    ----------
    dataloader : torch.utils.data.DataLoader
        The dataloader to commit the dataset/workers from.
        It must uses a KafkaDataset.

    Returns
    -------
    iterator
        An iterator over the DataLoader that will commit the batches as they
        come.
    """
    return _AutoCommitterIterator(dataloader)
