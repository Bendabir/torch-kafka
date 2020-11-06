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
import itertools as it
from torch.utils.data import DataLoader
from src.kafka_dataset import KafkaDataset


def auto_commit(dataloader: DataLoader):
    """Auto-magically commits data from a KafkaDataset.

    Parameters
    ----------
    dataloader : torch.utils.data.DataLoader
        The dataloader to commit the dataset/workers from.
        It must uses a KafkaDataset.

    Returns
    -------
    generator
        An iterator over the DataLoader that will commit the batches as they
        come.

    Raises
    ------
    TypeError
        If the dataloader is not a PyTorch dataloader.
    """
    if not isinstance(dataloader, DataLoader):
        raise TypeError("A DataLoader must be provided.")

    # For "regular" datasets, just iterating as the DataLoader would do
    if not isinstance(dataloader.dataset, KafkaDataset):
        yield from dataloader
    elif dataloader.num_workers == 0:
        # Define how to iter on the data
        # The way we process depends if we are using singleprocessing or
        # multiprocessing.
        # For singleprocessing, it's easy, we just need to commit the
        # dataset every time.
        for batch in dataloader:
            yield batch

            dataloader.dataset.commit()
    else:
        # Workers are actually created when iterated.
        # We need access to the iterator because it saves references to
        # workers and we need them to send commit signals.
        batches = iter(dataloader)

        # Cycling through the workers as the DataLoader
        workers = it.cycle(batches._workers)

        for w, batch in zip(workers, batches):
            yield batch

            # Using the instance to access the class method
            dataloader.dataset.commit_worker(w)
