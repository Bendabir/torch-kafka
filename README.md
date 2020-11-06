# PyTorch Dataset for Kafka

This repository provides some code sample to build a PyTorch dataset to stream data from Kafka. This is especially useful when you need to commit each batch of data explicitly.

This hasn't yet been released to PyPI but I might take time to do it.

## Installation

This has been developped and tested for Python 3.8 with PyTorch 1.6.0 and kafka-python 2.0.2 on Ubuntu 18.04. This should work on more recent versions, and probably on older releases as well but I haven't tried it. I'm not sure the multiprocessing part would work on Mac OS or Windows but if someone is willing to test it, feel free to give me feedbacks !

Of course, best is to use a virtual env.

```bash
virtualenv -p python3.8 env
source env/bin/activate

pip install -r requirements.txt
```

## Usage

This has been designed to handle both singleprocessing and multiprocessing through the PyTorch DataLoader class. Below are two examples, for both setups.

For every example, we'll use this class that implements a concrete data processing on the data fetched from Kafka.

```python
import torch
from torchkafka import KafkaDataset

class MyDataset(KafkaDataset):
    def _process(self, record):
        # Do something here. For example :
        return torch.rand(8)
```

### Regular usage (singleprocessing)

For singleprocessing, this is quite straight forward. We just need to build a dataset and a dataloader as usual, and then use a little helper to auto-commit the batches for us. The batch size is just given as an example.

```python
from torch.utils.data import DataLoader
from torchkafka import auto_commit

# We can define some other settings of the consumer here
# (max_poll_records, session_timeout_ms, etc.).
dataset = MyDataset(
    "topic",
    group_id = "group_1",
    bootstrap_servers = ["localhost:9092"]
)
dataloader = DataLoader(dataset, batch_size = 4)

for batch in auto_commit(dataloader):
    # DO STUFF
    pass
```

### Multiple workers (multiprocessing)

For multiprocessing, it requires some more tweaks (but not that much, don't worry). The batch size is just given as an example.

```python
from torch.utils.data import DataLoader
from torchkafka import auto_commit

# Define a placeholder dataset (without any Kafka consumer).
# Consumers will be initialized by the dataloader.
dataset = MyDataset.placeholder()
dataloader = DataLoader(
    dataset,
    batch_size = 4,
    # The number of workers should be >= 1 to enable multiprocessing.
    num_workers = 2,
    # We can define some other settings of the consumer here
    # (max_poll_records, session_timeout_ms, etc.).
    worker_init_fn = MyDataset.init_worker(
        "topic",
        group_id = "group_1",
        bootstrap_servers = ["localhost:9092"]
    )
)

for batch in auto_commit(dataloader):
    # DO STUFF
    pass
```

## Licence

Please observe the GNU General Public License v3.0 license that is listed in this repository.
