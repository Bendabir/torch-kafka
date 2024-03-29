# PyTorch Dataset for Kafka

This repository provides some code sample to build a PyTorch dataset to stream data from Kafka. This is especially useful when you need to commit each batch of data explicitly.

This hasn't yet been released to PyPI but I might take time to do it.

## Installation

This has been developped and tested for Python 3.8 with PyTorch 1.6.0 and kafka-python 2.0.2 on Ubuntu 18.04. This should work on more recent versions, and probably on older releases as well but I haven't tried it. I'm not sure the multiprocessing part would work on Mac OS or Windows but if someone is willing to test it, feel free to give me feedbacks !

Of course, best is to use a virtual env.

```bash
virtualenv -p python3.8 env
source env/bin/activate

pip install .
```

You can also install it directly from GitHub with one of the two following options.

```bash
# This first option is likely to be faster to download
pip install https://github.com/bendabir/torch-kafka/archive/1.2.0.tar.gz

# Probably slower because of the git clone
pip install git+https://github.com/bendabir/torch-kafka@1.2.0
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

In some cases, you might also want to override the consumer instanciation method (to force some parameters for instance). For example :

```python
class MyDataset(KafkaDataset):
    # ...

    @classmethod
    def new_consumer(cls, *args, **kwargs):
        kwargs["value_deserializer"] = json.loads

        return super(cls, cls).new_consumer(*args, **kwargs)
```

If you need to add some attributes to your dataset, you can override both the `__init__` and `placeholder` methods. If the `_process` method returns `None`, then the element is skipped by the dataset.

```python
class MyDataset(KafkaDataset):
    def __init__(self, min_size: int, *args, **kwargs):
        self.min_size = min_size

        super().__init__(*args, **kwargs)

    @classmethod
    def placeholder(cls, min_size: int):
        return cls(min_size, _is_placeholder = True)

    def _process(self, record):
        # Do something here with the attribute
        elements: list = json.loads(record.value)

        if len(elements) < self.min_size:
            return None

        return elements
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
