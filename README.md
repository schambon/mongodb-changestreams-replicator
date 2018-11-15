"Replicate" inserts and updates from one MongoDB cluster to another
===================================================================

Uses change streams to get changes from one cluster and apply them to another.

This only gets operations of type "insert" and "update". Anything else will not be applied.

usage: ```python replicator.py --source <source mongodb uri> --target <target mongodb uri>```

Optional argument: --track to give a collection to track resumability.
