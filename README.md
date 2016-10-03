# BatchingSpinQueue #

A very simple implementation of a blocking queue which will squash all
submitted items into batches of a maximum size. 

On submit, a CompletableFuture<T> is returned, which is completed after
the batch consumer has completed it's work, or after the specified timeout
has passed. 

