# Model

A Model is the core atomic building block. It specifies a machine learning artifact that will be loaded onto one of the running Servers. A model could be a standard machine learning inference component such as

 * a Tensorflow model, PyTorch model or SKLearn model.
 * an inference transformation component such as a SKLearn pipeline or a piece of custom python logic.
 a monitoring component such as an outlier detector or drift detector.
 * An alibi-explain model explainer

An example is shown below for a SKLearn model for iris classification:

```{literalinclude} ../../../../../../samples/models/sklearn-iris-gs.yaml 
:language: yaml
```

Its Kubernetes `spec` has two core requirements

 * A `storageUri` specifying the location of the artifact. This can be any rclone URI specification.
 * A `requirements` list which provides tags that need to be matched by the Server that can run this artifact type. By default when you install Seldon we provide a set of Servers that cover a range of artifact types.

## Model Versioning and Rollouts

When a `Model` is updated the operator and scheduler cooperate to avoid dropping the version that
is currently serving inference traffic before the new version is ready.

### How versions are created

Each load of a model carries the Kubernetes `generation` of the `Model` resource. The scheduler
uses it to decide whether an update needs a new model version:

 * A change to the model definition itself, for example `storageUri`, `artifactVersion`,
   `requirements`, `modelType`, `parameters`, the explainer/LLM spec or logging, creates a **new
   model version**. Both versions exist in the scheduler until the rollout completes.
 * A change that only affects deployment settings, such as `replicas`, `minReplicas` or
   `maxReplicas`, updates the **current version in place** and does not create an extra version.

### Old versions are kept until the new version is ready

The previous model version keeps serving while the new version is being loaded. A version is only
eligible for cleanup once the latest version reports `ModelAvailable`, and even then an earlier
version that still has live (traffic-serving) replicas is retained. Cleanup is retried on every
subsequent control-plane sync, so the old version is unloaded automatically as soon as the new one
has fully taken over. This matters in particular when a version switch, an unready Server pod and a
configuration update happen close together: the old version is never removed while it is the only
one able to receive traffic.

The model gateway state is cleaned up independently: once the latest version is gateway-available,
older gateway versions are terminated even if their inference replicas are still being drained.

### Deletion while a rollout is in progress

A `Model` carries the `seldon.model.finalizer` finalizer so that deletion waits for the scheduler
to unload it. If the `Model` is deleted while a new version is still rolling out, the operator
defers the unload instead of racing the in-flight rollout. A rollout is considered in progress when
the `ModelReady` condition is not `True` but the model already reports `availableReplicas` greater
than zero. In that state the controller:

 * keeps the finalizer and the previous version serving,
 * emits a `ModelRolloutInProgress` event,
 * requeues reconciliation (every 10 seconds by default) until the rollout settles.

When the model is not in that state the unload is sent immediately. The finalizer is only removed
once the scheduler reports the model (and its model gateway) as terminated, or when the scheduler
returns a non-retryable unload error.

### Stale status events are ignored

The scheduler streams status for the latest version only. Events that reference an older
Kubernetes `generation` than the one currently on the `Model` resource are ignored, so a stale
`ModelAvailable` event from a previous version can never overwrite the progressing state of the
version currently being rolled out.

