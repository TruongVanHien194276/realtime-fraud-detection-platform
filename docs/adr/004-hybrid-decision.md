# ADR 004: Hard rules plus versioned model

Status: accepted for the separate scoring API.

Context: a PaySim-trained model has simulation-specific shortcuts and can fail or time out. A fraud policy must retain deterministic, explainable decisions.

Decision: hard rules take precedence; the version-pinned MLflow model can elevate review risk. On model unavailability, exception, timeout, or worker saturation, the API returns a rule-only decision and marks `degraded=true` with a reason code.

Alternatives: model-only serving would be simpler but less robust and harder to explain. Rules-only serving omits the model registry and temporal-validation work.

Consequences: deterministic fallback, model-version traceability, and explicit policy thresholds. The local in-process timeout is bounded but cannot forcibly terminate a hung Python thread; production serving would need process isolation or a remote model service with circuit breaking. The streaming rules worker does not call this API.
