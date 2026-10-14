# Week 7 — temporal ML baseline and honest PaySim evaluation

## Reproduce on the 8 GB Apple M3

Run only the `ml` Compose profile; its ARM64 container is capped at 2 GB and
two CPUs. Do not run Spark or Redpanda concurrently. The source is the Week 1
deterministic random sample `data/samples/representative_1m.parquet`, not the
10k fraud-enriched scenario sample. The Week 3 Gold mart has only 100k rows
and 137 frauds, so it is retained as a schema/lineage reference rather than
the model evaluation cohort.

```bash
docker compose --profile ml config --quiet
docker compose --profile ml build ml
docker compose --profile ml run --rm ml python scripts/build_week7_features.py
docker compose --profile ml run --rm ml pytest tests/ml -q
docker compose --profile ml run --rm ml python scripts/train_week7.py
docker compose --profile ml run --rm ml python scripts/evaluate_week7_holdout.py
```

The feature build produced 940,870 rows, 768 frauds, steps 1–408. The source
sample SHA-256 is `a64eaecef34800562cac9d7d7d180106558a8b1deec170600683186ce5d04463`.
The generated feature SHA-256 and all metrics are in the ignored
`artifacts/week7/report.json` file. Generated Parquet, model, MLflow SQLite
database, chart and logs are ignored by Git; they can be regenerated with the
commands above. `Dockerfile.ml` pins the versions, including MLflow 3.16.1
which accepts the project's PyArrow 23.0.1.

## Time and availability contract

| Cohort | PaySim steps | Transactions | Fraud | Use |
|---|---:|---:|---:|---|
| Train | 1–216 | 439,445 | 406 | Fit only |
| Gap 1 | 217–240 | 61,885 | 58 | History only |
| Validation | 241–312 | 188,102 | 130 | Model and threshold selection |
| Gap 2 | 313–336 | 62,536 | 42 | History only |
| Test | 337–408 | 188,902 | 132 | Final evaluation |
| Excluded tail | 409 onward | 59,130 | 568 | Distribution-shift diagnostic |

The gaps illustrate a **hypothetical** 24-hour label maturation delay; PaySim
does not contain real `label_observed_at`. Never claim this is an observed fraud
investigation SLA. Transactions in gaps remain available as prior-event
history. The late tail is excluded because normal transaction traffic drops
sharply while fraud continues, making a naive chronological 60/20/20 split
misleading. These boundaries were chosen after inspecting distribution, so
this is a transparent *portfolio experiment*, not a pre-registered production
validation.

The feature query uses `RANGE BETWEEN 24 PRECEDING AND 1 PRECEDING` on
PaySim's hourly `step`. It therefore excludes the current transaction and
all other transactions in the same hour. This is conservative: true
within-hour ordering is unavailable in the sample. The random 1m sample also
omits most of the source history, so velocity features are incomplete.

### Feature and leakage checklist

| Column | Main model? | Reason |
|---|---|---|
| `transaction_type`, `amount`, event hour | Yes | Known when the transaction arrives |
| Prior customer/recipient/pair counts | Yes | Only strictly earlier hourly steps |
| `oldbalanceOrg`, amount/balance ratio | No | Potentially available, but PaySim almost encodes the label in their equality |
| `isFraud`/`label`, `isFlaggedFraud` | No | Future truth or simulator flag |
| `newbalanceOrig`, `newbalanceDest` | No | Post-transaction values |
| `transaction_id`, raw customer/recipient IDs | No | Audit keys, not model inputs |
| Week 3 Gold rolling counts | No | Existing dbt windows may include the current row |

`oldbalanceOrg` is still used by the offline R001 rule subset and by a
validation-only diagnostic. The production availability of the old balance
must be verified against a real transaction contract before deployment.

## Verified run and interpretation

The primary model is a class-weighted Random Forest selected over a
class-weighted Logistic Regression by validation recall at 100 review
alerts/day, with average precision as tie-breaker. The test was not used by
the script to choose the model, features or threshold. All three test days
receive exactly 100 alerts in the capacity policy.

| Test metric | Safe Random Forest | Offline R001/R003/R004 subset |
|---|---:|---:|
| Average precision (AP) | 0.1301 | 0.0025 |
| Precision at 100 alerts/day | 0.0867 | 0.0200 |
| Recall at 100 alerts/day | 0.1970 | 0.0455 |
| Fraud amount in alerts | 65.53% | 17.54% |
| TP / FP / FN / TN | 26 / 274 / 106 / 188,496 | 6 / 294 / 126 / 188,476 |
| Illustrative cost | 78,005,670 | 186,624,188 |

The cost uses the deliberately optimistic formula `missed fraud amount + 5 ×
review alerts`, with 100% prevention of flagged fraud. PaySim money is
simulated; this is **not** measured financial savings. The rule comparison is
capacity-matched on the same cohort, but it covers only R001/R003/R004; the
Week 5 six-rule worker uses finer event-time state and must not be called
equivalent to this offline subset. Its 999-transaction smoke run has only
two frauds and is not a fair comparison cohort.

A second policy fixes the FPR threshold on validation at `0.9546722331` for
the chosen model and never retunes it on test. On test it yields TP=16,
FP=139, FN=116, TN=188,631; recall=0.1212 and FPR=0.000736. This is
different from the daily top-K capacity policy.

### Critical simulator-artifact finding

On validation, **130/130 fraud** and **0 legitimate transactions** have
`amount == oldbalanceOrg` to within 0.01. A Random Forest allowed to see old
balance reaches AP ~1.0 and recall 1.0 on validation. The earlier exploratory
run also scored the test set with this feature. This is not credible evidence
of real-world generalization, so the committed Week 7 champion deliberately
excludes old balance and its ratio. Because the test set was inspected during
this iteration, the revised test should be described as **exploratory**, not
as a pristine final holdout.

### Fresh row-disjoint temporal holdout

After model and threshold selection were frozen, a separate 100,000-row
holdout was drawn from the original PaySim CSV at steps 337–408. It excludes
every transaction ID in the representative 1m sample: verified overlap is
**zero**. The deterministic holdout seed is `20261015`; it contains 69 frauds.
The script refuses to score it again when its report already exists, because
repeated inspection would remove its status as a fresh holdout. On a clean
clone, the final reproduction command above generates it once.

| Fresh holdout metric | Safe Random Forest | Offline R001/R003/R004 subset |
|---|---:|---:|
| Average precision (AP) | 0.0792 | 0.0025 |
| Precision at 100 alerts/day | 0.0800 | 0.0200 |
| Recall at 100 alerts/day | 0.3478 (24/69) | 0.0870 (6/69) |
| Fraud amount in alerts | 80.78% | 31.49% |
| TP / FP / FN / TN | 24 / 276 / 45 / 99,655 | 6 / 294 / 63 / 99,637 |
| Illustrative cost | 18,027,083 | 64,250,438 |

The validation-selected fixed-FPR threshold yields TP=6, FP=70, FN=63,
TN=99,861 on this holdout (recall 0.0870, FPR 0.000700). The holdout is
later than training and row-disjoint, but comes from the same simulator and
same calendar segment as the exploratory test. Its point-in-time histories
include both the representative sample and new holdout rows, so history
coverage differs from training. These results support a portfolio baseline,
not a real-world deployment claim. A future independent dataset or genuine
forward-time production cohort remains necessary.

The feature-importance JSON is model-internal descriptive importance, not a
causal explanation or SHAP analysis. Customer and pair counts have zero
importance here, consistent with sparse histories in the 1m random sample.

## Verification and handoff

```bash
docker compose --profile ml run --rm ml python -m pip check
docker compose --profile ml run --rm ml ruff check .
docker compose --profile ml run --rm ml ruff format --check \
  scripts/build_week7_features.py scripts/train_week7.py \
  scripts/evaluate_week7_holdout.py tests/ml/test_week7_ml.py
docker compose --profile ml run --rm ml pytest tests/ml -q
docker compose run --rm dev pytest tests/unit -q
```

For the MLflow UI, run `docker compose --profile ml run --rm -p
127.0.0.1:5050:5050 ml mlflow ui --backend-store-uri
sqlite:////workspace/data/ml/week7/mlflow.db --host 0.0.0.0 --port 5050` and
open `http://127.0.0.1:5050`. The `fraud-week7-temporal` experiment records
validation candidates, the exploratory test and the fresh holdout run. The ML image
does not contain the Git executable, so MLflow emits a harmless warning about
automatic Git metadata; source and feature SHA-256 values are in the report.

Week 8 can load `artifacts/week7/champion.joblib` as a *candidate only*, then
add registry/versioning and an independent evaluation cohort before treating
it as a production champion.
