# Known quirks and constraints

Read this before building experiment and launch logic.

## 1. Chronology matters

Page variants and products enter the dataset over time. Do not compare variants across the full history unless they genuinely coexist in the same comparison window.

Examples:
- `/billing` first appears on 2012-03-19T09:19:52 and disappears on 2013-01-05T20:53:22
- `/billing-2` first appears on 2012-09-10T00:13:05 and continues through 2015-03-19T07:59:07
- `/lander-5` only appears from 2014-08-02T00:22:29

## 2. Refunds extend beyond the order activity window

Orders end on 2015-03-19T05:38:31, but refunds continue through 2015-04-01T18:11:08.
Net-value logic must not truncate refunds away.

## 3. UTM nulls are meaningful

`utm_source`, `utm_campaign`, and `utm_content` contain null values that represent direct or otherwise untagged sessions. Treat these as a valid traffic class unless there is an explicit reason to exclude them.

## 4. Product scope is intentionally small

There are only four products. Keep product logic focused on launch timing, bundle attachment, product-day economics, and refund behavior. Do not invent a deep merchandising taxonomy.

## 5. Product naming has a mild inconsistency

Product 4 is named `The Hudson River Mini bear` in the raw product file. Preserve raw values in source models. If you canonicalize names in a dimension layer, document the choice.

## 6. Reconciliation is unusually clean

In the supplied data:
- order totals reconcile exactly to item totals
- item counts reconcile exactly to `items_purchased`
- source-to-child foreign keys are orphan-free

That is helpful, but still validate the contract in the generated pipeline.
