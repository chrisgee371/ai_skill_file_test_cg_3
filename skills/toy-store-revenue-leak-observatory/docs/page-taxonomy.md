# Page taxonomy

The page catalog below should guide session-path logic and experiment handling.

## Page families

- landing: `/home`, `/lander-1` to `/lander-5`
- catalog: `/products`
- product_detail: product-specific pages
- cart: `/cart`
- shipping: `/shipping`
- billing: `/billing`, `/billing-2`
- confirmation: `/thank-you-for-your-order`

## Practical modeling guidance

- Entry-page analysis should work from the first pageview in each session.
- Funnel analysis should use ordered page sequence, not unordered existence checks.
- Checkout variant analysis should treat `/billing` and `/billing-2` as a variant family.
- Product-page analysis should bridge product detail pages back to `dim__products`.

## Page catalog

| pageview_url | category | variant_family | first_seen | last_seen | pageviews |
|---|---|---|---|---|---:|
| /home | landing | landing_home | 2012-03-19T08:04:16 | 2015-03-19T07:59:08 | 137,576 |
| /products | catalog | catalog | 2012-03-19T09:10:08 | 2015-03-19T07:57:22 | 261,231 |
| /the-original-mr-fuzzy | product_detail | product_detail | 2012-03-19T09:10:52 | 2015-03-19T07:59:32 | 162,525 |
| /cart | cart | cart | 2012-03-19T09:14:02 | 2015-03-19T07:55:03 | 94,953 |
| /shipping | shipping | shipping | 2012-03-19T09:16:52 | 2015-03-19T07:57:32 | 64,484 |
| /billing | billing | billing_v1 | 2012-03-19T09:19:52 | 2013-01-05T20:53:22 | 3,617 |
| /thank-you-for-your-order | confirmation | confirmation | 2012-03-19T10:42:46 | 2015-03-19T05:38:31 | 32,313 |
| /lander-1 | landing | landing_test | 2012-06-19T00:35:54 | 2013-03-10T23:10:57 | 47,574 |
| /billing-2 | billing | billing_v2 | 2012-09-10T00:13:05 | 2015-03-19T07:59:07 | 48,441 |
| /the-forever-love-bear | product_detail | product_detail | 2013-01-06T13:27:48 | 2015-03-19T07:38:35 | 26,033 |
| /lander-2 | landing | landing_test | 2013-01-14T00:28:28 | 2014-12-27T23:50:35 | 131,170 |
| /lander-3 | landing | landing_test | 2013-07-09T00:51:33 | 2015-03-19T07:55:40 | 79,000 |
| /the-birthday-sugar-panda | product_detail | product_detail | 2013-12-12T13:06:48 | 2015-03-19T07:51:57 | 19,046 |
| /lander-4 | landing | landing_test | 2014-02-02T00:29:20 | 2014-04-19T23:47:17 | 9,385 |
| /lander-5 | landing | landing_test | 2014-08-02T00:22:29 | 2015-03-19T07:56:29 | 68,166 |
| /the-hudson-river-mini-bear | product_detail | product_detail | 2014-12-05T14:28:49 | 2015-03-19T05:24:16 | 2,610 |
