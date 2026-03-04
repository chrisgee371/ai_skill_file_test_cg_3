# Traffic taxonomy

This file summarises the campaign and device dimensions that matter most for top-of-funnel analysis.

## Modeling guidance

- Preserve `NULL` UTM values as an explicit traffic class.
- When comparing paid acquisition, segment by device type where appropriate.
- Treat `utm_content` as the ad-content or creative-variant dimension.
- Do not assume the same campaign taxonomy is active across the full history.

## UTM source values


### utm_source

| value | sessions | first_seen | last_seen |
|---|---:|---|---|
| gsearch | 316,035 | 2012-03-19T08:04:16 | 2015-03-19T07:56:29 |
| NULL | 83,328 | 2012-03-25T07:24:18 | 2015-03-19T07:59:08 |
| bsearch | 62,823 | 2012-03-27T22:54:43 | 2015-03-19T07:54:36 |
| socialbook | 10,685 | 2014-01-12T00:38:10 | 2014-12-27T23:50:35 |

### utm_campaign

| value | sessions | first_seen | last_seen |
|---|---:|---|---|
| nonbrand | 337,615 | 2012-03-19T08:04:16 | 2015-03-19T07:56:29 |
| NULL | 83,328 | 2012-03-25T07:24:18 | 2015-03-19T07:59:08 |
| brand | 41,243 | 2012-03-25T23:06:28 | 2015-03-19T07:49:25 |
| desktop_targeted | 5,590 | 2014-08-17T04:55:32 | 2014-12-27T23:50:35 |
| pilot | 5,095 | 2014-01-12T00:38:10 | 2014-03-15T23:47:17 |

### utm_content

| value | sessions | first_seen | last_seen |
|---|---:|---|---|
| g_ad_1 | 282,706 | 2012-03-19T08:04:16 | 2015-03-19T07:56:29 |
| NULL | 83,328 | 2012-03-25T07:24:18 | 2015-03-19T07:59:08 |
| b_ad_1 | 54,909 | 2012-08-19T01:43:26 | 2015-03-19T07:54:36 |
| g_ad_2 | 33,329 | 2012-03-25T23:06:28 | 2015-03-19T07:49:25 |
| b_ad_2 | 7,914 | 2012-03-27T22:54:43 | 2015-03-19T06:28:23 |
| social_ad_2 | 5,590 | 2014-08-17T04:55:32 | 2014-12-27T23:50:35 |
| social_ad_1 | 5,095 | 2014-01-12T00:38:10 | 2014-03-15T23:47:17 |

### device_type

| value | sessions | first_seen | last_seen |
|---|---:|---|---|
| desktop | 327,027 | 2012-03-19T08:16:49 | 2015-03-19T07:56:29 |
| mobile | 145,844 | 2012-03-19T08:04:16 | 2015-03-19T07:59:08 |

### http_referer

| value | sessions | first_seen | last_seen |
|---|---:|---|---|
| https://www.gsearch.com | 351,237 | 2012-03-19T08:04:16 | 2015-03-19T07:56:29 |
| https://www.bsearch.com | 71,032 | 2012-03-27T22:54:43 | 2015-03-19T07:54:36 |
| NULL | 39,917 | 2012-03-25T07:24:18 | 2015-03-19T07:59:08 |
| https://www.socialbook.com | 10,685 | 2014-01-12T00:38:10 | 2014-12-27T23:50:35 |
