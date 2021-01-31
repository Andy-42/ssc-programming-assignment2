This repository contains a project that is a response to the
programming assignment for SSC.

# Problem Statement
Programming Assignment:
The Twitter Streaming API provides real-time access to public tweets.
In this assignment you will build an application that connects to the Streaming API and processes
incoming tweets to compute various statistics.
We'd like to see this as a Scala project, but otherwise feel free to use any libraries you want to
accomplish this task.  The sample endpoint provides a random sample of approximately 1% of the full tweet stream.
Your app should consume this sample stream and keep track of the following:
1] Total number of tweets received
2] Average tweets per hour/minute/second
3] Top emojis in tweets
4] Percent of tweets that contains emojis
5] Top hashtags
6] Percent of tweets that contain a url
7] Percent of tweets that contain a photo url (pic.twitter.com or instagram)
8] Top domains of urls in tweets

The emoji-data project provides a convenient emoji.json file that you can use to determine which emoji unicode characters to look for in the tweet text.  Your app should also provide some way to report these values to a user (periodically log to terminal, return from RESTful web service, etc).

If there are other interesting statistics you’d like to collect, that would be great. There is no need to store this data in a database; keeping everything in-memory is fine. That said, you should think about how you would persist data if that was a requirement.

It’s very important that when your system receives a tweet, you do not block while doing all of the tweet processing.

Twitter regularly sees 5700 tweets/second, so your app may likely receive 57 tweets/second, with higher burst rates. The app should also process tweets as concurrently as possible, to take advantage of all available computing resources. While this system doesn’t need to handle the full tweet stream, you should think about how you could scale up your app to handle such a high volume of tweets.When you're finished, please put your project in a repository on either Github or Bitbucket and send us a link. We'll then do an interview session where we'll have some questions for you about your code and possibly make some additions to it.

# Implementation Overview

The solution is implemented as an FS2 stream.

* The Twitter sample stream is read using http4s.
  This produces an FS2 Stream of io.circe.Json. 
  See: `TWStream.scala`
* The stream of Twitter JSON is mapped through
  `TweetExtract.decode` which produces Stream of `TweetExtract`
  domain objects that extracts the part of the tweet
  that we are interested in (`created_at` and URLS, emoji and hashtags from `text`).
* Using a scan, these `TweetExtract`s are aggregated
  in tumbling windows, and the final `WindowSummaryOutput`
  is emitted downstream as windows expire.
* The sample pipeline simply encodes the output as
  JSON and writes it to the console.

Some notable features:
* There are two overloads of the window aggregation
  `WindowSummary.add`. One aggregates a single `TweetExtract`
  at a time, and one aggregates in chunks (presumably
  this is more efficient).
* The implementations of `WindowSummaries.combineTweet`
  and `WindowSummaries.combineChunkedTweet` have side effects
  (they use the system clock). The modeling of this is incomplete
  since they also use a `com.codahale.metrics.Meter`, which has side
  effects (it uses the system clock), but this is not actually modeled as such.
  
There are a few minor deviations from the original problem statement:
* It does not use the emoji-data project to extract emoji from text.
  The Twitter Text library is used instead.
* The exact set of rate statistics requested in the problem statement were
  not implemented. Instead, I punted on this by using a DropWizard Meter,
  so the hourly rates are not there.
* Reporting the rate statistics in along with the window summaries
  probably doesn't make sense. It is fine for the purse of this example,
  but it probably doesn't make sense from as far as design goes.
  
# Running

Before running the project, you will need to define
environment variables containing the credentials for authenticating with the
sample realtime Twitter API.

```bash
export API_KEY=xxxxxxxxxxxx
export API_KEY_SECRET=xxxxxxxxxxxx
export ACCESS_TOKEN=xxxxxxxxxxxx
export ACCESS_TOKEN_SECRET=xxxxxxxxxxxx
```

For more information about this API and the authentication credential
needed see: https://developer.twitter.com/en/docs/twitter-api/v1/tweets/sample-realtime/overview

The project can be run using:

```bash
sbt run
```

This will run the sample stream implementation that reads from the
Twitter sample API, and will write the generated summary statistics JSON
to the console.

In the default configuration, it will take approximately 15 seconds before
the first summary is written, and then further output will happen approximately
every 5 seconds.

# Sample Output

```json
{
  "windowStart" : "2021-01-31T14:50:45Z",
  "windowEnd" : "2021-01-31T14:50:49.999Z",
  "windowLastUpdate" : "2021-01-31T14:50:55.882Z",
  "totalTweetCount" : 842,
  "tweetCountThisWindow" : 184,
  "oneMinuteRate" : 39.05474750745308,
  "fiveMinuteRate" : 37.26610500223038,
  "fifteenMinuteRate" : 36.95623228626191,
  "topEmojis" : [
    "🏆",
    "☺️",
    "🥺",
    "💜",
    "💙",
    "😉",
    "😭",
    "❗",
    "❤",
    "✨"
  ],
  "topHashtags" : [
    "분명히_더행복할_스물여섯_도영이",
    "OurPreciousDoyoungDay",
    "HAPPYDOYOUNGDAY",
    "주헌",
    "MobileBNK48",
    "EnVen",
    "분명히_더행복할_26살_도영이",
    "오늘의ENHYPEN",
    "세븐틴",
    "오늘의방탄"
  ],
  "topDomains" : [
    "t.co"
  ],
  "tweetsWithEmojiPercent" : 16.304347826086957,
  "tweetsWithUrlPercent" : 28.804347826086957,
  "tweetsWithPhotoUrlPercent" : 28.804347826086957
}
```

# Next Steps

This project was a first attempt at using FS2 and Cats Effect.
I implemented a couple of variations on the stream processing
(e.g., chunked vs. one-at-a-time aggregation). The next round of
learning steps include:
* Support for increasing concurrency in TweetExtract was implemented in the TWStreamApp stream.
  Investigate how changing the level of concurrency scale in terms of throughput.
* Learning about Cats Effect in general. I learned just enough to get by
  in this project, and so now I have a better idea of what I need to learn!