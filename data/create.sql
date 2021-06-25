CREATE TABLE "comments" ("id" text NOT NULL,"score" bigint,"created" real NOT NULL,"body" text,"author" text NOT NULL,"upvote" bigint,"downvote" bigint,"comment_type" text,"name" text,"parent_id" text,"post_id" text,"timestamp" datetime NOT NULL, "tickers" blob FIRST, PRIMARY KEY (id));

CREATE TABLE "posts" ("id" text NOT NULL,"title" text NOT NULL,"score" bigint NOT NULL,"url" text NOT NULL,"comms_num" bigint,"created" real,"timestamp" datetime,"body" text,"upvote_ratio" real,"author" text,"is_original_content" text FIRST, "tickers" blob FIRST, "subreddit" text FIRST, PRIMARY KEY (id));

CREATE TABLE "tickers" ("ticker" text, PRIMARY KEY (ticker));

CREATE TABLE "tickers_timeseries" ("id" text NOT NULL DEFAULT NULL,"day" datetime NOT NULL,"source" text NOT NULL,"blob" blob, PRIMARY KEY (id));

