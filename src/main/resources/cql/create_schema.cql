create keyspace if not exists datastax_liveindexing_demo WITH replication = {'class': 'NetworkTopologyStrategy', 'SearchAnalytics': 1};

use datastax_liveindexing_demo; 
 
CREATE TABLE if not exists credit_card_transactions_balance (
	cc_no text,
	transaction_time timestamp,
	transaction_id text,
	amount double,
	balance_at timestamp static,
	balance double static,
	PRIMARY KEY (cc_no, transaction_time )
) WITH CLUSTERING ORDER BY (transaction_time DESC)
AND compaction = {'class': 'DateTieredCompactionStrategy'};


create table if not exists transactions(
	cc_no text,
	transaction_time timestamp,
 	transaction_id text,
 	user_id text,
	location text,
	items map<text, double>,
	merchant text,
	amount double,
	status text,
	notes text,
	PRIMARY KEY (transaction_id)
) WITH compaction = {'class': 'DateTieredCompactionStrategy'};

create table if not exists latest_transactions(
	cc_no text,
	transaction_time timestamp,
 	transaction_id text,
 	user_id text,
	location text,
	items map<text, double>,
	merchant text,
	amount double,
	status text,
	notes text,
	PRIMARY KEY (cc_no, transaction_time)
) WITH compaction = {'class': 'DateTieredCompactionStrategy'};

     
create table if not exists users(
	user_id text PRIMARY KEY,
	first text,
	last text,
	gender text,
	city text,
	state text,
	cc_no text,
	email text
) WITH compaction= { 'class': 'MemoryOnlyStrategy' }
     AND compression = {'sstable_compression' : ''}
     AND caching = {'keys':'NONE', 'rows_per_partition':'NONE'};
    
