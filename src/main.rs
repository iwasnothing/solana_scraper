use threadpool::ThreadPool;
use chrono::NaiveDateTime;
use std::cmp;
use std::env;
use neo4rs::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use futures::stream::*;
use futures::executor::block_on;

use std::time::Duration;
use std::result::Result;
use kafka::error::Error as KafkaError;
use kafka::producer::{Producer, Record, RequiredAcks};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use std::{thread, time};
use std::num::Wrapping;
use regex::Regex;
use std::io::Write;
use serde_json::Value;
use solana_client::rpc_client::RpcClient;
use solana_sdk::system_instruction::create_account_with_seed;
use solana_sdk::{
     instruction::Instruction,
     message::Message,
     pubkey::Pubkey,
     signature::{Keypair, Signer},
     transaction::Transaction,
     account::Account,
};
use solana_transaction_status::{EncodedTransaction,UiTransaction,UiMessage};
use solana_transaction_status::parse_accounts::ParsedAccount;
use solana_program::instruction::{AccountMeta};
use solana_program::clock::Slot;
use std::mem::size_of_val;
use borsh::{BorshSerialize, BorshDeserialize};

#[derive(Clone)]
struct AcOp {
    ac_key: String,
    delta: i64,
}
#[derive(Clone)]
struct TransferOp {    
    debit: String,
    credit: String,
    delta: i64,
}

fn get_client() -> RpcClient {
    let _url = "https://solana-api.projectserum.com".to_string();
    return RpcClient::new(_url);
}
fn start_kafka_workers() {
    let pool = ThreadPool::new(1);
    for i in 1..2 {
        pool.execute(move|| {
            pull_transfer_msg(i);
            thread::sleep(Duration::from_millis(100));
        });
    }
}
fn main() {
        let mut _client = get_client();
        let mut epoch_start = _client.get_epoch_info().unwrap();
	let _period = time::Duration::from_millis(10000);
        thread::sleep(_period);
        let mut epoch_end = _client.get_epoch_info().unwrap();
        let mut end_slot = epoch_end.absolute_slot;
        let mut start_slot = epoch_start.absolute_slot;
        start_kafka_workers() ;
        //while end_slot > start_slot {
        loop {
            get_trns(start_slot,end_slot);
            start_slot = end_slot;
            _client = get_client();
            epoch_end = _client.get_epoch_info().unwrap();
            end_slot = epoch_end.absolute_slot;
            thread::sleep(_period);
	}
	
        
}
fn get_trns(start: Slot, end: Slot) {
        println!("{:?}->{:?}",start,end);
        let _client = get_client();
        let blocks = _client.get_blocks(start, Some(end)).unwrap();
        let pool = ThreadPool::new(1);
        for s in blocks {
          pool.execute(move|| {
            println!("get_block {:?}",s);
            let _my_client = get_client();
            let _blk = _my_client.get_block(s);
	    let blk = match _blk {
		Ok(b) =>b,
		Err(_) => std::process::abort(),
	    };	
            let dt = NaiveDateTime::from_timestamp(blk.block_time.unwrap(),0);
            for tn in blk.transactions {
                let _meta = tn.meta.unwrap();
                let msg = _meta.log_messages.unwrap();
		if show_transfer(&msg) {
		    let _tnx = tn.transaction;
                    let ac = get_tnx_data(_tnx);
		    let delta = get_balance_delta(&_meta.pre_balances,&_meta.post_balances,&ac,&dt);
		}
            }
	    let ten_millis = time::Duration::from_millis(1000);
            thread::sleep(ten_millis);
          });
        }
}
fn get_balance_delta(pre:&Vec<u64>, post:&Vec<u64>, ac:&Vec<String>, dt:&NaiveDateTime) -> Vec<i64> {
    let mut credit = Vec::new();
    let mut debit = Vec::new();
    let mut delta = Vec::new();
    let n1 = pre.len();
    let n2 = post.len();
    let n3 = ac.len();
    if n1 == n2 && n2 == n3 {
        for i in 0..n1 {
            let d = post[i] as i64 - pre[i] as i64;
            delta.push(d);
	    let op = AcOp{ ac_key: ac[i].to_string(), delta: d}; 
            if d > 0 {
		credit.push(op.clone());
            }
	    if d < 0 {
		debit.push(op.clone());
	    }
        }
        for c in &credit {
	    for d in &debit {
                let amt = cmp::min(c.delta.abs(),d.delta.abs());
		let fee = (c.delta.abs() - d.delta.abs()).abs();
		publish_transfer_msg(&c.ac_key,&d.ac_key,amt,dt);
	    }
        }
    }
    return delta;
}
fn filter_msg(_list: &Vec<String>) {
	let _ignore_list = [    r".*invalid program agrument.*", 
				r".*Program Vote.*",
			   ];
	let mut score = 0;
	for s in _list {
	    for ign in &_ignore_list {
	        let re = Regex::new(ign).unwrap();
	        if re.is_match(&s) {
		    score += 1;
                }
	    }
            if score == 0 {
		println!("{}",s); 
	    }
	}
}
fn show_log_msg(_list: &Vec<String>) {
	for s in _list {
	        let re = Regex::new(r"^Program log.*").unwrap();
	        if re.is_match(&s) {
			println!("{}", s);
		}
	}
}
fn show_transfer(_list: &Vec<String>) -> bool{
	for s in _list {
	        let re1 = Regex::new(r"^Program log: Instruction: Transfer.*").unwrap();
                let re2 = Regex::new(r"^Program 11111111111111111111111111111111 success.*").unwrap();
	        if re1.is_match(&s) || re2.is_match(&s) {
			return true;
		}
	}
	return false
}
fn get_tnx_data(_tnx: EncodedTransaction) -> Vec<String> {
    match _tnx {
        EncodedTransaction::Json(uitnx) => get_tnx_msg(uitnx.message),
        _ => Vec::new(),
    }
}
fn get_tnx_msg(uitnx: UiMessage) -> Vec<String> {
    match uitnx {
        UiMessage::Raw(msg) => msg.account_keys,
        UiMessage::Parsed(msg) => parsed_ac(&msg.account_keys),
        _ => Vec::new(),
    }
}
fn parsed_ac(ac_keys:&Vec<ParsedAccount>) -> Vec<String> {
    let mut ac = Vec::new();
    for a in ac_keys {
        ac.push(a.pubkey.to_string())
    }
    return ac;
}
#[tokio::main]
async fn setup_graph(credit: &str, debit: &str, amt: i64, dt: &NaiveDateTime) -> Result<(),Error>{
   let uri = match env::var("DB_HOST") {
                   Ok(v) => v,
                   Err(e) => "neo4jdb:7687".to_string()
   };
   let user = "neo4j";
   let pass = "94077079";
   let id = "1".to_string();

   let graph = match Graph::new(&uri, user, pass).await {
                   Ok(g) => Arc::new(g),
                   Err(e) => return Err(e)
   };
   let mut txn = match graph.start_txn().await {
                   Ok(tx) => tx,
                   Err(e) => return Err(e)
   };
   txn.run_queries(vec![
        query("MERGE (a:Account {key: $key})").param("key",credit.to_string()),
        query("MERGE (a:Account {key: $key})").param("key",debit.to_string()),
        query("MATCH (ac1:Account {key: $ckey}),(ac2:Account {key: $dkey}) MERGE (ac1)-[rel:TRANSFER_TO {amount: $amount, datetime: $datetime}]->(ac2)").param("ckey",credit.clone()).param("dkey",debit.clone()).param("amount",amt.clone()).param("datetime",dt.clone() ),
    ])
    .await
    .unwrap();
    txn.commit().await.unwrap(); //or txn.rollback().await.unwrap();
    println!("created relation: {}->{},{},{}",credit,debit,amt,dt);
    return Ok(());
}
fn publish_transfer_msg(credit: &str, debit: &str, amt: i64, dt: &NaiveDateTime) {
    let broker = "kafka:9092";
    let topic = "transfer";
    let _msg_str = format!("{},{},{},{}",credit,debit,amt,dt);
    println!("publish msg: {}",_msg_str);
    let data = _msg_str.as_bytes();
    if let Err(e) = produce_message(data, topic, vec![broker.to_owned()]) {
        println!("Failed producing messages: {}", e);
    }
}
fn pull_transfer_msg(grp: i32) {
    loop {
        let broker = "kafka:9092".to_owned();
        let topic = "transfer".to_owned();
        let group = format!("my-group-{}", grp);
        println!("Thread-{} created to pull kafka msg", group);

        if let Err(e) = consume_messages(group, topic, vec![broker]) {
            println!("Failed consuming messages: {}", e);
        }
        thread::sleep(Duration::from_millis(100));
    }
}
fn produce_message<'a, 'b>(
    data: &'a [u8],
    topic: &'b str,
    brokers: Vec<String>,
) -> Result<(), KafkaError> {

    // ~ create a producer. this is a relatively costly operation, so
    // you'll do this typically once in your application and re-use
    // the instance many times.
    let mut producer = Producer::from_hosts(brokers)
        // ~ give the brokers one second time to ack the message
        .with_ack_timeout(Duration::from_secs(1))
        // ~ require only one broker to ack the message
        .with_required_acks(RequiredAcks::One)
        // ~ build the producer with the above settings
        .create()?;

    // ~ now send a single message.  this is a synchronous/blocking
    // operation.

    // ~ we're sending 'data' as a 'value'. there will be no key
    // associated with the sent message.

    // ~ we leave the partition "unspecified" - this is a negative
    // partition - which causes the producer to find out one on its
    // own using its underlying partitioner.
    producer.send(&Record {
        topic,
        partition: -1,
        key: (),
        value: data,
    })?;

    // ~ we can achieve exactly the same as above in a shorter way with
    // the following call
    producer.send(&Record::from_value(topic, data))?;

    Ok(())
}
fn consume_messages(group: String, topic: String, brokers: Vec<String>) -> Result<(), KafkaError> {
    let mut con = Consumer::from_hosts(brokers)
        .with_topic(topic)
        .with_group(group)
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()?;

    let mss = con.poll()?;
    if mss.is_empty() {
        println!("No messages available right now.");
        return Ok(());
    }
    let pool = ThreadPool::new(1);
    let mut prev_offset:i64 = 0;
    for ms in mss.iter() {
            for m in ms.messages() {
                println!("msg offset: {}",m.offset);
                if m.offset == prev_offset {
                    continue;
                }
                prev_offset = m.offset;
                let _msg_str = String::from_utf8(m.value.to_vec()).expect("Found invalid UTF-8");
                let _msg_clone = _msg_str.clone();
                println!("pulled msg: {}",_msg_clone);
                pool.execute(move|| {
                 let mut done: i32 = 0;
                 while done == 0 {  
                    let v: Vec<&str> = _msg_clone.split(",").collect();
                    let amt_str = v[2].to_string();
                    let amt:i64 = amt_str.parse::<i64>().unwrap();
                    let dt = NaiveDateTime::parse_from_str(v[3], "%Y-%m-%d %H:%M:%S").unwrap();
	            let ret = match setup_graph(&v[0],&v[1],amt,&dt) {
                         Ok(()) => 1,
                         Err(e) => 0
                    };
                    done = ret
                 }
                });
            
            }
            let _ = con.consume_messageset(ms);
    }
    con.commit_consumed()?;
    return Ok(());
}
