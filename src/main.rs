use chrono::NaiveDateTime;
use std::cmp;
use std::env;
use neo4rs::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use futures::stream::*;

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

fn main() {
        let mut _client = get_client();
        let mut epoch_start = _client.get_epoch_info().unwrap();
	let _period = time::Duration::from_millis(5000);
        thread::sleep(_period);
        let mut epoch_end = _client.get_epoch_info().unwrap();
        let mut end_slot = epoch_end.absolute_slot;
        let mut start_slot = epoch_start.absolute_slot;
        while end_slot > start_slot {
            println!("{:?}",epoch_end);
            get_trns(start_slot,end_slot);
            end_slot = start_slot;
            _client = get_client();
            epoch_end = _client.get_epoch_info().unwrap();
            end_slot = epoch_end.absolute_slot;
	}
	
        
}
fn get_trns(start: Slot, end: Slot) {
        let _client = get_client();
        let blocks = _client.get_blocks(start, Some(end)).unwrap();
        for s in blocks {
            let _blk = _client.get_block(s);
	    let blk = match _blk {
		Ok(b) =>b,
		Err(_) => continue,
	    };	
            let dt = NaiveDateTime::from_timestamp(blk.block_time.unwrap(),0);
            println!("block time: {:?}",dt);
            for tn in blk.transactions {
                //println!("------------------------------------------"); 
                let _meta = tn.meta.unwrap();
                let msg = _meta.log_messages.unwrap();
		if show_transfer(&msg) {
		    //println!("{:?},{:?},{:?}",_meta.pre_balances,_meta.post_balances,_meta.status);
		    //println!("{:?},{:?},{:?}",_meta.pre_balances.len(),_meta.post_balances.len(),_meta.status);
		    let _tnx = tn.transaction;
                    let ac = get_tnx_data(_tnx);
		    let delta = get_balance_delta(&_meta.pre_balances,&_meta.post_balances,&ac,&dt);
		    //println!("{:?} of size {:?}",delta, delta.len());
		    //println!("{:?} of size {:?}",ac,ac.len());
		}
            }
	    let ten_millis = time::Duration::from_millis(1000);
            thread::sleep(ten_millis);
        }
}
fn get_balance_delta(pre:&Vec<u64>, post:&Vec<u64>, ac:&Vec<String>, dt:&NaiveDateTime) -> Vec<i64> {
    let mut credit = Vec::new();
    let mut debit = Vec::new();
    let mut delta = Vec::new();
    let n1 = pre.len();
    let n2 = post.len();
    let n3 = ac.len();
    //println!("delta {:?},{:?}",post, pre);
    if n1 == n2 && n2 == n3 {
        for i in 0..n1 {
	    //println!("{},{}",post[i] , pre[i]);
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
                println!("{} -> {} : {},{}",c.ac_key,d.ac_key,amt,fee);
		setup_graph(&c.ac_key,&d.ac_key,amt,dt);
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
			println!("{:?}", s);
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
async fn setup_graph(credit: &str, debit: &str, amt: i64, dt: &NaiveDateTime) {
   let uri = match env::var("DB_HOST") {
                   Ok(v) => v,
                   Err(e) => "localhost:7687".to_string()
   };
   let user = "neo4j";
   let pass = "94077079";
   let id = "1".to_string();

//   println!("get new graph");
   let graph = Arc::new(Graph::new(&uri, user, pass).await.unwrap());
   //println!("create node");
    let mut txn = graph.start_txn().await.unwrap();
    txn.run_queries(vec![
        query("MERGE (a:Account {key: $key})").param("key",credit.to_string()),
        query("MERGE (a:Account {key: $key})").param("key",debit.to_string()),
        query("MATCH (ac1:Account {key: $ckey}),(ac2:Account {key: $dkey}) MERGE (ac1)-[rel:TRANSFER_TO {amount: $amount, datetime: $datetime}]->(ac2)").param("ckey",credit.clone()).param("dkey",debit.clone()).param("amount",amt.clone()).param("datetime",dt.clone() ),
    ])
    .await
    .unwrap();
    txn.commit().await.unwrap(); //or txn.rollback().await.unwrap();
}
