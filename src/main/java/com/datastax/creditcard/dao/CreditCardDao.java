package com.datastax.creditcard.dao;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.creditcard.model.Transaction;
import com.datastax.creditcard.model.User;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

public class CreditCardDao {

	private static Logger logger = LoggerFactory.getLogger(CreditCardDao.class);
	private static final int DEFAULT_LIMIT = 10000;
	private Session session;

	private DateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
	private static String keyspaceName = "datastax_liveindexing_demo";

	private static String transactionTable = keyspaceName + ".transactions";
	private static String latestTransactionTable = keyspaceName + ".latest_transactions";
	private static String userTable = keyspaceName + ".users";

	private static final String INSERT_INTO_TRANSACTION = "Insert into " + transactionTable
			+ " (cc_no, transaction_time, transaction_id, items, location, merchant, amount, user_id, status, notes) values (?,?,?,?,?,?,?,?,?,?);";

	private static final String GET_TRANSACTIONS_BY_ID = "select * from "
			+ transactionTable + " where transaction_id = ?";
	private static final String GET_TRANSACTIONS_BY_CCNO = "select * from "
			+ latestTransactionTable + " where cc_no = ? order by transaction_time desc";

	private static final String GET_USER = "Select * from " + userTable + " where user_id = ?";
	
	private PreparedStatement insertTransactionStmt;
	private PreparedStatement getTransactionById;
	private PreparedStatement getTransactionByCCno;
	
		public CreditCardDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder()				
				.addContactPoints(contactPoints)
				.build();

		this.session = cluster.connect();
		
		try {
			this.insertTransactionStmt = session.prepare(INSERT_INTO_TRANSACTION);
			
			
			this.getTransactionById = session.prepare(GET_TRANSACTIONS_BY_ID);
			this.getTransactionByCCno = session.prepare(GET_TRANSACTIONS_BY_CCNO);

			this.insertTransactionStmt.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			
			
		} catch (Exception e) {
			e.printStackTrace();
			session.close();
			cluster.close();
		}
	}

	public void saveTransaction(Transaction transaction) {
		insertTransaction(transaction);
	}	

	public void insertTransaction(Transaction transaction) {

		session.execute(this.insertTransactionStmt.bind(transaction.getCreditCardNo(), transaction.getTransactionTime(),
				transaction.getTransactionId(), transaction.getItems(), transaction.getLocation(),
				transaction.getMerchant(), transaction.getAmount(), transaction.getUserId(), transaction.getStatus(), transaction.getNotes()));
	}

	private User rowToUser(Row row) {
		User user = new User();
		user.setUserId(row.getString("user_id"));
		user.setCityName(row.getString("city"));
		user.setCreditCardNo(row.getString("cc_no"));
		user.setFirstname(row.getString("first"));
		user.setGender(row.getString("gender"));
		user.setLastname(row.getString("last"));
		user.setStateName(row.getString("state"));
	
		return user;
	}

	public Transaction getTransaction(String transactionId) {

		ResultSet rs = this.session.execute(this.getTransactionById.bind(transactionId));
		
		Row row = rs.one();
		if (row == null){
			throw new RuntimeException("Error - no transaction for id:" + transactionId);			
		}

		return rowToTransaction(row);		
	}

	private Transaction rowToTransaction(Row row) {

		Transaction t = new Transaction();
		
		t.setAmount(row.getDouble("amount"));
		t.setCreditCardNo(row.getString("cc_no"));
		t.setMerchant(row.getString("merchant"));
		t.setItems(row.getMap("items", String.class, Double.class));
		t.setLocation(row.getString("location"));
		t.setTransactionId(row.getString("transaction_id"));
		t.setTransactionTime(row.getDate("transaction_time"));
		t.setUserId(row.getString("user_id"));
		t.setNotes(row.getString("notes"));
		t.setStatus(row.getString("status"));

		return t;
	}
	public Transaction getTransactions(String transactionId) {

		logger.info("Getting transaction :" + transactionId);
		
		ResultSet resultSet = this.session.execute(getTransactionById.bind(transactionId));		
		Row row = resultSet.one();

		if (row == null){
			throw new RuntimeException("Error - no issuer for id:" + transactionId);			
		}
		return rowToTransaction(row);
	}
	
	
	public List<Transaction> getLatestTransactionsForCCNo(String ccNo) {		
		ResultSet resultSet = this.session.execute(getTransactionByCCno.bind(ccNo));		
		List<Row> rows = resultSet.all();
		
		List<Transaction> transactions = new ArrayList<Transaction>();

		for (Row row : rows){
			transactions.add(rowToTransaction(row));
		}
		
		return transactions;
	}
	
	public List<Transaction> getTransactions(List<String> transactionIds) {

		List<Transaction> transactions = new ArrayList<Transaction>();
		List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();

		for (String transactionId : transactionIds) {

			results.add(this.session.executeAsync(getTransactionById.bind(transactionId)));
		}

		for (ResultSetFuture future : results) {
			transactions.add(this.rowToTransaction(future.getUninterruptibly().one()));
		}

		return transactions;
	}


	public Map<String,String> getCreditCardUserIdMap() {
		
		Statement stmt = new SimpleStatement ("select cc_no, user_id from " + userTable);
		stmt.setFetchSize(DEFAULT_LIMIT);
		ResultSet rs = this.session.execute(stmt);		
		Iterator<Row> iter = rs.iterator();
		
		Map<String,String> ccNoUserIdMap = new HashMap<String, String>();
		
		while (iter.hasNext()){
			
			Row row = iter.next();
			ccNoUserIdMap.put(row.getString("cc_no"), row.getString("user_id"));
		}
		
		return ccNoUserIdMap;
	}
	
	public List<String> getCreditCardNosIter() {
		
		Statement stmt = new SimpleStatement ("select * from " + userTable);
		ResultSet rs = this.session.execute(stmt);		
		Iterator<Row> iter = rs.iterator();
		
		List<String> creditCardNos = new ArrayList<String>();
		
		int count=0;
		
		while (iter.hasNext()){
			
			Row row = iter.next();
			creditCardNos.add(row.getString("cc_no"));
			count++;
		}
		
		logger.info("Count (iter): " + count);
		return creditCardNos;
	}
	
	public Transaction getTransactionByCCNoSearch(String ccno){
		
		String cql = "Select * from " + transactionTable + " where solr_query='{\"q\":\"cc_no:"+ ccno +"\"}'";
		ResultSet resultSet = session.execute(cql);
		
		if (resultSet.isExhausted()){
			return null;			
		}else{
			return this.rowToTransaction(resultSet.one());
		}		
	}
}
