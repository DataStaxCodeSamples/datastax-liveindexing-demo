package com.datastax.creditcard;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.creditcard.dao.CreditCardDao;
import com.datastax.creditcard.model.Transaction;
import com.datastax.creditcard.model.Transaction.Status;
import com.datastax.demo.utils.PropertyHelper;

public class TransactionCreator {

	private static final int DEFAULT_DAY_IN_MILLIS = 86400000;
	private static final int DEFAULT_SLEEP = 2000;
	private static Logger logger = LoggerFactory.getLogger(TransactionCreator.class);
	private static int BATCH = 25000;
	
	private DecimalFormat creditCardFormatter = new DecimalFormat("0000000000000000");
	private static int noOfUsers = 10000000;
	private static int noOfIssuers = 5000000;
	private static int noOfLocations = 10000;
	private int RANDOM = 200;
	private CreditCardDao dao;
	
	
	public TransactionCreator() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String noOfTransactionsPerDayStr = PropertyHelper.getProperty("noOfTransactionsPerDay", "500000");
		
		int noOfTransactionsPerDay = Integer.parseInt(noOfTransactionsPerDayStr);

		dao = new CreditCardDao(contactPointsStr.split(","));
		
		getRandom(noOfTransactionsPerDay);
		
	}

	private void getRandom(int noOfTransactionsPerDay) {
		
		double divisor = noOfTransactionsPerDay * (Math.random() + .5);
		
		RANDOM = new Double(86400000/divisor).intValue();
	}

	private void sleep(int i) {
		try {
			Thread.sleep(i);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private Transaction createRandomTransaction(Date date) {

		int creditCardNo = new Double(Math.ceil(Math.random() * noOfUsers)).intValue();
		int noOfItems = new Double(Math.ceil(Math.random() * 5)).intValue();
		
		int issuerNo = new Double(Math.random() * noOfIssuers).intValue();
		int locationNo = new Double(Math.random() * noOfLocations).intValue();
				
		String issuerId;
		String location;
		
		if (issuerNo < issuers.size()){
			issuerId = issuers.get(issuerNo);
		}else{
			issuerId = "Issuer" + (issuerNo + 1);
		}
		
		if (locationNo < locations.size()){
			location = locations.get(locationNo);
		}else{
			location = "City-" + (locationNo + 1);
		}
		
		Transaction transaction = new Transaction();
		createItemsAndAmount(noOfItems, transaction);
		transaction.setCreditCardNo(creditCardFormatter.format(creditCardNo));
		transaction.setMerchant(issuerId);
		transaction.setTransactionId(UUID.randomUUID().toString());
		transaction.setTransactionTime(date);
		transaction.setLocation(location);
		transaction.setUserId(creditCardNo + "");
		transaction.setStatus(Status.APPROVED.toString());

		return transaction;
	}

	private void createItemsAndAmount(int noOfItems, Transaction transaction) {
		Map<String, Double> items = new HashMap<String, Double>();
		double totalAmount = 0;

		for (int i = 0; i < noOfItems; i++) {

			double amount = new Double(Math.random() * 1000);
			items.put("item" + i, amount);

			totalAmount += amount;
		}
		transaction.setAmount(totalAmount);
		transaction.setItems(items);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new TransactionCreator();
	}

	private List<String> locations = Arrays.asList("London", "Manchester", "Liverpool", "Glasgow", "Dundee",
			"Birmingham", "New York", "Chicago", "Denver", "Los Angeles", "San Jose", "Santa Clara", "San Fransisco");

	private List<String> issuers = Arrays.asList("Tesco", "Sainsbury", "Asda Wal-Mart Stores", "Morrisons",
			"Marks & Spencer", "Boots", "John Lewis", "Waitrose", "Argos", "Co-op", "Currys", "PC World", "B&Q",
			"Somerfield", "Next", "Spar", "Amazon", "Costa", "Starbucks", "BestBuy", "Wickes", "TFL", "National Rail",
			"Pizza Hut", "Local Pub");	
}
