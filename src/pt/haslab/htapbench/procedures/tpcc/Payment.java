
/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************
/*
 * Copyright 2017 by INESC TEC                                                                                                
 * This work was based on the OLTPBenchmark Project                          
 *
 * Licensed under the Apache License, Version 2.0 (the "License");           
 * you may not use this file except in compliance with the License.          
 * You may obtain a copy of the License at                                   
 *
 * http://www.apache.org/licenses/LICENSE-2.0                              
 *
 * Unless required by applicable law or agreed to in writing, software       
 * distributed under the License is distributed on an "AS IS" BASIS,         
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  
 * See the License for the specific language governing permissions and       
 * limitations under the License. 
 */
package pt.haslab.htapbench.procedures.tpcc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Random;

import org.apache.log4j.Logger;
import pt.haslab.htapbench.api.SQLStmt;
import pt.haslab.htapbench.benchmark.HTAPBConstants;
import pt.haslab.htapbench.util.TPCCUtil;
import pt.haslab.htapbench.core.TPCCWorker;
import pt.haslab.htapbench.configuration.loader.pojo.Customer;

public class Payment extends TPCCProcedure {

	private static final Logger LOG = Logger.getLogger(Payment.class);

	private SQLStmt payUpdateWhseSQL = new SQLStmt("UPDATE " + HTAPBConstants.TABLENAME_WAREHOUSE + " SET W_YTD = W_YTD + ?  WHERE W_ID = ? ");
	private SQLStmt payGetWhseSQL = new SQLStmt("SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME"
			+ " FROM " + HTAPBConstants.TABLENAME_WAREHOUSE + " WHERE W_ID = ?");
	private SQLStmt payUpdateDistSQL = new SQLStmt("UPDATE " + HTAPBConstants.TABLENAME_DISTRICT + " SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?");
	private SQLStmt payGetDistSQL = new SQLStmt("SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME"
			+ " FROM " + HTAPBConstants.TABLENAME_DISTRICT + " WHERE D_W_ID = ? AND D_ID = ?");
	private SQLStmt payGetCustSQL = new SQLStmt("SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, "
			+ "C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, "
			+ "C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + HTAPBConstants.TABLENAME_CUSTOMER + " WHERE "
			+ "C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
	private SQLStmt payGetCustCdataSQL = new SQLStmt("SELECT C_DATA FROM " + HTAPBConstants.TABLENAME_CUSTOMER + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
	private SQLStmt payUpdateCustBalCdataSQL = new SQLStmt("UPDATE " + HTAPBConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = ?, C_YTD_PAYMENT = ?, "
			+ "C_PAYMENT_CNT = ?, C_DATA = ? "
			+ "WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
	private SQLStmt payUpdateCustBalSQL = new SQLStmt("UPDATE " + HTAPBConstants.TABLENAME_CUSTOMER + " SET C_BALANCE = ?, C_YTD_PAYMENT = ?, "
			+ "C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");
	private SQLStmt payInsertHistSQL = new SQLStmt("INSERT INTO " + HTAPBConstants.TABLENAME_HISTORY + " (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) "
			+ " VALUES (?,?,?,?,?,?,?,?)");
	private SQLStmt customerByNameSQL = new SQLStmt("SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, "
			+ "C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, "
			+ "C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + HTAPBConstants.TABLENAME_CUSTOMER + " "
			+ "WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST");

	// Payment Txn
	private PreparedStatement payUpdateWhse = null;
	private PreparedStatement payGetWhse = null;
	private PreparedStatement payUpdateDist = null;
	private PreparedStatement payGetDist = null;
	private PreparedStatement payGetCust = null;
	private PreparedStatement payGetCustCdata = null;
	private PreparedStatement payUpdateCustBalCdata = null;
	private PreparedStatement payUpdateCustBal = null;
	private PreparedStatement payInsertHist = null;
	private PreparedStatement customerByName = null;

	@Override
	public ResultSet run(Connection conn, Random gen,
						 int terminalWarehouseID, int numWarehouses,
						 int terminalDistrictLowerID, int terminalDistrictUpperID,
						 TPCCWorker w) throws SQLException {

		//initializing all prepared statements
		payUpdateWhse = this.getPreparedStatement(conn, payUpdateWhseSQL);
		payGetWhse = this.getPreparedStatement(conn, payGetWhseSQL);
		payUpdateDist = this.getPreparedStatement(conn, payUpdateDistSQL);
		payGetDist = this.getPreparedStatement(conn, payGetDistSQL);
		payGetCust = this.getPreparedStatement(conn, payGetCustSQL);
		payGetCustCdata = this.getPreparedStatement(conn, payGetCustCdataSQL);
		payUpdateCustBalCdata = this.getPreparedStatement(conn, payUpdateCustBalCdataSQL);
		payUpdateCustBal = this.getPreparedStatement(conn, payUpdateCustBalSQL);
		payInsertHist = this.getPreparedStatement(conn, payInsertHistSQL);
		customerByName = this.getPreparedStatement(conn, customerByNameSQL);
		payUpdateWhse = this.getPreparedStatement(conn, payUpdateWhseSQL);

		int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);

		int x = TPCCUtil.randomNumber(1, 100, gen);
		int customerDistrictID;
		int customerWarehouseID;
		if (x <= 85) {
			customerDistrictID = districtID;
			customerWarehouseID = terminalWarehouseID;
		} else {
			customerDistrictID = TPCCUtil.randomNumber(1,
					HTAPBConstants.configDistPerWhse, gen);
			do {
				customerWarehouseID = TPCCUtil.randomNumber(1,
						numWarehouses, gen);
			} while (customerWarehouseID == terminalWarehouseID
					&& numWarehouses > 1);
		}

		long y = TPCCUtil.randomNumber(1, 100, gen);
		boolean customerByName;
		String customerLastName = null;
		int customerID = -1;
		if (y <= 60) {
			// 60% lookups by last name
			customerByName = true;
			customerLastName = TPCCUtil.getNonUniformRandomLastNameForRun(gen);
		} else {
			// 40% lookups by customer ID
			customerByName = false;
			customerID = TPCCUtil.getCustomerID(gen);
		}

		float paymentAmount = (float) (TPCCUtil.randomNumber(100, 500000, gen) / 100.0);

		paymentTransaction(terminalWarehouseID,
				customerWarehouseID, paymentAmount, districtID,
				customerDistrictID, customerID,
				customerLastName, customerByName, conn, w);


		return null;
	}

	private void paymentTransaction(int w_id, int c_w_id, float h_amount,
									int d_id, int c_d_id, int c_id, String c_last, boolean c_by_name, Connection conn, TPCCWorker w)
			throws SQLException {
		String w_street_1, w_street_2, w_city, w_state, w_zip, w_name;
		String d_street_1, d_street_2, d_city, d_state, d_zip, d_name;

		// Query 1
		payUpdateWhse.setFloat(1, h_amount);
		payUpdateWhse.setInt(2, w_id);

		int result = payUpdateWhse.executeUpdate();
		if (result == 0) {
			LOG.debug("Payment: failed 1st stmt");
			throw new RuntimeException("W_ID = " + w_id + " not found!");
		}

		// Query 2
		payGetWhse.setInt(1, w_id);

		ResultSet rs = payGetWhse.executeQuery();
		if (!rs.next()) {
			LOG.debug("Payment: failed 2nd stmt");
			throw new RuntimeException("W_ID = " + w_id + " not found!");
		}

		w_street_1 = rs.getString("W_STREET_1");
		w_street_2 = rs.getString("W_STREET_2");
		w_city = rs.getString("W_CITY");
		w_state = rs.getString("W_STATE");
		w_zip = rs.getString("W_ZIP");
		w_name = rs.getString("W_NAME");
		rs.close();

		// Query 3
		payUpdateDist.setFloat(1, h_amount);
		payUpdateDist.setInt(2, w_id);
		payUpdateDist.setInt(3, d_id);

		result = payUpdateDist.executeUpdate();
		if (result == 0) {
			LOG.debug("Payment: failed 3rd stmt");
			throw new RuntimeException("D_ID = " + d_id + " D_W_ID = " + w_id
					+ " not found!");
		}

		// Query 4
		payGetDist.setInt(1, w_id);
		payGetDist.setInt(2, d_id);

		rs = payGetDist.executeQuery();
		if (!rs.next()) {
			LOG.debug("Payment: failed 4th stmt");
			throw new RuntimeException("D_ID = " + d_id + " D_W_ID = " + w_id
					+ " not found!");
		}

		d_street_1 = rs.getString("D_STREET_1");
		d_street_2 = rs.getString("D_STREET_2");
		d_city = rs.getString("D_CITY");
		d_state = rs.getString("D_STATE");
		d_zip = rs.getString("D_ZIP");
		d_name = rs.getString("D_NAME");
		rs.close();

		// Lookup by name or by customerID
		Customer c;
		if (c_by_name) {
			assert c_id <= 0;
			c = getCustomerByName(c_w_id, c_d_id, c_last);
		} else {
			assert c_last == null;
			c = getCustomerById(c_w_id, c_d_id, c_id);
		}

		// Perform bookkeeping
		c.c_balance -= h_amount;
		c.c_ytd_payment += h_amount;
		c.c_payment_cnt += 1;
		String c_data = null;

		if (c.c_credit.equals("BC")) { // bad credit

			// Prepare query
			payGetCustCdata.setInt(1, c_w_id);
			payGetCustCdata.setInt(2, c_d_id);
			payGetCustCdata.setInt(3, c.c_id);

			rs = payGetCustCdata.executeQuery();
			if (!rs.next()) {
				LOG.debug("Payment: failed 5th stmt");
				throw new RuntimeException("C_ID = " + c.c_id + " C_W_ID = "
						+ c_w_id + " C_D_ID = " + c_d_id + " not found!");
			}

			c_data = rs.getString("C_DATA");
			rs.close();

			c_data = c.c_id + " " + c_d_id + " " + c_w_id + " " + d_id + " "
					+ w_id + " " + h_amount + " | " + c_data;

			if (c_data.length() > 500)
				c_data = c_data.substring(0, 500);

			// Prepare query
			payUpdateCustBalCdata.setFloat(1, c.c_balance);
			payUpdateCustBalCdata.setFloat(2, c.c_ytd_payment);
			payUpdateCustBalCdata.setInt(3, c.c_payment_cnt);
			payUpdateCustBalCdata.setString(4, c_data);
			payUpdateCustBalCdata.setInt(5, c_w_id);
			payUpdateCustBalCdata.setInt(6, c_d_id);
			payUpdateCustBalCdata.setInt(7, c.c_id);

			result = payUpdateCustBalCdata.executeUpdate();
			if (result == 0) {
				LOG.debug("Payment: failed 6th stmt");
				throw new RuntimeException(
						"Error in PYMNT Txn updating Customer C_ID = " + c.c_id
								+ " C_W_ID = " + c_w_id + " C_D_ID = " + c_d_id);
			}
		} else { // GoodCredit

			// Prepare query
			payUpdateCustBal.setFloat(1, c.c_balance);
			payUpdateCustBal.setFloat(2, c.c_ytd_payment);
			payUpdateCustBal.setInt(3, c.c_payment_cnt);
			payUpdateCustBal.setInt(4, c_w_id);
			payUpdateCustBal.setInt(5, c_d_id);
			payUpdateCustBal.setInt(6, c.c_id);

			result = payUpdateCustBal.executeUpdate();
			if (result == 0) {
				LOG.debug("Payment: failed 7th stmt");
				throw new RuntimeException("C_ID = " + c.c_id + " C_W_ID = "
						+ c_w_id + " C_D_ID = " + c_d_id + " not found!");
			}
		}

		// Bookkeeping
		if (w_name.length() > 10)
			w_name = w_name.substring(0, 10);
		if (d_name.length() > 10)
			d_name = d_name.substring(0, 10);
		String h_data = w_name + "    " + d_name;

		// Prepare query
		payInsertHist.setInt(1, c_d_id);
		payInsertHist.setInt(2, c_w_id);
		payInsertHist.setInt(3, c.c_id);
		payInsertHist.setInt(4, d_id);
		payInsertHist.setInt(5, w_id);
		payInsertHist.setFloat(7, h_amount);
		payInsertHist.setString(8, h_data);

		if (w.getWrkld().getCalibrate()) {
			//increments the ts counter for density assessment
			w.getTsCounter().incrementAndGet();
			payInsertHist.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
		} else {
			payInsertHist.setTimestamp(6, new Timestamp(w.getClock().tick()));
		}
		payInsertHist.executeUpdate();
		conn.commit();

		traceLogger(w_id, w_street_1, w_street_2, w_city, w_state, w_zip, d_id, d_street_1, d_street_2, d_city,
				d_state, d_zip, c, h_amount, c_data);
	}

	private Customer getCustomerById(int c_w_id, int c_d_id, int c_id)
			throws SQLException {
		// Prepare query
		payGetCust.setInt(1, c_w_id);
		payGetCust.setInt(2, c_d_id);
		payGetCust.setInt(3, c_id);

		ResultSet rs = payGetCust.executeQuery();
		if (!rs.next()) {
			LOG.debug("Payment: failed 8th stmt");
			throw new RuntimeException("C_ID = " + c_id + " C_D_ID = " + c_d_id
					+ " C_W_ID = " + c_w_id + " not found!");
		}

		Customer c = TPCCUtil.newCustomerFromResults(rs);
		c.c_id = c_id;
		c.c_last = rs.getString("C_LAST");
		rs.close();
		return c;
	}

	private Customer getCustomerByName(int c_w_id, int c_d_id, String c_last)
			throws SQLException {
		ArrayList<Customer> customers = new ArrayList<Customer>();


		// Prepare query
		customerByName.setInt(1, c_w_id);
		customerByName.setInt(2, c_d_id);
		customerByName.setString(3, c_last);
		ResultSet rs = customerByName.executeQuery();

		// Get the results
		while (rs.next()) {
			Customer c = TPCCUtil.newCustomerFromResults(rs);
			c.c_id = rs.getInt("C_ID");
			c.c_last = c_last;
			customers.add(c);
		}

		rs.close();

		if (customers.size() == 0) {
			LOG.debug("Payment: failed 9th stmt");
			throw new RuntimeException("C_LAST = " + c_last + " C_D_ID = " + c_d_id
					+ " C_W_ID = " + c_w_id + " not found!");
		}

		// TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but
		// that counts starting from 1.
		int index = customers.size() / 2;
		if (customers.size() % 2 == 0) {
			index -= 1;
		}
		return customers.get(index);
	}


	private void traceLogger(int w_id, String w_street_1, String w_street_2, String w_city, String w_state,
							 String w_zip, int d_id, String d_street_1, String d_street_2, String d_city,
							 String d_state, String d_zip, Customer c, float h_amount, String c_data) {
		StringBuilder terminalMessage = new StringBuilder();
		terminalMessage.append("\n+---------------------------- PAYMENT ----------------------------+");
		terminalMessage.append("\n Date: ").append(TPCCUtil.getCurrentTime());
		terminalMessage.append("\n\n Warehouse: ");
		terminalMessage.append(w_id);
		terminalMessage.append("\n   Street:  ");
		terminalMessage.append(w_street_1);
		terminalMessage.append("\n   Street:  ");
		terminalMessage.append(w_street_2);
		terminalMessage.append("\n   City:    ");
		terminalMessage.append(w_city);
		terminalMessage.append("   State: ");
		terminalMessage.append(w_state);
		terminalMessage.append("  Zip: ");
		terminalMessage.append(w_zip);
		terminalMessage.append("\n\n District:  ");
		terminalMessage.append(d_id);
		terminalMessage.append("\n   Street:  ");
		terminalMessage.append(d_street_1);
		terminalMessage.append("\n   Street:  ");
		terminalMessage.append(d_street_2);
		terminalMessage.append("\n   City:    ");
		terminalMessage.append(d_city);
		terminalMessage.append("   State: ");
		terminalMessage.append(d_state);
		terminalMessage.append("  Zip: ");
		terminalMessage.append(d_zip);
		terminalMessage.append("\n\n Customer:  ");
		terminalMessage.append(c.c_id);
		terminalMessage.append("\n   Name:    ");
		terminalMessage.append(c.c_first);
		terminalMessage.append(" ");
		terminalMessage.append(c.c_middle);
		terminalMessage.append(" ");
		terminalMessage.append(c.c_last);
		terminalMessage.append("\n   Street:  ");
		terminalMessage.append(c.c_street_1);
		terminalMessage.append("\n   Street:  ");
		terminalMessage.append(c.c_street_2);
		terminalMessage.append("\n   City:    ");
		terminalMessage.append(c.c_city);
		terminalMessage.append("   State: ");
		terminalMessage.append(c.c_state);
		terminalMessage.append("  Zip: ");
		terminalMessage.append(c.c_zip);
		terminalMessage.append("\n   Since:   ");
		if (c.c_since != null) terminalMessage.append(c.c_since.toString());
		terminalMessage.append("\n   Credit:  ");
		terminalMessage.append(c.c_credit);
		terminalMessage.append("\n   %Disc:   ");
		terminalMessage.append(c.c_discount);
		terminalMessage.append("\n   Phone:   ");
		terminalMessage.append(c.c_phone);
		terminalMessage.append("\n\n Amount Paid:      ");
		terminalMessage.append(h_amount);
		terminalMessage.append("\n Credit Limit:     ");
		terminalMessage.append(c.c_credit_lim);
		terminalMessage.append("\n New Cust-Balance: ");
		terminalMessage.append(c.c_balance);
		if (c.c_credit.equals("BC")) {
			if (c_data.length() > 50) {
				terminalMessage.append("\n\n Cust-Data: ").append(c_data, 0, 50);
				int data_chunks = c_data.length() > 200 ? 4
						: c_data.length() / 50;
				for (int n = 1; n < data_chunks; n++)
					terminalMessage.append("\n            ").append(c_data, n * 50, (n + 1) * 50);
			} else {
				terminalMessage.append("\n\n Cust-Data: ").append(c_data);
			}
		}
		terminalMessage.append("\n+-----------------------------------------------------------------+\n\n");

		if (LOG.isTraceEnabled()) LOG.trace(terminalMessage.toString());
	}

	public long getKeyingTime() {
		return HTAPBConstants.keyingTime_Payment; // Keying time in seconds.
	}
}