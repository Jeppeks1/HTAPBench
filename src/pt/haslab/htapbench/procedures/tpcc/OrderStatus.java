
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


public class OrderStatus extends TPCCProcedure {

	private static final Logger LOG = Logger.getLogger(OrderStatus.class);

	private SQLStmt ordStatGetNewestOrdSQL = new SQLStmt("SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM " + HTAPBConstants.TABLENAME_ORDER
			+ " WHERE O_W_ID = ?"
			+ " AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC");

	private SQLStmt ordStatGetOrderLinesSQL = new SQLStmt("SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY,"
			+ " OL_AMOUNT, OL_DELIVERY_D"
			+ " FROM " + HTAPBConstants.TABLENAME_ORDERLINE
			+ " WHERE OL_O_ID = ?"
			+ " AND OL_D_ID =?"
			+ " AND OL_W_ID = ?");

	private SQLStmt customerByIdSQL = new SQLStmt("SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, "
			+ "C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, "
			+ "C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + HTAPBConstants.TABLENAME_CUSTOMER + " WHERE "
			+ "C_W_ID = ? AND C_D_ID = ? AND C_ID = ?");

	private SQLStmt customerByNameSQL = new SQLStmt("SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, "
			+ "C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, "
			+ "C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM " + HTAPBConstants.TABLENAME_CUSTOMER
			+ " WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST");

	private PreparedStatement ordStatGetNewestOrd = null;
	private PreparedStatement ordStatGetOrderLines = null;
	private PreparedStatement customerByName = null;
	private PreparedStatement payGetCust = null;

	public ResultSet run(Connection conn, Random gen,
						 int terminalWarehouseID, int numWarehouses,
						 int terminalDistrictLowerID, int terminalDistrictUpperID,
						 TPCCWorker w) throws SQLException {

		int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
		int y = TPCCUtil.randomNumber(1, 100, gen);
		String customerLastName = null;

		payGetCust = this.getPreparedStatement(conn, customerByIdSQL);
		customerByName = this.getPreparedStatement(conn, customerByNameSQL);
		ordStatGetNewestOrd = this.getPreparedStatement(conn, ordStatGetNewestOrdSQL);
		ordStatGetOrderLines = this.getPreparedStatement(conn, ordStatGetOrderLinesSQL);

		boolean isCustomerByName;
		int customerID = -1;

		if (y <= 60) {
			isCustomerByName = true;
			customerLastName = TPCCUtil.getNonUniformRandomLastNameForRun(gen);
		} else {
			isCustomerByName = false;
			customerID = TPCCUtil.getCustomerID(gen);
		}

		orderStatusTransaction(terminalWarehouseID, districtID,
				customerID, customerLastName, isCustomerByName, conn, w);

		return null;
	}


	private void orderStatusTransaction(int w_id, int d_id, int c_id,
										String c_last, boolean c_by_name, Connection conn, TPCCWorker w) throws SQLException {
		int o_id, o_carrier_id;
		ArrayList<String> orderLines = new ArrayList<String>();
		Timestamp entrydate;
		Customer c;

		if (c_by_name) {
			assert c_id <= 0;
			c = getCustomerByName(w_id, d_id, c_last);
		} else {
			assert c_last == null;
			c = getCustomerById(w_id, d_id, c_id);
		}

		ResultSet rs = null;

		try {
			ordStatGetNewestOrd.setInt(1, w_id);
			ordStatGetNewestOrd.setInt(2, d_id);
			ordStatGetNewestOrd.setInt(3, c.c_id);
			ordStatGetNewestOrd.setMaxRows(1);
			rs = ordStatGetNewestOrd.executeQuery();

			if (!rs.next()) {
				throw new RuntimeException("No orders for O_W_ID = " + w_id
						+ " O_D_ID = " + d_id + " O_C_ID = " + c.c_id);
			}

			o_id = rs.getInt("O_ID");
			o_carrier_id = rs.getInt("O_CARRIER_ID");
			entrydate = rs.getTimestamp("O_ENTRY_D");
			rs.close();

			// retrieve the order lines for the most recent order
			ordStatGetOrderLines.setInt(1, o_id);
			ordStatGetOrderLines.setInt(2, d_id);
			ordStatGetOrderLines.setInt(3, w_id);
			rs = ordStatGetOrderLines.executeQuery();

			while (rs.next()) {
				StringBuilder orderLine = new StringBuilder();
				orderLine.append("[");
				orderLine.append(rs.getLong("OL_SUPPLY_W_ID"));
				orderLine.append(" - ");
				orderLine.append(rs.getLong("OL_I_ID"));
				orderLine.append(" - ");
				orderLine.append(rs.getLong("OL_QUANTITY"));
				orderLine.append(" - ");
				orderLine.append(TPCCUtil.formattedDouble(rs.getDouble("OL_AMOUNT")));
				orderLine.append(" - ");
				if (rs.getTimestamp("OL_DELIVERY_D") != null)
					orderLine.append(rs.getTimestamp("OL_DELIVERY_D"));
				else
					orderLine.append("99-99-9999");
				orderLine.append("]");
				orderLines.add(orderLine.toString());
			}
		} finally {
			if (rs != null)
				rs.close();
		}

		traceLogger(w_id, d_id, o_id, o_carrier_id, orderLines, entrydate, c);
	}

	private Customer getCustomerById(int c_w_id, int c_d_id, int c_id)
			throws SQLException {

		payGetCust.setInt(1, c_w_id);
		payGetCust.setInt(2, c_d_id);
		payGetCust.setInt(3, c_id);

		ResultSet rs = payGetCust.executeQuery();

		if (!rs.next()) {
			throw new RuntimeException("OrderStatus error: C_ID = " + c_id + " C_D_ID = " + c_d_id
					+ " C_W_ID = " + c_w_id + " not found!");
		}

		Customer c = TPCCUtil.newCustomerFromResults(rs);

		c.c_last = rs.getString("C_LAST");
		c.c_id = c_id;

		rs.close();

		return c;
	}

	//attention this code is repeated in other transacitons... ok for now to allow for separate statements.
	private Customer getCustomerByName(int c_w_id, int c_d_id, String c_last)
			throws SQLException {
		ArrayList<Customer> customers = new ArrayList<Customer>();

		customerByName.setInt(1, c_w_id);
		customerByName.setInt(2, c_d_id);
		customerByName.setString(3, c_last);
		ResultSet rs = customerByName.executeQuery();

		while (rs.next()) {
			Customer c = TPCCUtil.newCustomerFromResults(rs);
			c.c_id = rs.getInt("C_ID");
			c.c_last = c_last;
			customers.add(c);
		}

		rs.close();

		if (customers.size() == 0) {
			throw new RuntimeException("OrderStatus error: C_LAST = " + c_last + " C_D_ID = " + c_d_id
					+ " C_W_ID =" + c_w_id + " not found!");
		}

		// TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but that counts starting from 1.
		int index = customers.size() / 2;
		if (customers.size() % 2 == 0) {
			index -= 1;
		}

		return customers.get(index);
	}

	private void traceLogger(int w_id, int d_id, int o_id, int o_carrier_id
			, ArrayList<String> orderLines, Timestamp entdate, Customer c) {
		StringBuilder terminalMessage = new StringBuilder();
		terminalMessage.append("\n");
		terminalMessage.append("+-------------------------- ORDER-STATUS -------------------------+\n");
		terminalMessage.append(" Date: ");
		terminalMessage.append(TPCCUtil.getCurrentTime());
		terminalMessage.append("\n\n Warehouse: ");
		terminalMessage.append(w_id);
		terminalMessage.append("\n District:  ");
		terminalMessage.append(d_id);
		terminalMessage.append("\n\n Customer:  ");
		terminalMessage.append(c.c_id);
		terminalMessage.append("\n   Name:    ");
		terminalMessage.append(c.c_first);
		terminalMessage.append(" ");
		terminalMessage.append(c.c_middle);
		terminalMessage.append(" ");
		terminalMessage.append(c.c_last);
		terminalMessage.append("\n   Balance: ");
		terminalMessage.append(c.c_balance);
		terminalMessage.append("\n\n");
		if (o_id == -1) {
			terminalMessage.append(" Customer has no orders placed.\n");
		} else {
			terminalMessage.append(" Order-Number: ");
			terminalMessage.append(o_id);
			terminalMessage.append("\n    Entry-Date: ");
			terminalMessage.append(entdate);
			terminalMessage.append("\n    Carrier-Number: ");
			terminalMessage.append(o_carrier_id);
			terminalMessage.append("\n\n");
			if (orderLines.size() != 0) {
				terminalMessage.append(" [Supply_W - Item_ID - Qty - Amount - Delivery-Date]\n");
				for (String orderLine : orderLines) {
					terminalMessage.append(" ");
					terminalMessage.append(orderLine);
					terminalMessage.append("\n");
				}
			} else {
				if (LOG.isTraceEnabled()) LOG.trace(" This Order has no Order-Lines.\n");
			}
		}
		terminalMessage.append("+-----------------------------------------------------------------+\n\n");
		if (LOG.isTraceEnabled()) LOG.trace(terminalMessage.toString());
	}

	public long getKeyingTime() {
		return HTAPBConstants.keyingTime_OrderStatus; // Keying time in seconds.
	}
}


