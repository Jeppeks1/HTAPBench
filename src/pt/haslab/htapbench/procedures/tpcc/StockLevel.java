
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
import java.util.Random;

import org.apache.log4j.Logger;
import pt.haslab.htapbench.api.SQLStmt;
import pt.haslab.htapbench.benchmark.HTAPBConstants;
import pt.haslab.htapbench.util.TPCCUtil;
import pt.haslab.htapbench.core.TPCCWorker;

public class StockLevel extends TPCCProcedure {

	private static final Logger LOG = Logger.getLogger(StockLevel.class);

	private SQLStmt stockGetDistOrderIdSQL = new SQLStmt("SELECT D_NEXT_O_ID " +
			"FROM " + HTAPBConstants.TABLENAME_DISTRICT + " " +
			"WHERE D_W_ID = ? AND D_ID = ?");
	private SQLStmt stockGetCountStockSQL = new SQLStmt("SELECT COUNT(DISTINCT(S_I_ID)) AS STOCK_COUNT"
			+ " FROM " + HTAPBConstants.TABLENAME_ORDERLINE + ", " + HTAPBConstants.TABLENAME_STOCK
			+ " WHERE OL_W_ID = ?"
			+ " AND OL_D_ID = ?"
			+ " AND OL_O_ID < ?"
			+ " AND OL_O_ID >= ? - 20"
			+ " AND S_W_ID = ?"
			+ " AND S_I_ID = OL_I_ID"
			+ " AND S_QUANTITY < ?");

	// Stock Level Txn

	@Override
	public ResultSet run(Connection conn, Random gen,
						 int terminalWarehouseID, int numWarehouses,
						 int terminalDistrictLowerID, int terminalDistrictUpperID,
						 TPCCWorker w) throws SQLException {

		int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
		int threshold = TPCCUtil.randomNumber(10, 20, gen);

		stockLevelTransaction(terminalWarehouseID, districtID, threshold, conn);

		return null;
	}


	private void stockLevelTransaction(int w_id, int d_id, int threshold, Connection conn)
			throws SQLException {
		PreparedStatement stockGetDistOrderId = getPreparedStatement(conn, stockGetDistOrderIdSQL);
		PreparedStatement stockGetCountStock = getPreparedStatement(conn, stockGetCountStockSQL);

		stockGetDistOrderId.setInt(1, w_id);
		stockGetDistOrderId.setInt(2, d_id);

		ResultSet rs = null;

		try {
			// Execute the first stock query
			rs = stockGetDistOrderId.executeQuery();

			// Try to point to the first row; otherwise throw an error
			if (!rs.next())
				throw new RuntimeException("D_W_ID = " + w_id + " D_ID = " + d_id + " not found!");

			int o_id = rs.getInt("D_NEXT_O_ID");
			rs.close();

			// Prepare variables for the second query
			stockGetCountStock.setInt(1, w_id);
			stockGetCountStock.setInt(2, d_id);
			stockGetCountStock.setInt(3, o_id);
			stockGetCountStock.setInt(4, o_id);
			stockGetCountStock.setInt(5, w_id);
			stockGetCountStock.setInt(6, threshold);

			// Execute the second query
			rs = stockGetCountStock.executeQuery();

			if (!rs.next())
				throw new RuntimeException("OL_W_ID = " + w_id + " OL_D_ID = " + d_id + " OL_O_ID = " + o_id + " not found!");

			int stock_count = rs.getInt("STOCK_COUNT");
			traceLogger(w_id, d_id, threshold, stock_count);
		} finally {
			if (rs != null)
				rs.close();
		}
	}

	private void traceLogger(int w_id, int d_id, int threshold, int stock_count) {
		StringBuilder terminalMessage = new StringBuilder();
		terminalMessage
				.append("\n+-------------------------- STOCK-LEVEL --------------------------+");
		terminalMessage.append("\n Warehouse: ");
		terminalMessage.append(w_id);
		terminalMessage.append("\n District:  ");
		terminalMessage.append(d_id);
		terminalMessage.append("\n\n Stock Level Threshold: ");
		terminalMessage.append(threshold);
		terminalMessage.append("\n Low Stock Count:       ");
		terminalMessage.append(stock_count);
		terminalMessage
				.append("\n+-----------------------------------------------------------------+\n\n");
		if (LOG.isTraceEnabled()) LOG.trace(terminalMessage.toString());
	}

	public long getKeyingTime() {
		return HTAPBConstants.keyingTime_StockLevel; // Keying time in seconds
	}
}
