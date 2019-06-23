
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
package pt.haslab.htapbench.procedures.tpch;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import pt.haslab.htapbench.api.Procedure;
import pt.haslab.htapbench.api.SQLStmt;
import pt.haslab.htapbench.core.Worker;
import pt.haslab.htapbench.benchmark.WorkloadConfiguration;
import pt.haslab.htapbench.core.Clock;

public abstract class GenericQuery extends Procedure {

    protected static final Logger LOG = Logger.getLogger(GenericQuery.class);

    private PreparedStatement stmt;
    private Worker owner;

    public void setOwner(Worker w) {
        this.owner = w;
    }

    protected abstract SQLStmt get_query(Clock clock, WorkloadConfiguration wrklConf);

    public int run(Connection conn, Clock clock, WorkloadConfiguration wrklConf)
            throws SQLException, InvalidResultException {
        // Initializing all prepared statements
        stmt = this.getPreparedStatement(conn, get_query(clock, wrklConf));

        // Set the owner of the statement, so that it can be cancelled if needed
        if (owner != null)
            owner.setCurrStatement(stmt);

        ResultSet rs;
        try {
            rs = stmt.executeQuery();
        } catch (SQLException ex) {
            // If the system thinks we're missing a prepared statement, then we should regenerate them.
            if (ex.getErrorCode() == 0 && ex.getSQLState() != null && ex.getSQLState().equals("07003")) {
                this.resetPreparedStatements();
                rs = stmt.executeQuery();
            } else {
                throw ex;
            }
        }

        return validateResult(rs, wrklConf.getHybridWorkload());
    }

    /**
     * Validate the results based on the number of rows retrieved from the
     * result set and depending on the current workload.
     *
     * @param rs          the ResultSet to be validated
     * @param hybridWrkld boolean flag indicating if the current workload is hybrid
     * @return the number of rows in the ResultSet
     * @throws SQLException           if an exception is thrown while operating on the ResultSet
     * @throws InvalidResultException if the result is NULL or if the ResultSet is empty
     */
    private int validateResult(ResultSet rs, boolean hybridWrkld)
            throws SQLException, InvalidResultException {
        // Determine the number of rows in the ResultSet
        int t = 0;
        while (rs.next()) {
            t = t + 1;
        }

        // ----------------------------------------
        //           Validation Exceptions
        // ----------------------------------------
        // Some queries are inherently designed in such a way that an InvalidResultException
        // would always be thrown. This is especially true for pure OLAP workloads, which is
        // often dependent on OLTP workloads having modified a particular value. The validation
        // of such queries are simply skipped.



        // ----------------------------------------
        //            Actual Validation
        // ----------------------------------------

        // Check that non-zero rows are returned in the ResultSet
        if (t == 0) {
            throw new InvalidResultException("Query " + this + " returned zero rows", stmt);
        }

        // Check that the returned value is not NULL
        if (t == 1) {
            rs.beforeFirst();
            rs.next();
            String result = rs.getString(1);
            if (result == null)
                throw new InvalidResultException("Query " + this + " returned a null value", stmt);
        }

        return t;
    }

    /**
     * Thrown from a TPC-H procedure to indicate to the Worker
     * that the result set should be treated as invalid.
     */
    public static class InvalidResultException extends RuntimeException {
        private static final long serialVersionUID = -1L;

        private PreparedStatement stmt = null;

        /**
         * Constructs a new InvalidResultException with the specified detail message.
         */
        InvalidResultException(String msg, Throwable ex, PreparedStatement stmt) {
            super(msg, ex);
            this.stmt = stmt;
        }

        /**
         * Constructs a new InvalidResultException with the specified detail message.
         */
        InvalidResultException(String msg, PreparedStatement stmt) {
            this(msg, null, stmt);
        }

        /**
         * Get the PreparedStatement that caused an InvalidResultException
         *
         * @return PreparedStatement causing the InvalidResultException
         */
        public PreparedStatement getStmt() {
            return stmt;
        }
    } // END CLASS
}
