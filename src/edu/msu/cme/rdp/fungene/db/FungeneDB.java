/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msu.cme.rdp.fungene.db;

import edu.msu.cme.rdp.fungene.utils.FungeneProps;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.dbcp.BasicDataSource;

/**
 *
 * @author fishjord
 */
public abstract class FungeneDB {

    public static class RedundentId {
        public int redundentId;
        public int nonRedundentId;
        public String hmmName;
        public int hmmVersion;
        public float bitsSaved;
    }

    private static final String protIdSql = "select gi from protein";
    protected BasicDataSource ds;

    public FungeneDB(FungeneProps props) {
        ds = new BasicDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUsername(props.getDbProps().getUserName());
        ds.setPassword(props.getDbProps().getPassword());
        ds.setUrl(props.getDbProps().getDbUrl());
    }

    protected static String makeSeqSubSelect(boolean aligned, boolean prot, String seq) {

        String table = "unaligned";
        if (aligned) {
            table = "aligned";
        }

        if(prot) {
            table += "_prot_sequence";
        } else {
            table += "_nuc_sequence";
        }

        return "(select seq_id from " + table + " where seq_hash=md5('" + seq + "') and seq='" + seq + "')";
    }

    protected static String makeHMMSubSelect(String hmm, int release) {
        return "(select hmm_id from hmm_model where hmm_name='" + hmm + "' and added_in_release=" + release + ")";
    }

    protected static void setString(PreparedStatement prepStmt, int index, String val) throws SQLException {
        if (val == null) {
            prepStmt.setNull(index, Types.VARCHAR);
        } else {
            prepStmt.setString(index, val);
        }
    }

    public Set<Integer> getProtIds() {
        Set<Integer> ret = new HashSet();

        Connection dbConn = null;
        Statement stmt = null;
        ResultSet result = null;

        try {
            dbConn = ds.getConnection();

            stmt = dbConn.createStatement();
            result = stmt.executeQuery(protIdSql);

            while (result.next()) {
                ret.add(result.getInt("gi"));
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                result.close();
            } catch (Exception e) {
            }
            try {
                stmt.close();
            } catch (Exception e) {
            }
            try {
                dbConn.close();
            } catch (Exception e) {
            }
        }

        return ret;
    }
}
